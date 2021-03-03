#include "BTreeVI.hpp"

#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
using OP_RESULT = leanstore::OP_RESULT;
// -------------------------------------------------------------------------------------
// Assumptions made in this implementation:
// 1) We don't insert an already removed key
// 2) Secondary Versions contain delta
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::lookup(u8* o_key, u16 o_key_length, function<void(const u8*, u16)> payload_callback)
{
   // TODO: use optimistic latches for leaf 5K (optimistic scans)
   // -------------------------------------------------------------------------------------
   u16 key_length = o_key_length + sizeof(SN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, o_key, o_key_length);
   MutableSlice key(key_buffer, key_length);
   setSN(key, 0);
   jumpmuTry()
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekExact(Slice(key.data(), key.length()));
      if (ret != OP_RESULT::OK) {
         raise(SIGTRAP);
         jumpmu_return OP_RESULT::NOT_FOUND;
      }
      const auto primary_version =
          *reinterpret_cast<const PrimaryVersion*>(iterator.value().data() + iterator.value().length() - sizeof(PrimaryVersion));
      auto reconstruct = reconstructTuple(iterator, key, [&](Slice value) { payload_callback(value.data(), value.length()); });
      ret = std::get<0>(reconstruct);
      if (ret != OP_RESULT::OK) {  // For debugging
         cout << endl;
         cout << u64(std::get<1>(reconstruct)) << endl;
         cout << u64(primary_version.tmp) << endl;
         raise(SIGTRAP);
         jumpmu_return OP_RESULT::NOT_FOUND;
      }
      jumpmu_return ret;
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::updateSameSizeInPlace(u8* o_key,
                                         u16 o_key_length,
                                         function<void(u8* value, u16 value_size)> callback,
                                         UpdateSameSizeInPlaceDescriptor& update_descriptor)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   const u16 key_length = o_key_length + sizeof(SN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, o_key, o_key_length);
   Slice key(key_buffer, key_length);
   MutableSlice m_key(key_buffer, key_length);
   setSN(m_key, 0);
   OP_RESULT ret;
   // -------------------------------------------------------------------------------------
   // 20K instructions more
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         raise(SIGTRAP);
         jumpmu_return ret;
      }
      // -------------------------------------------------------------------------------------
      u16 delta_and_descriptor_size, secondary_payload_length;
      u8 secondary_payload[PAGE_SIZE];
      SN secondary_sn;
      // -------------------------------------------------------------------------------------
      {
         auto primary_payload = iterator.mutableValue();
         PrimaryVersion& primary_version =
             *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
         if (primary_version.isWriteLocked() || !isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
            jumpmu_return OP_RESULT::ABORT_TX;
         }
         primary_version.writeLock();
         // -------------------------------------------------------------------------------------
         delta_and_descriptor_size = update_descriptor.size() + BTreeLL::calculateDeltaSize(update_descriptor);
         secondary_payload_length = delta_and_descriptor_size + sizeof(SecondaryVersion);
         // -------------------------------------------------------------------------------------
         std::memcpy(secondary_payload, &update_descriptor, update_descriptor.size());
         BTreeLL::copyDiffTo(update_descriptor, secondary_payload + update_descriptor.size(), primary_payload.data());
         // -------------------------------------------------------------------------------------
         SecondaryVersion& secondary_version =
             *new (secondary_payload + delta_and_descriptor_size) SecondaryVersion(primary_version.worker_id, primary_version.tts, false, true);
         secondary_version.next_sn = primary_version.next_sn;
         secondary_version.prev_sn = 0;
      }
      // -------------------------------------------------------------------------------------
      {
         do {
            secondary_sn = leanstore::utils::RandomGenerator::getRand<SN>(0, std::numeric_limits<SN>::max());
            // -------------------------------------------------------------------------------------
            setSN(m_key, secondary_sn);
            ret = iterator.insertKV(key, Slice(secondary_payload, secondary_payload_length));
         } while (ret != OP_RESULT::OK);
      }
      // -------------------------------------------------------------------------------------
      {
         setSN(m_key, 0);
         ret = iterator.seekExactWithHint(key, false);
         ensure(ret == OP_RESULT::OK);
         MutableSlice primary_payload = iterator.mutableValue();
         PrimaryVersion& primary_version =
             *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
         // -------------------------------------------------------------------------------------
         // WAL
         auto wal_entry = iterator.leaf.reserveWALEntry<WALUpdateSSIP>(o_key_length + delta_and_descriptor_size);
         wal_entry->type = WAL_LOG_TYPE::WALUpdate;
         wal_entry->key_length = o_key_length;
         wal_entry->delta_length = delta_and_descriptor_size;
         wal_entry->before_worker_id = primary_version.worker_id;
         wal_entry->before_tts = primary_version.tts;
         wal_entry->after_worker_id = myWorkerID();
         wal_entry->after_tts = myTTS();
         std::memcpy(wal_entry->payload, o_key, o_key_length);
         std::memcpy(wal_entry->payload + o_key_length, &update_descriptor, update_descriptor.size());
         callback(primary_payload.data(), primary_payload.length() - sizeof(PrimaryVersion));  // Update
         BTreeLL::XORDiffTo(update_descriptor, wal_entry->payload + o_key_length + update_descriptor.size(), primary_payload.data());
         wal_entry.submit();
         // -------------------------------------------------------------------------------------
         primary_version.worker_id = myWorkerID();
         primary_version.tts = myTTS();
         primary_version.next_sn = secondary_sn;
         if (primary_version.versions_counter++ == 0) {
            ensure(primary_version.versions_counter < 500);
            primary_version.prev_sn = secondary_sn;
         }
         // -------------------------------------------------------------------------------------
         if (!primary_version.is_gc_scheduled) {
            cr::Worker::my().addTODO(myWorkerID(), myTTS(), dt_id, key_length + sizeof(TODOEntry), [&](u8* entry) {
               auto& todo_entry = *reinterpret_cast<TODOEntry*>(entry);
               todo_entry.key_length = o_key_length;
               todo_entry.sn = secondary_sn;
               std::memcpy(todo_entry.key, o_key, o_key_length);
            });
            primary_version.is_gc_scheduled = true;
         }
         primary_version.unlock();
         jumpmu_return OP_RESULT::OK;
      }
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::insert(u8* o_key, u16 o_key_length, u8* value, u16 value_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   const u16 key_length = o_key_length + sizeof(SN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, o_key, o_key_length);
   MutableSlice m_key(key_buffer, key_length);
   Slice key(key_buffer, key_length);
   setSN(m_key, 0);
   const u16 payload_length = value_length + sizeof(PrimaryVersion);
   // -------------------------------------------------------------------------------------
   while (true) {
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
         OP_RESULT ret = iterator.seekToInsert(key);
         if (ret == OP_RESULT::DUPLICATE) {
            MutableSlice primary_payload = iterator.mutableValue();
            auto& primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
            if (primary_version.isWriteLocked() || !isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
               jumpmu_return OP_RESULT::ABORT_TX;
            }
            ensure(false);  // Not implemented: maybe it has been removed but no GCed
         }
         ret = iterator.enoughSpaceInCurrentNode(key, payload_length);
         if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
            iterator.splitForKey(key);
            jumpmu_continue;
         }
         // -------------------------------------------------------------------------------------
         // WAL
         auto wal_entry = iterator.leaf.reserveWALEntry<WALInsert>(o_key_length + value_length);
         wal_entry->type = WAL_LOG_TYPE::WALInsert;
         wal_entry->key_length = o_key_length;
         wal_entry->value_length = value_length;
         std::memcpy(wal_entry->payload, o_key, o_key_length);
         std::memcpy(wal_entry->payload + o_key_length, iterator.value().data(), value_length);
         wal_entry.submit();
         // -------------------------------------------------------------------------------------
         iterator.insertInCurrentNode(key, payload_length);
         MutableSlice payload = iterator.mutableValue();
         std::memcpy(payload.data(), value, value_length);
         new (payload.data() + value_length) PrimaryVersion(myWorkerID(), myTTS());
         jumpmu_return OP_RESULT::OK;
      }
      jumpmuCatch() { ensure(false); }
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::remove(u8* o_key, u16 o_key_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   u8 key_buffer[o_key_length + sizeof(SN)];
   const u16 key_length = o_key_length + sizeof(SN);
   std::memcpy(key_buffer, o_key, o_key_length);
   MutableSlice m_key(key_buffer, key_length);
   Slice key(key_buffer, key_length);
   setSN(m_key, 0);
   // -------------------------------------------------------------------------------------
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      OP_RESULT ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         raise(SIGTRAP);
         jumpmu_return OP_RESULT::NOT_FOUND;
      }
      // -------------------------------------------------------------------------------------
      u16 value_length, secondary_payload_length;
      u8 secondary_payload[PAGE_SIZE];
      SN secondary_sn;
      // -------------------------------------------------------------------------------------
      {
         auto primary_payload = iterator.mutableValue();
         PrimaryVersion& primary_version =
             *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
         if (primary_version.isWriteLocked() || !isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
            jumpmu_return OP_RESULT::ABORT_TX;
         }
         ensure(primary_version.is_removed == false);
         //         if()
         primary_version.writeLock();
         // -------------------------------------------------------------------------------------
         value_length = iterator.value().length() - sizeof(PrimaryVersion);
         secondary_payload_length = value_length + sizeof(SecondaryVersion);
         std::memcpy(secondary_payload, primary_payload.data(), value_length);
         auto& secondary_version =
             *new (secondary_payload + value_length) SecondaryVersion(primary_version.worker_id, primary_version.tts, false, false);
         secondary_version.worker_id = primary_version.worker_id;
         secondary_version.tts = primary_version.tts;
         secondary_version.next_sn = primary_version.next_sn;
      }
      // -------------------------------------------------------------------------------------
      {
         do {
            secondary_sn = leanstore::utils::RandomGenerator::getRand<SN>(0, std::numeric_limits<SN>::max());
            // -------------------------------------------------------------------------------------
            setSN(m_key, secondary_sn);
            ret = iterator.insertKV(key, Slice(secondary_payload, secondary_payload_length));
         } while (ret != OP_RESULT::OK);
      }
      // -------------------------------------------------------------------------------------
      {
         setSN(m_key, 0);
         ret = iterator.seekExactWithHint(key, false);
         ensure(ret == OP_RESULT::OK);
         MutableSlice primary_payload = iterator.mutableValue();
         auto old_primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
         // -------------------------------------------------------------------------------------
         // WAL
         auto wal_entry = iterator.leaf.reserveWALEntry<WALRemove>(o_key_length + value_length);
         wal_entry->type = WAL_LOG_TYPE::WALRemove;
         wal_entry->key_length = o_key_length;
         wal_entry->value_length = value_length;
         wal_entry->before_worker_id = old_primary_version.worker_id;
         wal_entry->before_tts = old_primary_version.tts;
         std::memcpy(wal_entry->payload, o_key, o_key_length);
         std::memcpy(wal_entry->payload + o_key_length, iterator.value().data(), value_length);
         wal_entry.submit();
         // -------------------------------------------------------------------------------------
         iterator.shorten(sizeof(PrimaryVersion));
         primary_payload = iterator.mutableValue();
         auto& primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data());
         //         auto& primary_version = *new (primary_payload.data()) PrimaryVersion(myWorkerID(), myTTS());
         primary_version = old_primary_version;
         primary_version.is_removed = true;
         primary_version.worker_id = myWorkerID();
         primary_version.tts = myTTS();
         primary_version.next_sn = secondary_sn;
         primary_version.versions_counter++;
         primary_version.unlock();
         // -------------------------------------------------------------------------------------
         if (!primary_version.is_gc_scheduled) {
            cr::Worker::my().addTODO(myWorkerID(), myTTS(), dt_id, key_length + sizeof(TODOEntry), [&](u8* entry) {
               auto& todo_entry = *reinterpret_cast<TODOEntry*>(entry);
               todo_entry.key_length = o_key_length;
               std::memcpy(todo_entry.key, o_key, o_key_length);
            });
            primary_version.is_gc_scheduled = true;
         }
      }
   }
   jumpmuCatch() { ensure(false); }
   // -------------------------------------------------------------------------------------
   return OP_RESULT::OK;
}
// -------------------------------------------------------------------------------------
// This undo implementation works only for rollback and not for undo operations during recovery
void BTreeVI::undo(void* btree_object, const u8* wal_entry_ptr, const u64)
{
   // TODO:
   auto& btree = *reinterpret_cast<BTreeVI*>(btree_object);
   static_cast<void>(btree);
   const WALEntry& entry = *reinterpret_cast<const WALEntry*>(wal_entry_ptr);
   switch (entry.type) {
      case WAL_LOG_TYPE::WALInsert: {  // Assuming no insert after remove
         auto& insert_entry = *reinterpret_cast<const WALInsert*>(&entry);
         jumpmuTry()
         {
            const u16 key_length = insert_entry.key_length + sizeof(SN);
            u8 key_buffer[key_length];
            std::memcpy(key_buffer, insert_entry.payload, insert_entry.key_length);
            *reinterpret_cast<SN*>(key_buffer + insert_entry.key_length) = 0;
            Slice key(key_buffer, key_length);
            // -------------------------------------------------------------------------------------
            BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(&btree));
            auto ret = iterator.seekExact(key);
            ensure(ret == OP_RESULT::OK);
            ret = iterator.removeCurrent();
            ensure(ret == OP_RESULT::OK);
            iterator.leaf.incrementGSN();  // TODO: write CLS
         }
         jumpmuCatch() {}
         break;
      }
      case WAL_LOG_TYPE::WALUpdate: {
         auto& update_entry = *reinterpret_cast<const WALUpdateSSIP*>(&entry);
         jumpmuTry()
         {
            const u16 key_length = update_entry.key_length + sizeof(SN);
            u8 key_buffer[key_length];
            std::memcpy(key_buffer, update_entry.payload, update_entry.key_length);
            Slice key(key_buffer, key_length);
            MutableSlice m_key(key_buffer, key_length);
            // -------------------------------------------------------------------------------------
            SN undo_sn;
            OP_RESULT ret;
            u8 secondary_payload[PAGE_SIZE];
            u16 secondary_payload_length;
            // -------------------------------------------------------------------------------------
            BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(&btree));
            {
               btree.setSN(m_key, 0);
               ret = iterator.seekExact(key);
               ensure(ret == OP_RESULT::OK);
               Slice primary_payload = iterator.value();
               const auto& primary_version =
                   *reinterpret_cast<const PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
               ensure(primary_version.worker_id == btree.myWorkerID());
               ensure(primary_version.tts == btree.myTTS());
               undo_sn = primary_version.next_sn;
            }
            {
               btree.setSN(m_key, undo_sn);
               ret = iterator.seekExact(key);
               ensure(ret == OP_RESULT::OK);
               secondary_payload_length = iterator.value().length();
               std::memcpy(secondary_payload, iterator.value().data(), secondary_payload_length);
            }
            {
               btree.setSN(m_key, 0);
               ret = iterator.seekExact(key);
               ensure(ret == OP_RESULT::OK);
               MutableSlice primary_payload = iterator.mutableValue();
               auto& primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
               const auto& secondary_version =
                   *reinterpret_cast<SecondaryVersion*>(secondary_payload + secondary_payload_length - sizeof(SecondaryVersion));
               primary_version.writeLock();
               primary_version.next_sn = secondary_version.next_sn;
               primary_version.tts = secondary_version.tts;
               primary_version.worker_id = secondary_version.worker_id;
               primary_version.versions_counter--;
               // -------------------------------------------------------------------------------------
               const auto& update_descriptor = *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(secondary_payload);
               btree.copyDiffFrom(update_descriptor, primary_payload.data(), secondary_payload + update_descriptor.size());
               // -------------------------------------------------------------------------------------
               primary_version.unlock();
               iterator.leaf.incrementGSN();  // TODO: write CLS
            }
            {
               btree.setSN(m_key, undo_sn);
               ret = iterator.seekExact(key);
               ensure(ret == OP_RESULT::OK);
               ret = iterator.removeCurrent();
               ensure(ret == OP_RESULT::OK);
            }
         }
         jumpmuCatch() { ensure(false); }
         break;
      }
      case WAL_LOG_TYPE::WALRemove: {
         auto& remove_entry = *reinterpret_cast<const WALRemove*>(&entry);
         const u16 key_length = remove_entry.key_length + sizeof(SN);
         u8 key_buffer[key_length];
         std::memcpy(key_buffer, remove_entry.payload, remove_entry.key_length);
         Slice key(key_buffer, key_length);
         MutableSlice m_key(key_buffer, key_length);
         const u16 payload_length = remove_entry.value_length + sizeof(PrimaryVersion);
         // -------------------------------------------------------------------------------------
         jumpmuTry()
         {
            SN secondary_sn, undo_next_sn;
            OP_RESULT ret;
            BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(&btree));
            u16 removed_value_length;
            u8 removed_value[PAGE_SIZE];
            u8 undo_worker_id;
            u64 undo_tts;
            // -------------------------------------------------------------------------------------
            {
               btree.setSN(m_key, 0);
               ret = iterator.seekExact(key);
               ensure(ret == OP_RESULT::OK);
               const auto& primary_version =
                   *reinterpret_cast<const PrimaryVersion*>(iterator.value().data() + iterator.value().length() - sizeof(PrimaryVersion));
               secondary_sn = primary_version.next_sn;
               if (primary_version.worker_id != btree.myWorkerID()) {
                  raise(SIGTRAP);
                  cout << endl << u64(primary_version.tmp) << endl;
               }
               ensure(primary_version.worker_id == btree.myWorkerID());
               ensure(primary_version.tts == btree.myTTS());
            }
            // -------------------------------------------------------------------------------------
            {
               btree.setSN(m_key, secondary_sn);
               ret = iterator.seekExact(key);
               ensure(ret == OP_RESULT::OK);
               auto secondary_payload = iterator.value();
               removed_value_length = secondary_payload.length() - sizeof(SecondaryVersion);
               std::memcpy(removed_value, secondary_payload.data(), removed_value_length);
               auto const secondary_version =
                   *reinterpret_cast<const SecondaryVersion*>(secondary_payload.data() + secondary_payload.length() - sizeof(SecondaryVersion));
               undo_worker_id = secondary_version.worker_id;
               undo_tts = secondary_version.tts;
               undo_next_sn = secondary_version.next_sn;
            }
            // -------------------------------------------------------------------------------------
            {
               btree.setSN(m_key, 0);
               while (true) {
                  ret = iterator.seekExact(key);
                  ensure(ret == OP_RESULT::OK);
                  ret = iterator.enoughSpaceInCurrentNode(key, payload_length);  // TODO:
                  if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
                     iterator.splitForKey(key);
                     continue;
                  }
                  break;
               }
               ret = iterator.removeCurrent();
               ensure(ret == OP_RESULT::OK);
               const u16 primary_payload_length = removed_value_length + sizeof(PrimaryVersion);
               iterator.insertInCurrentNode(key, primary_payload_length);
               auto primary_payload = iterator.mutableValue();
               std::memcpy(primary_payload.data(), removed_value, removed_value_length);
               auto primary_version = new (primary_payload.data() + removed_value_length) PrimaryVersion(undo_worker_id, undo_tts);
               primary_version->next_sn = undo_next_sn;
               ensure(primary_version->is_removed == false);
            }
            // -------------------------------------------------------------------------------------
            {
               btree.setSN(m_key, secondary_sn);
               ret = iterator.seekExact(key);
               ensure(ret == OP_RESULT::OK);
               ret = iterator.removeCurrent();
               ensure(ret == OP_RESULT::OK);
            }
         }
         jumpmuCatch() { ensure(false); }
         break;
      }
      default: {
         break;
      }
   }
}  // namespace btree
// -------------------------------------------------------------------------------------
void BTreeVI::todo(void* btree_object, const u8* entry_ptr, const u64)
{
   auto& btree = *reinterpret_cast<BTreeVI*>(btree_object);
   const TODOEntry& todo_entry = *reinterpret_cast<const TODOEntry*>(entry_ptr);
   // -------------------------------------------------------------------------------------
   const u16 key_length = todo_entry.key_length + sizeof(SN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, todo_entry.key, todo_entry.key_length);
   MutableSlice m_key(key_buffer, key_length);
   Slice key(key_buffer, key_length);
   btree.setSN(m_key, 0);
   OP_RESULT ret;
   // -------------------------------------------------------------------------------------
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(&btree));
      ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {  // Because of rollbacks
         jumpmu_return;
      }
      // -------------------------------------------------------------------------------------
      MutableSlice primary_payload = iterator.mutableValue();
      PrimaryVersion* primary_version = reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
      if (primary_version->worker_id != btree.myWorkerID()) {
         jumpmu_return;
      }
      const bool safe_to_remove = cr::Worker::my().isVisibleForAll(primary_version->worker_id, primary_version->tts);
      SN next_sn = primary_version->next_sn;
      if (safe_to_remove) {
         if (primary_version->is_removed) {
            ret = iterator.removeCurrent();
            ensure(ret == OP_RESULT::OK);
         } else {
            primary_version->next_sn = 0;
            primary_version->prev_sn = 0;
            primary_version->versions_counter = 0;
            primary_version->is_gc_scheduled = false;
            primary_version->tmp = 99;
            primary_version->unlock();
         }
         // -------------------------------------------------------------------------------------
         while (next_sn != 0) {
            btree.setSN(m_key, next_sn);
            ret = iterator.seekExact(key);
            if (ret != OP_RESULT::OK) {
               break;
            }
            // -------------------------------------------------------------------------------------
            Slice secondary_payload = iterator.value();
            const auto& secondary_version =
                *reinterpret_cast<const SecondaryVersion*>(secondary_payload.data() + secondary_payload.length() - sizeof(SecondaryVersion));
            next_sn = secondary_version.next_sn;
            iterator.removeCurrent();
         }
      }
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
struct DTRegistry::DTMeta BTreeVI::getMeta()
{
   DTRegistry::DTMeta btree_meta = {.iterate_children = iterateChildrenSwips,
                                    .find_parent = findParent,
                                    .check_space_utilization = checkSpaceUtilization,
                                    .checkpoint = checkpoint,
                                    .undo = undo,
                                    .todo = todo,
                                    .serialize = serialize,
                                    .deserialize = deserialize};
   return btree_meta;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::scanDesc(u8* o_key, u16 o_key_length, function<bool(const u8*, u16, const u8*, u16)> callback, function<void()>)
{
   scan<false>(o_key, o_key_length, callback);
   return OP_RESULT::OK;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::scanAsc(u8* o_key,
                           u16 o_key_length,
                           function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback,
                           function<void()>)
{
   scan<true>(o_key, o_key_length, callback);
   return OP_RESULT::OK;
}
// -------------------------------------------------------------------------------------
std::tuple<OP_RESULT, u16> BTreeVI::reconstructTupleSlowPath(BTreeSharedIterator& iterator,
                                                             MutableSlice key,
                                                             std::function<void(Slice value)> callback)
{
restart : {
   assert(getSN(key) == 0);
   u16 chain_length = 1;
   OP_RESULT ret;
   u16 materialized_value_length;
   std::unique_ptr<u8[]> materialized_value;
   SN secondary_sn;
   {
      Slice primary_payload = iterator.value();
      const PrimaryVersion& primary_version =
          *reinterpret_cast<const PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
      if (isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
         if (primary_version.is_removed) {
            return {OP_RESULT::NOT_FOUND, 1};
         }
         callback(primary_payload.substr(0, primary_payload.length() - sizeof(PrimaryVersion)));
         return {OP_RESULT::OK, 1};
      }
      if (primary_version.isFinal()) {
         return {OP_RESULT::NOT_FOUND, chain_length};
      }
      materialized_value_length = primary_payload.length() - sizeof(PrimaryVersion);
      materialized_value = std::make_unique<u8[]>(materialized_value_length);
      std::memcpy(materialized_value.get(), primary_payload.data(), materialized_value_length);
      secondary_sn = primary_version.next_sn;
   }
   while (secondary_sn != 0) {
      setSN(key, secondary_sn);
      ret = iterator.seekExact(Slice(key.data(), key.length()));
      if (ret != OP_RESULT::OK) {
         setSN(key, 0);
         ret = iterator.seekExact(Slice(key.data(), key.length()));
         ensure(ret == OP_RESULT::OK);
         goto restart;
      }
      chain_length++;
      Slice payload = iterator.value();
      const auto& secondary_version = *reinterpret_cast<const SecondaryVersion*>(payload.data() + payload.length() - sizeof(SecondaryVersion));
      if (secondary_version.is_delta) {
         // Apply delta
         const auto& update_descriptor = *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(payload.data());
         BTreeLL::copyDiffFrom(update_descriptor, materialized_value.get(), payload.data() + update_descriptor.size());
      } else {
         materialized_value_length = payload.length() - sizeof(SecondaryVersion);
         materialized_value = std::make_unique<u8[]>(materialized_value_length);
         std::memcpy(materialized_value.get(), payload.data(), materialized_value_length);
      }
      ensure(!secondary_version.is_removed);
      if (isVisibleForMe(secondary_version.worker_id, secondary_version.tts)) {
         if (secondary_version.is_removed) {
            raise(SIGTRAP);
            return {OP_RESULT::NOT_FOUND, chain_length};
         }
         callback(Slice(materialized_value.get(), materialized_value_length));
         return {OP_RESULT::OK, chain_length};
      }
      if (secondary_version.isFinal()) {
         raise(SIGTRAP);
         return {OP_RESULT::NOT_FOUND, chain_length};
      } else {
         secondary_sn = secondary_version.next_sn;
      }
   }
   raise(SIGTRAP);
   return {OP_RESULT::NOT_FOUND, chain_length};
}
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
