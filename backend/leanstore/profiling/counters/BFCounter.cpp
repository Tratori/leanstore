// -------------------------------------------------------------------------------------
#include "BFCounter.hpp"
// -------------------------------------------------------------------------------------
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
std::mutex mutex;
std::mutex update_hist_mutex;
std::mutex insert_hist_mutex;
std::mutex remove_hist_mutex;
BFCounters BFCounters::bfCounters;
void addHist(Hist<int, u64>& hist)
{
   std::unique_lock guard(mutex);

   BFCounters::bfCounters.writeHist += hist;
}

void addUpdatesHist(u64 n)
{
   std::unique_lock guard(update_hist_mutex);

   BFCounters::bfCounters.updatesHist.increaseSlot(n);
}
void addInsertsHist(u64 n)
{
   std::unique_lock guard(insert_hist_mutex);

   BFCounters::bfCounters.insertsHist.increaseSlot(n);
}
void addRemovesHist(u64 n)
{
   std::unique_lock guard(remove_hist_mutex);

   BFCounters::bfCounters.writeHist.increaseSlot(n);
}

}  // namespace leanstore
