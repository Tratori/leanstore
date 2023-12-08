#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "PerfEvent.hpp"
#include "leanstore/utils/Hist.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
// -------------------------------------------------------------------------------------

namespace leanstore
{

struct BFCounters {
   Hist<int, u64> writeHist{100, 0, 4 * 1024};
   Hist<int, u64> updatesHist{100, 0, 100};
   Hist<int, u64> insertsHist{100, 0, 100};
   Hist<int, u64> removesHist{100, 0, 100};
   std::atomic<u64> updates = 0;
   std::atomic<u64> inserts = 0;
   std::atomic<u64> removes = 0;
   std::atomic<u64> totalEvicts = 0;
   void reset()
   {
      updates = 0;
      inserts = 0;
      removes = 0;
      totalEvicts = 0;
      writeHist.resetData();
      updatesHist.resetData();
      insertsHist.resetData();
      removesHist.resetData();
   };
   static BFCounters bfCounters;
};

void addHist(Hist<int, u64>& hist);
void addUpdatesHist(u64 n);
void addInsertsHist(u64 n);
void addRemovesHist(u64 n);

}  // namespace leanstore
