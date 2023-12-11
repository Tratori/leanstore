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
   Hist<int, u64> heatmapHist{4096, 0, 4096};
   Hist<int, u64> writeHist{4096, 0, 4096};
   Hist<int, u64> numWriteHist{1000, 0, 1000};
   std::atomic<u64> totalEvicts = 0;
   void reset()
   {
      totalEvicts = 0;
      writeHist.resetData();
      heatmapHist.resetData();
   };
   static BFCounters bfCounters;
   void printStats();
};
void addWriteHeatmap(u64 start, int size);
void addWriteSize(int size);
void addNumWrites(int numWrites);
}  // namespace leanstore