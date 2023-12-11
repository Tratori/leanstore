// -------------------------------------------------------------------------------------
#include "BFCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
// -------------------------------------------------------------------------------------
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
std::mutex mutex;
std::mutex update_heatmap_mutex;
std::mutex update_write_hist_mutex;
std::mutex update_num_write_hist_mutex;
BFCounters BFCounters::bfCounters;
void addHist(Hist<int, u64>& hist)
{
   std::unique_lock guard(mutex);

   BFCounters::bfCounters.writeHist += hist;
}

void addWriteHeatmap(u64 start, int size)
{
   std::unique_lock guard(update_heatmap_mutex);
   for (int i = 0; i < size; i++) {
      BFCounters::bfCounters.heatmapHist.increaseSlot(start + i);
   }
}

void addWriteSize(int size)
{
   std::unique_lock guard(update_write_hist_mutex);
   BFCounters::bfCounters.writeHist.increaseSlot(size);
}

void BFCounters::printStats()
{
   std::cout << "writeHist of BFCounters:" << std::endl;
   BFCounters::bfCounters.writeHist.print();
   std::cout << std::endl << "heatmapHist of BFCounters:" << std::endl;
   BFCounters::bfCounters.heatmapHist.print();
   std::cout << std::endl << "Total totalEvicts" << BFCounters::bfCounters.totalEvicts << std::endl;
   std::cout << "numWriteHist of BFCounters" << std::endl;
   BFCounters::bfCounters.numWriteHist.print();
   std::cout << std::endl;
}

void addNumWrites(int numWrites)
{
   std::unique_lock guard(update_num_write_hist_mutex);
   BFCounters::bfCounters.numWriteHist.increaseSlot(numWrites);
}

}  // namespace leanstore