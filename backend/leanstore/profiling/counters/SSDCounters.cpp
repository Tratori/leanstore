#include "SSDCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
tbb::enumerable_thread_specific<SSDCounters> SSDCounters::ssd_counters;
}  // namespace leanstore