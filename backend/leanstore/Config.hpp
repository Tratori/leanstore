#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
DECLARE_double(dram_gib); // 1 GiB
DECLARE_uint32(ssd); // 10 GiB
DECLARE_string(ssd_path);
DECLARE_string(free_pages_list_path);
DECLARE_uint32(cool);
DECLARE_uint32(free);
DECLARE_uint32(partition_bits);
DECLARE_uint32(async_batch_size);
DECLARE_uint32(falloc);
DECLARE_bool(trunc);
DECLARE_bool(print_debug);
DECLARE_uint32(print_debug_interval_s);
// -------------------------------------------------------------------------------------
