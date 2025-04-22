#pragma once

#include "runner.hpp"

/**
 * Profile how much the allocation costs when considering the minimum string size that cannot be inlined into
 * a Vector but requires a buffer allocation.
 */
void BM_allocate_empty_string_12b_vector_buffer(const State &state);

/**
 * Profile how much the allocation costs when considering the maximum possible decompressed string size.
 */
void BM_allocate_empty_string_max_vector_buffer(const State &state);