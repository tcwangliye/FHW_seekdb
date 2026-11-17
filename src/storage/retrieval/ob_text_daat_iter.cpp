/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_text_daat_iter.h"
#include <algorithm>

namespace oceanbase
{
namespace storage
{
int ObTextDaaTIter::init(const ObTextDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iters_) || OB_ISNULL(param.allocator_)
      || OB_ISNULL(param.relevance_collector_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObSRDaaTIterImpl::init(*param.base_param_, *param.dim_iters_,
                                            *param.allocator_, *param.relevance_collector_))) {
  } else if (param.base_param_->need_project_relevance()
      && OB_FAIL(bm25_param_estimator_.init(param.bm25_param_est_ctx_))) {
  } else {
    mode_flag_ = param.mode_flag_;
    function_lookup_mode_ = param.function_lookup_mode_;
    use_bitmap_union_ = sql::ObMatchAgainstMode::NATURAL_LANGUAGE_MODE == mode_flag_
        && !function_lookup_mode_ && !iter_param_->need_project_relevance()
        && !iter_param_->need_filter() && iter_param_->eval_ctx_->is_vectorized()
        && iter_param_->id_proj_expr_->is_batch_result()
        && (common::ObUInt64Type == iter_param_->id_proj_expr_->datum_meta_.type_
            || (common::ObVarcharType == iter_param_->id_proj_expr_->datum_meta_.type_
                && common::CS_TYPE_BINARY == iter_param_->id_proj_expr_->datum_meta_.cs_type_
                && OB_DOC_ID_COLUMN_BYTE_LENGTH == iter_param_->id_proj_expr_->max_length_))
        && dim_iters_->count() > 0;
    if (use_bitmap_union_ && OB_FAIL(init_bitmap_union())) {
    }
  }
  return ret;
}

void ObTextDaaTIter::reset()
{
  bitmap_cursors_.reset();
  bitmap_ids_.reset();
  bitmap_results_.reset();
  bitmap_heap_.reset();
  reuse_bitmap_union();
  use_bitmap_union_ = false;
  bm25_param_estimator_.reset();
  // this class can know the type of dim_iters_, but ObSRDaaTIterImpl maybe not
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->reset();
    }
  }
  ObSRDaaTIterImpl::reset();
}

void ObTextDaaTIter::reuse(const bool switch_tablet)
{
  reuse_bitmap_union();
  if ((OB_NOT_NULL(dim_iters_) && 0 == dim_iters_->count())) {
    // do nothing
  } else {
    bm25_param_estimator_.reuse(switch_tablet);
  }
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->reuse();
    }
  }
  ObSRDaaTIterImpl::reuse(switch_tablet);
}

int ObTextDaaTIter::pre_process()
{
  int ret = OB_SUCCESS;
  if (dim_iters_->count() == 0) {
    ret = OB_ITER_END;
  } else if (iter_param_->need_project_relevance()) {
    if (OB_FAIL(bm25_param_estimator_.do_estimation(*iter_param_->eval_ctx_))) {
    }
  }
  return ret;
}

int ObTextDaaTIter::init_bitmap_union()
{
  int ret = OB_SUCCESS;
  const int64_t term_count = dim_iters_->count();
  bitmap_batch_size_ = OB_MAX(iter_param_->max_batch_size_, iter_param_->eval_ctx_->max_batch_size_);
  bitmap_cursors_.set_allocator(iter_allocator_);
  bitmap_ids_.set_allocator(iter_allocator_);
  bitmap_results_.set_allocator(iter_allocator_);
  bitmap_heap_.set_allocator(iter_allocator_);
  if (OB_FAIL(bitmap_cursors_.init(term_count))) {
  } else if (OB_FAIL(bitmap_cursors_.prepare_allocate(term_count))) {
  } else if (OB_FAIL(bitmap_ids_.init(term_count * bitmap_batch_size_))) {
  } else if (OB_FAIL(bitmap_ids_.prepare_allocate(term_count * bitmap_batch_size_))) {
  } else if (OB_FAIL(bitmap_results_.init(bitmap_batch_size_))) {
  } else if (OB_FAIL(bitmap_results_.prepare_allocate(bitmap_batch_size_))) {
  } else if (OB_FAIL(bitmap_heap_.init(term_count))) {
  } else if (OB_FAIL(bitmap_heap_.prepare_allocate(term_count))) {
  } else {
    for (int64_t i = 0; i < term_count; ++i) {
      bitmap_cursors_[i].ids_ = &bitmap_ids_.at(i * bitmap_batch_size_);
    }
  }
  return ret;
}

void ObTextDaaTIter::reuse_bitmap_union()
{
  MEMSET(bitmap_words_, 0, sizeof(bitmap_words_));
  bitmap_nonempty_words_ = 0;
  bitmap_heap_count_ = 0;
  bitmap_initialized_ = false;
}

int ObTextDaaTIter::load_bitmap_cursor(const int64_t index)
{
  int ret = OB_SUCCESS;
  PostingCursor &cursor = bitmap_cursors_.at(index);
  cursor.pos_ = 0;
  cursor.count_ = 0;
  // Each scan shares expression storage. Copy its IDs before fetching
  // another term, retaining only one bounded batch per posting list.
  do {
    ret = static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(index))->get_next_bitmap_ids(
        cursor.ids_, bitmap_batch_size_, cursor.count_);
  } while (OB_SUCC(ret) && 0 == cursor.count_);
  return ret;
}

int ObTextDaaTIter::fill_bitmap_window()
{
  int ret = OB_SUCCESS;
  auto later_id = [this](const int64_t left, const int64_t right) {
    return cursor_id(right) < cursor_id(left);
  };
  int64_t *heap = &bitmap_heap_.at(0);
  if (!bitmap_initialized_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
      if (OB_FAIL(load_bitmap_cursor(i))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      } else {
        heap[bitmap_heap_count_++] = i;
      }
    }
    if (OB_SUCC(ret)) {
      std::make_heap(heap, heap + bitmap_heap_count_, later_id);
      bitmap_initialized_ = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (0 == bitmap_heap_count_) {
    ret = OB_ITER_END;
  } else {
    const PostingCursor &cursor = bitmap_cursors_.at(heap[0]);
    const BitmapDocId &first = cursor.ids_[cursor.pos_];
    const uint64_t difference = first.low_ ^ cursor.ids_[cursor.count_ - 1].low_;
    // Select the least-significant changing byte. This also compresses the
    // native binary IDs, whose sequence bytes need not be in numeric order.
    bitmap_shift_ = difference == 0 ? 0 : MIN(52, (__builtin_ctzll(difference) / 8) * 8);
    bitmap_high_ = first.high_;
    bitmap_prefix_ = first.low_ & ~(UINT64_C(4095) << bitmap_shift_);
    while (OB_SUCC(ret) && bitmap_heap_count_ > 0
           && in_bitmap_window(cursor_id(heap[0]))) {
      ret = absorb_bitmap_cursor();
    }
  }
  return ret;
}

int ObTextDaaTIter::absorb_bitmap_cursor()
{
  int ret = OB_SUCCESS;
  auto later_id = [this](const int64_t left, const int64_t right) {
    return cursor_id(right) < cursor_id(left);
  };
  int64_t *heap = &bitmap_heap_.at(0);
  const int64_t index = heap[0];
  std::pop_heap(heap, heap + bitmap_heap_count_, later_id);
  --bitmap_heap_count_;
  PostingCursor &cursor = bitmap_cursors_.at(index);
  bool exhausted = false;
  while (OB_SUCC(ret) && !exhausted) {
    while (cursor.pos_ < cursor.count_ && in_bitmap_window(cursor.ids_[cursor.pos_])) {
      const uint64_t slot = (cursor.ids_[cursor.pos_++].low_ >> bitmap_shift_) & 4095;
      const uint64_t word = slot >> 6;
      bitmap_words_[word] |= UINT64_C(1) << (slot & 63);
      bitmap_nonempty_words_ |= UINT64_C(1) << word;
    }
    if (cursor.pos_ < cursor.count_) {
      break;
    } else if (OB_FAIL(load_bitmap_cursor(index))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        exhausted = true;
      }
    }
  }
  if (OB_SUCC(ret) && !exhausted) {
    heap[bitmap_heap_count_++] = index;
    std::push_heap(heap, heap + bitmap_heap_count_, later_id);
  }
  return ret;
}

int ObTextDaaTIter::take_heap_id(BitmapDocId &id)
{
  int ret = OB_SUCCESS;
  auto later_id = [this](const int64_t left, const int64_t right) {
    return cursor_id(right) < cursor_id(left);
  };
  int64_t *heap = &bitmap_heap_.at(0);
  id = cursor_id(heap[0]);
  do {
    const int64_t index = heap[0];
    std::pop_heap(heap, heap + bitmap_heap_count_, later_id);
    --bitmap_heap_count_;
    PostingCursor &cursor = bitmap_cursors_.at(index);
    ++cursor.pos_;
    bool exhausted = false;
    if (cursor.pos_ == cursor.count_ && OB_FAIL(load_bitmap_cursor(index))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        exhausted = true;
      }
    }
    if (OB_SUCC(ret) && !exhausted) {
      heap[bitmap_heap_count_++] = index;
      std::push_heap(heap, heap + bitmap_heap_count_, later_id);
    }
  } while (OB_SUCC(ret) && bitmap_heap_count_ > 0 && cursor_id(heap[0]) == id);
  return ret;
}

int ObTextDaaTIter::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (!use_bitmap_union_) {
    ret = ObSRDaaTIterImpl::get_next_rows(capacity, count);
  } else {
    count = 0;
    const int64_t real_capacity = MIN(capacity, iter_param_->max_batch_size_);
    if (iter_param_->limit_param_->is_valid()
        && output_row_cnt_ >= iter_param_->limit_param_->limit_) {
      ret = OB_ITER_END;
    }
    while (OB_SUCC(ret) && count < real_capacity) {
      if (0 == bitmap_nonempty_words_ && OB_FAIL(fill_bitmap_window())) {
      } else {
        while (OB_SUCC(ret) && bitmap_heap_count_ > 0
               && in_bitmap_window(cursor_id(bitmap_heap_[0]))) {
          ret = absorb_bitmap_cursor();
        }
        if (OB_SUCC(ret)) {
          const uint64_t word = __builtin_ctzll(bitmap_nonempty_words_);
          const uint64_t bit = __builtin_ctzll(bitmap_words_[word]);
          BitmapDocId next;
          next.high_ = bitmap_high_;
          next.low_ = bitmap_prefix_ | (((word << 6) | bit) << bitmap_shift_);
          // A foreign key may fall between bitmap entries. Merge it in order;
          // do not assume that every ID has the same suffix or tablet prefix.
          if (bitmap_heap_count_ > 0 && cursor_id(bitmap_heap_[0]) < next) {
            ret = take_heap_id(next);
          } else {
            bitmap_words_[word] &= bitmap_words_[word] - 1;
            if (0 == bitmap_words_[word]) {
              bitmap_nonempty_words_ &= ~(UINT64_C(1) << word);
            }
          }
          if (OB_SUCC(ret)) {
            ++input_row_cnt_;
            const common::ObLimitParam &limit = *iter_param_->limit_param_;
            if (!limit.is_valid() || input_row_cnt_ > limit.offset_) {
              bitmap_results_[count++] = next;
              ++output_row_cnt_;
              if (limit.is_valid() && output_row_cnt_ >= limit.limit_) {
                ret = OB_ITER_END;
              }
            }
          }
        }
      }
    }
    if ((OB_SUCC(ret) || OB_ITER_END == ret) && count > 0) {
      // Posting scans share expression frames. Project only after the batch
      // is complete, using one compact buffer instead of generic row copies.
      sql::ObExpr *expr = iter_param_->id_proj_expr_;
      sql::ObEvalCtx &ctx = *iter_param_->eval_ctx_;
      ObDatum *datums = expr->locate_datums_for_update(ctx, count);
      if (common::ObUInt64Type == expr->datum_meta_.type_) {
        for (int64_t i = 0; i < count; ++i) {
          datums[i].set_uint(bitmap_results_[i].low_);
        }
      } else {
        static_assert(sizeof(BitmapDocId) == OB_DOC_ID_COLUMN_BYTE_LENGTH,
            "bitmap result buffer must hold one binary document ID");
        for (int64_t i = 0; i < count; ++i) {
          const BitmapDocId id = bitmap_results_[i];
          char *bytes = reinterpret_cast<char *>(&bitmap_results_[i]);
          id.to_binary(bytes);
          datums[i].set_string(bytes, OB_DOC_ID_COLUMN_BYTE_LENGTH);
        }
      }
      expr->get_evaluated_flags(ctx).set_all(count);
      expr->set_evaluated_projected(ctx);
    }
  }
  return ret;
}

ObTextBMWIter::ObTextBMWIter()
  : ObSRBMWIterImpl(),
    bm25_param_estimator_() {}

void ObTextBMWIter::reuse(const bool switch_tablet)
{
  bm25_param_estimator_.reuse(switch_tablet);
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i))->reuse();
    }
  }
  ObSRBMWIterImpl::reuse(switch_tablet);
}

void ObTextBMWIter::reset()
{
  bm25_param_estimator_.reset();
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i))->reset();
    }
  }
  ObSRBMWIterImpl::reset();
}

int ObTextBMWIter::init(const ObTextDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iters_) || OB_ISNULL(param.allocator_)
      || OB_ISNULL(param.relevance_collector_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObSRBMWIterImpl::init(*param.base_param_, *param.dim_iters_,
                                           *param.allocator_, *param.relevance_collector_))) {
  } else if (OB_FAIL(bm25_param_estimator_.init(param.bm25_param_est_ctx_))) {
  }
  return ret;
}

int ObTextBMWIter::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (dim_iters_->count() == 0) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(bm25_param_estimator_.do_estimation(*iter_param_->eval_ctx_))) {
  }

  if (FAILEDx(ObSRBMWIterImpl::get_next_rows(capacity, count))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
    }
  }
  return ret;
}

int ObTextBMWIter::init_before_wand_process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!bm25_param_estimator_.is_estimated())) {
    ret = OB_ERR_UNEXPECTED;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
    ObTextRetrievalBlockMaxIter *block_max_iter = static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i));
    if (OB_FAIL(block_max_iter->init_block_max_iter(
        bm25_param_estimator_.get_total_doc_cnt(), bm25_param_estimator_.get_avg_doc_token_cnt()))) {
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
