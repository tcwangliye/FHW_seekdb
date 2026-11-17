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

#ifndef OB_TEXT_DAAT_ITER_H_
#define OB_TEXT_DAAT_ITER_H_

#include "query/ob_sql_define.h"
#include "ob_inv_idx_param_estimator.h"
#include "ob_sparse_bmw_iter.h"
#include "ob_sparse_daat_iter.h"
#include "ob_text_retrieval_token_iter.h"

namespace oceanbase
{
namespace sql
{
  class ObDASScanIter;
}
namespace storage
{

struct ObTextDaaTParam
{
  ObTextDaaTParam()
    : dim_iters_(nullptr), base_param_(nullptr), allocator_(nullptr), relevance_collector_(nullptr),
      bm25_param_est_ctx_(), mode_flag_(sql::ObMatchAgainstMode::NATURAL_LANGUAGE_MODE),
      function_lookup_mode_(false) {}
  ~ObTextDaaTParam() {}
  TO_STRING_KV(K_(base_param), KP_(dim_iters), K_(bm25_param_est_ctx),  K_(mode_flag), K_(function_lookup_mode));
  ObIArray<ObISRDaaTDimIter *> *dim_iters_;
  ObSparseRetrievalMergeParam *base_param_;
  common::ObArenaAllocator *allocator_;
  ObSRDaaTRelevanceCollector *relevance_collector_;
  ObBM25ParamEstCtx bm25_param_est_ctx_;
  sql::ObMatchAgainstMode mode_flag_;
  bool function_lookup_mode_;
};

class ObTextDaaTIter final : public ObSRDaaTIterImpl
{
public:
  ObTextDaaTIter() : ObSRDaaTIterImpl(),
      bm25_param_estimator_(),
      mode_flag_(sql::ObMatchAgainstMode::NATURAL_LANGUAGE_MODE),
      function_lookup_mode_(false),
      bitmap_cursors_(), bitmap_ids_(), bitmap_results_(), bitmap_heap_(), bitmap_words_(),
      bitmap_nonempty_words_(0), bitmap_prefix_(0), bitmap_high_(0), bitmap_shift_(0),
      bitmap_heap_count_(0),
      bitmap_batch_size_(0), use_bitmap_union_(false), bitmap_initialized_(false) {}
  virtual ~ObTextDaaTIter() { reset(); }

  int init(const ObTextDaaTParam &param);
  virtual int get_next_rows(const int64_t capacity, int64_t &count) override;
  virtual void reuse(const bool switch_tablet = false) override;
  virtual void reset() override;
protected:
  virtual int pre_process() override;
protected:
  ObBM25ParamEstimator bm25_param_estimator_;
  sql::ObMatchAgainstMode mode_flag_;
  bool function_lookup_mode_;
private:
  struct PostingCursor
  {
    BitmapDocId *ids_ = nullptr;
    int64_t count_ = 0;
    int64_t pos_ = 0;
    TO_STRING_KV(KP_(ids), K_(count), K_(pos));
  };
  int init_bitmap_union();
  int load_bitmap_cursor(const int64_t index);
  int fill_bitmap_window();
  int absorb_bitmap_cursor();
  int take_heap_id(BitmapDocId &id);
  void reuse_bitmap_union();
  const BitmapDocId &cursor_id(const int64_t index) const
  {
    const PostingCursor &cursor = bitmap_cursors_.at(index);
    return cursor.ids_[cursor.pos_];
  }
  bool in_bitmap_window(const BitmapDocId &id) const
  {
    return id.high_ == bitmap_high_
        && (id.low_ & ~(UINT64_C(4095) << bitmap_shift_)) == bitmap_prefix_;
  }
  // An adaptive 4096-bit slice compresses both integer and binary document IDs.
  // Interleaved keys stay in the heap and merge with the pending bitmap, so the
  // output retains binary key order even across arbitrary IDs and tablets.
  ObFixedArray<PostingCursor, ObIAllocator> bitmap_cursors_;
  ObFixedArray<BitmapDocId, ObIAllocator> bitmap_ids_;
  ObFixedArray<BitmapDocId, ObIAllocator> bitmap_results_;
  ObFixedArray<int64_t, ObIAllocator> bitmap_heap_;
  uint64_t bitmap_words_[64];
  uint64_t bitmap_nonempty_words_;
  uint64_t bitmap_prefix_;
  uint64_t bitmap_high_;
  uint32_t bitmap_shift_;
  int64_t bitmap_heap_count_;
  int64_t bitmap_batch_size_;
  bool use_bitmap_union_;
  bool bitmap_initialized_;
  DISALLOW_COPY_AND_ASSIGN(ObTextDaaTIter);
};


class ObTextBMWIter final : public ObSRBMWIterImpl
{
public:
  ObTextBMWIter();
  virtual ~ObTextBMWIter() { reset(); }
  virtual void reuse(const bool switch_tablet = false) override;
  virtual void reset() override;
  int init(const ObTextDaaTParam &param);
protected:
  virtual int get_next_rows(const int64_t capacity, int64_t &count) override;
  virtual int init_before_wand_process() override;
protected:
  ObBM25ParamEstimator bm25_param_estimator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTextBMWIter);
};

} // namespace storage
} // namespace oceanbase

#endif
