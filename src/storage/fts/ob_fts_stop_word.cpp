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

#include "object/ob_object.h"
#define USING_LOG_PREFIX STORAGE_FTS

#include "share/rc/ob_server_runtime.h"
#include "storage/fts/ob_fts_stop_word.h"
#include "storage/fts/ob_fts_parser_helper.h"
#include "storage/fts/ob_fts_parser_property.h"

namespace oceanbase
{
namespace storage
{

namespace
{
// The built-in list is immutable. ASCII UTF-8 words have the same folded
// letter weights under its general_ci collation; other inputs use that
// collation's full implementation below.
struct AsciiStopwordIndex
{
  static constexpr uint64_t SIZE = 512;
  static constexpr int64_t MAX_LEN = 32;
  struct Entry
  {
    uint64_t hash_;
    const char *word_;
    uint8_t len_;
  };
  Entry entries_[SIZE] = {};
  bool compatible_ = true;

  static uint64_t hash_word(const char *value, const int64_t length)
  {
    uint64_t hash = 1469598103934665603ULL;
    for (int64_t i = 0; i < length; ++i) {
      hash ^= static_cast<unsigned char>(value[i]) | 0x20;
      hash *= 1099511628211ULL;
    }
    return hash;
  }

  static bool same_letters(const char *left, const char *right, const int64_t length)
  {
    bool same = true;
    for (int64_t i = 0; same && i < length; ++i) {
      same = (static_cast<unsigned char>(left[i]) | 0x20)
          == (static_cast<unsigned char>(right[i]) | 0x20);
    }
    return same;
  }

  static constexpr uint64_t bucket(const uint64_t hash)
  {
    return (hash * 11400714819323198485ULL) >> 55;
  }

  AsciiStopwordIndex()
  {
    for (const auto &word : ob_stop_word_list) {
      int64_t length = 0;
      for (; word[length] != '\0'; ++length) {
        const unsigned char ch = static_cast<unsigned char>(word[length]) | 0x20;
        if (ch < 'a' || ch > 'z' || length >= MAX_LEN) {
          compatible_ = false;
          break;
        }
      }
      if (compatible_ && length > 0) {
        const uint64_t hash = hash_word(word, length);
        uint64_t pos = bucket(hash);
        while (entries_[pos].len_ != 0 && entries_[pos].hash_ != hash) {
          pos = (pos + 1) & (SIZE - 1);
        }
        entries_[pos].hash_ = hash;
        entries_[pos].word_ = word;
        entries_[pos].len_ = static_cast<uint8_t>(length);
      }
    }
  }

  // Handles pure ASCII words of any length, so long words no longer fall back
  // to a temporary allocator plus charset conversion for a hash lookup.
  bool check(const ObFTWord &word, bool &found) const
  {
    bool handled = compatible_
        && ObCharset::charset_type_by_coll(word.get_collation_type()) == CHARSET_UTF8MB4;
    if (handled) {
      const ObString value = word.get_word().get_string();
      handled = value.length() > 0 && value.length() <= MAX_LEN;
      for (int64_t i = 0; handled && i < value.length(); ++i) {
        const unsigned char ch = static_cast<unsigned char>(value.ptr()[i]) | 0x20;
        if (ch < 'a' || ch > 'z') {
          handled = false;
        }
      }
      if (handled) {
        const uint64_t hash = hash_word(value.ptr(), value.length());
        uint64_t pos = bucket(hash);
        found = false;
        while (entries_[pos].len_ != 0) {
          if (entries_[pos].hash_ == hash
              && entries_[pos].len_ == value.length()
              && same_letters(entries_[pos].word_, value.ptr(), value.length())) {
            found = true;
            break;
          }
          pos = (pos + 1) & (SIZE - 1);
        }
      }
    }
    return handled;
  }
};
const AsciiStopwordIndex ascii_stopwords;
} // namespace

////////////////////////////////////////////////////////////////////////////////
// class ObStopWordChecker
ObStopWordChecker::~ObStopWordChecker()
{
  destroy();
}

int ObStopWordChecker::init()
{
  int ret = OB_SUCCESS;
  
  

  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(stopword_set_.create(DEFAULT_STOPWORD_BUCKET_NUM, "StopWordSet", "StopWordSet"))) {
  } else {
    ObObjMeta stop_meta;
    stop_meta.set_varchar();
    stop_meta.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);

    stopword_type_.set_meta(stop_meta);
    const int64_t stopword_count = sizeof(ob_stop_word_list) / sizeof(ob_stop_word_list[0]);
    for (int64_t i = 0; OB_SUCC(ret) && i < stopword_count; ++i) {
      ObFTWord stopword(STRLEN(ob_stop_word_list[i]), ob_stop_word_list[i], stopword_type_);
      if (OB_FAIL(stopword_set_.set_refactored(stopword))) {
      }
    }

    if (OB_SUCC(ret)) {
      inited_ = true;
    }
  }
  return ret;
}

void ObStopWordChecker::destroy()
{
  if (inited_) {
    stopword_set_.destroy();
    inited_ = false;
  }
}

int ObStopWordChecker::check_stopword(const ObFTWord &word, bool &is_stopword)
{
  int ret = OB_SUCCESS;
  
  
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(word.empty())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (ascii_stopwords.check(word, is_stopword)) {
  } else if (word.get_collation_type() == stopword_type_.get_collation_type()) {
    // The word already uses the stop-word collation, so probe the set directly;
    // converting it through a per-token arena is pure overhead here.
    ret = stopword_set_.exist_refactored(word);
    if (OB_HASH_NOT_EXIST == ret) {
      is_stopword = false;
      ret = OB_SUCCESS;
    } else if (OB_HASH_EXIST == ret) {
      is_stopword = true;
      ret = OB_SUCCESS;
    } else if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    common::ObArenaAllocator allocator(lib::ObMemAttr("ChkStopWord"));
    common::ObString cmp_str;
    // do nothing set out with in if type is the same.
    if (OB_FAIL(common::ObCharset::charset_convert(
                                       allocator,
                                       word.get_word().get_string(),
                                       word.get_collation_type(),
                                       stopword_type_.get_collation_type(),
                                       cmp_str))) {
    } else {
      ObFTWord converted(cmp_str.length(), cmp_str.ptr(), stopword_type_);
      ret = stopword_set_.exist_refactored(converted);
      if (OB_HASH_NOT_EXIST == ret) {
        is_stopword = false;
        ret = OB_SUCCESS;
      } else if (OB_HASH_EXIST == ret) {
        is_stopword = true;
        ret = OB_SUCCESS;
      } else if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// class ObAddWord
ObAddWord::ObAddWord(
    const ObFTParserProperty &property,
    const ObObjMeta &meta,
    const ObAddWordFlag &flag,
    common::ObIAllocator &allocator,
    ObFTWordMap &word_map,
    const char *fulltext,
    int64_t fulltext_len)
  : word_meta_(meta),
    allocator_(allocator),
    word_map_(word_map),
    min_max_word_cnt_(0),
    non_stopword_cnt_(0),
    stopword_cnt_(0),
    min_token_size_(property.min_token_size_),
    max_token_size_(property.max_token_size_),
    flag_(flag),
    fulltext_(fulltext),
    fulltext_len_(fulltext_len),
    shared_fulltext_(nullptr),
    can_share_fulltext_(nullptr != fulltext && fulltext_len > 0
        && ObCharset::charset_type_by_coll(meta.get_collation_type()) == CHARSET_UTF8MB4)
{
}

int ObAddWord::process_word(
    const char *word,
    const int64_t word_len,
    const int64_t char_cnt,
    const int64_t word_freq)
{
  int ret = OB_SUCCESS;
  bool is_stopword = false;
  ObFTWord src_word(word_len, word, word_meta_);
  ObFTWord dst_word;
  if (OB_ISNULL(word) || OB_UNLIKELY(0 >= word_len || 0 >= char_cnt || 0 >= word_freq)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_min_max_word(char_cnt)) {
    ++min_max_word_cnt_;
  } else if (OB_FAIL(casedown_word(src_word, dst_word))) {
  } else if (OB_FAIL(check_stopword(dst_word, is_stopword))) {
  } else if (OB_UNLIKELY(is_stopword)) {
    ++stopword_cnt_;
  } else if (OB_FAIL(groupby_word(dst_word, word_freq))) {
  } else {
    non_stopword_cnt_ += word_freq;
  }
  return ret;
}

bool ObAddWord::is_min_max_word(const int64_t c_len) const
{
  return flag_.min_max_word() && (c_len < min_token_size_ || c_len > max_token_size_);
}

bool ObAddWord::can_share_unchanged_word(const ObFTWord &word) const
{
  if (!can_share_fulltext_) {
    return false;
  }
  const ObString value = word.get_word().get_string();
  const uintptr_t begin = reinterpret_cast<uintptr_t>(fulltext_);
  const uintptr_t address = reinterpret_cast<uintptr_t>(value.ptr());
  if (address < begin || address - begin > static_cast<uint64_t>(fulltext_len_)
      || value.length() > fulltext_len_ - static_cast<int64_t>(address - begin)) {
    return false;
  }
  const unsigned char *bytes = reinterpret_cast<const unsigned char *>(value.ptr());
  for (int64_t i = 0; i < value.length();) {
    const unsigned char ch = bytes[i];
    if (ch < 0x80 && !(ch >= 'A' && ch <= 'Z')) {
      ++i;
    } else if (ch >= 0xE4 && ch <= 0xE9 && i + 2 < value.length()
        && (bytes[i + 1] & 0xC0) == 0x80 && (bytes[i + 2] & 0xC0) == 0x80
        && (ch != 0xE4 || bytes[i + 1] >= 0xB8)) {
      // U+4E00..U+9FFF have no lowercase mapping.
      i += 3;
    } else {
      return false;
    }
  }
  return true;
}

int ObAddWord::casedown_word(const ObFTWord &src, ObFTWord &dst)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(src.empty())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (flag_.casedown() && can_share_unchanged_word(src)) {
    // Own one bounded copy of a small document. Token pointers must remain
    // valid after segment() returns, including when the input is a LOB view.
    if (nullptr == shared_fulltext_) {
      ObString owned;
      if (OB_FAIL(ob_write_string(allocator_, ObString(fulltext_len_, fulltext_), owned))) {
      } else {
        shared_fulltext_ = owned.ptr();
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t offset = reinterpret_cast<uintptr_t>(src.get_word().ptr_)
          - reinterpret_cast<uintptr_t>(fulltext_);
      dst = ObFTWord(src.get_word().len_, shared_fulltext_ + offset, word_meta_);
    }
  } else if (flag_.casedown()) {
    ObString dst_str;
    if (OB_FAIL(ObCharset::tolower(
                    word_meta_.get_collation_type(),
                    src.get_word().get_string(),
                    dst_str,
                    allocator_))) {
    } else {
      ObFTWord tmp(dst_str.length(), dst_str.ptr(), word_meta_);
      dst = tmp;
    }
  } else {
    dst = src;
  }
  return ret;
}

int ObAddWord::check_stopword(const ObFTWord &ft_word, bool &is_stopword)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ft_word.empty())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (flag_.stopword()) {
    ObStopWordChecker *stop_word_checker = ObFTParseData::instance().stop_word_checker();
    if (OB_ISNULL(stop_word_checker)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(stop_word_checker->check_stopword(ft_word, is_stopword))) {
    }
  }
  return ret;
}

int ObAddWord::groupby_word(const ObFTWord &word, const int64_t word_freq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(word.empty() || word_freq <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!flag_.groupby_word()) {
    if (OB_FAIL(word_map_.set_refactored(word, 1/*word count*/))) {
    }
  } else {
    auto add_frequency = [word_freq](auto &entry) {
      entry.second += word_freq;
    };
    if (OB_FAIL(word_map_.set_or_update(word, 1/*word count*/, add_frequency))) {
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
