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

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_DEF_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_DEF_H_

#include "lib/charset/ob_charset.h"

#include <cstdint>

namespace oceanbase
{
namespace storage
{
typedef int32_t ObFTWordCode;
typedef int32_t ObFTWordBase;
typedef uint32_t ObFTWordStateIndex;

/**
 * @class ObFTSingleWord
 * @brief sturct to store a single character of a charset;
 */
struct ObFTSingleWord
{
public:
  ObFTSingleWord() : word(""), word_len(0) {}
  ObFTSingleWord(const ObFTSingleWord &other) = default;
  ObFTSingleWord &operator=(const ObFTSingleWord &other) = default;

  int32_t set_word(const char *word, int32_t word_len);
  ObString get_word() const;
  bool operator==(const ObFTSingleWord &other) const;

  static constexpr uint32_t UNICODE_PAGE_COUNT = 0x1100;
  static constexpr uint32_t UNICODE_PAGE_WORDS = UNICODE_PAGE_COUNT / 64;

  // Canonical UTF-8 is a one-to-one encoding: direct lookup preserves the
  // original byte-wise character equality, including embedded NUL characters.
  static bool decode_utf8(const ObString &value, uint32_t &codepoint)
  {
    const uint8_t *s = reinterpret_cast<const uint8_t *>(value.ptr());
    const int32_t len = value.length();
    bool valid = false;
    if (1 == len && s[0] < 0x80) {
      codepoint = s[0];
      valid = true;
    } else if (2 == len && s[0] >= 0xc2 && s[0] <= 0xdf
               && (s[1] & 0xc0) == 0x80) {
      codepoint = ((s[0] & 0x1f) << 6) | (s[1] & 0x3f);
      valid = true;
    } else if (3 == len && s[0] >= 0xe0 && s[0] <= 0xef
               && (s[1] & 0xc0) == 0x80 && (s[2] & 0xc0) == 0x80) {
      codepoint = ((s[0] & 0x0f) << 12) | ((s[1] & 0x3f) << 6) | (s[2] & 0x3f);
      valid = codepoint >= 0x800 && (codepoint < 0xd800 || codepoint > 0xdfff);
    } else if (4 == len && s[0] >= 0xf0 && s[0] <= 0xf4
               && (s[1] & 0xc0) == 0x80 && (s[2] & 0xc0) == 0x80
               && (s[3] & 0xc0) == 0x80) {
      codepoint = ((s[0] & 7) << 18) | ((s[1] & 0x3f) << 12)
          | ((s[2] & 0x3f) << 6) | (s[3] & 0x3f);
      valid = codepoint >= 0x10000 && codepoint <= 0x10ffff;
    }
    return valid;
  }

public:
  char word[common::ObCharset::MAX_MB_LEN];
  uint8_t word_len;
} __attribute__((packed));

enum class ObFTDictType : uint32_t
{
  DICT_TYPE_INVALID = 0,
  DICT_IK_MAIN = 1,
  DICT_IK_QUAN = 2,
  DICT_IK_STOP = 3,
};

class ObFTDictDesc
{
public:
  ObFTDictDesc(const ObString &name,
               const ObFTDictType type,
               const ObCharsetType charset,
               const ObCollationType coll_type)
      : name_(name), type_(type), charset_(charset), coll_type_(coll_type)
  {
  }

public:
  ObString name_;
  ObFTDictType type_;
  ObCharsetType charset_;
  ObCollationType coll_type_;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_DEF_H_
