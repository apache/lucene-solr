/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.analysis.cn.smart.hhmm;

import java.io.UnsupportedEncodingException;

/**
 * <p>
 * SmartChineseAnalyzer abstract dictionary implementation.
 * </p>
 * <p>
 * Contains methods for dealing with GB2312 encoding.
 * </p>
 * @lucene.experimental
 */
abstract class AbstractDictionary {
  /**
   * First Chinese Character in GB2312 (15 * 94)
   * Characters in GB2312 are arranged in a grid of 94 * 94, 0-14 are unassigned or punctuation.
   */
  public static final int GB2312_FIRST_CHAR = 1410;

  /**
   * Last Chinese Character in GB2312 (87 * 94). 
   * Characters in GB2312 are arranged in a grid of 94 * 94, 88-94 are unassigned.
   */
  public static final int GB2312_CHAR_NUM = 87 * 94;

  /**
   * Dictionary data contains 6768 Chinese characters with frequency statistics.
   */
  public static final int CHAR_NUM_IN_FILE = 6768;

  // =====================================================
  // code +0 +1 +2 +3 +4 +5 +6 +7 +8 +9 +A +B +C +D +E +F
  // B0A0 啊 阿 埃 挨 哎 唉 哀 皑 癌 蔼 矮 艾 碍 爱 隘
  // B0B0 鞍 氨 安 俺 按 暗 岸 胺 案 肮 昂 盎 凹 敖 熬 翱
  // B0C0 袄 傲 奥 懊 澳 芭 捌 扒 叭 吧 笆 八 疤 巴 拔 跋
  // B0D0 靶 把 耙 坝 霸 罢 爸 白 柏 百 摆 佰 败 拜 稗 斑
  // B0E0 班 搬 扳 般 颁 板 版 扮 拌 伴 瓣 半 办 绊 邦 帮
  // B0F0 梆 榜 膀 绑 棒 磅 蚌 镑 傍 谤 苞 胞 包 褒 剥
  // =====================================================
  //
  // GB2312 character set：
  // 01 94 Symbols
  // 02 72 Numbers
  // 03 94 Latin
  // 04 83 Kana
  // 05 86 Katakana
  // 06 48 Greek
  // 07 66 Cyrillic
  // 08 63 Phonetic Symbols
  // 09 76 Drawing Symbols
  // 10-15 Unassigned
  // 16-55 3755 Plane 1, in pinyin order
  // 56-87 3008 Plane 2, in radical/stroke order
  // 88-94 Unassigned
  // ======================================================

  /**
   * <p>
   * Transcode from GB2312 ID to Unicode
   * </p>
   * <p>
   * GB2312 is divided into a 94 * 94 grid, containing 7445 characters consisting of 6763 Chinese characters and 682 symbols.
   * Some regions are unassigned (reserved).
   * </p>
   * 
   * @param ccid GB2312 id
   * @return unicode String
   */
  public String getCCByGB2312Id(int ccid) {
    if (ccid < 0 || ccid > AbstractDictionary.GB2312_CHAR_NUM)
      return "";
    int cc1 = ccid / 94 + 161;
    int cc2 = ccid % 94 + 161;
    byte[] buffer = new byte[2];
    buffer[0] = (byte) cc1;
    buffer[1] = (byte) cc2;
    try {
      String cchar = new String(buffer, "GB2312");
      return cchar;
    } catch (UnsupportedEncodingException e) {
      return "";
    }
  }

  /**
   * Transcode from Unicode to GB2312
   * 
   * @param ch input character in Unicode, or character in Basic Latin range.
   * @return position in GB2312
   */
  public short getGB2312Id(char ch) {
    try {
      byte[] buffer = Character.toString(ch).getBytes("GB2312");
      if (buffer.length != 2) {
        // Should be a two-byte character
        return -1;
      }
      int b0 = (buffer[0] & 0x0FF) - 161; // Code starts from A1, therefore subtract 0xA1=161
      int b1 = (buffer[1] & 0x0FF) - 161; // There is no Chinese char for the first and last symbol. 
                                          // Therefore, each code page only has 16*6-2=94 characters.
      return (short) (b0 * 94 + b1);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * 32-bit FNV Hash Function
   * 
   * @param c input character
   * @return hashcode
   */
  public long hash1(char c) {
    final long p = 1099511628211L;
    long hash = 0xcbf29ce484222325L;
    hash = (hash ^ (c & 0x00FF)) * p;
    hash = (hash ^ (c >> 8)) * p;
    hash += hash << 13;
    hash ^= hash >> 7;
    hash += hash << 3;
    hash ^= hash >> 17;
    hash += hash << 5;
    return hash;
  }

  /**
   * 32-bit FNV Hash Function
   * 
   * @param carray character array
   * @return hashcode
   */
  public long hash1(char carray[]) {
    final long p = 1099511628211L;
    long hash = 0xcbf29ce484222325L;
    for (int i = 0; i < carray.length; i++) {
      char d = carray[i];
      hash = (hash ^ (d & 0x00FF)) * p;
      hash = (hash ^ (d >> 8)) * p;
    }

    // hash += hash << 13;
    // hash ^= hash >> 7;
    // hash += hash << 3;
    // hash ^= hash >> 17;
    // hash += hash << 5;
    return hash;
  }

  /**
   * djb2 hash algorithm，this algorithm (k=33) was first reported by dan
   * bernstein many years ago in comp.lang.c. another version of this algorithm
   * (now favored by bernstein) uses xor: hash(i) = hash(i - 1) * 33 ^ str[i];
   * the magic of number 33 (why it works better than many other constants,
   * prime or not) has never been adequately explained.
   * 
   * @param c character
   * @return hashcode
   */
  public int hash2(char c) {
    int hash = 5381;

    /* hash 33 + c */
    hash = ((hash << 5) + hash) + c & 0x00FF;
    hash = ((hash << 5) + hash) + c >> 8;

    return hash;
  }

  /**
   * djb2 hash algorithm，this algorithm (k=33) was first reported by dan
   * bernstein many years ago in comp.lang.c. another version of this algorithm
   * (now favored by bernstein) uses xor: hash(i) = hash(i - 1) * 33 ^ str[i];
   * the magic of number 33 (why it works better than many other constants,
   * prime or not) has never been adequately explained.
   * 
   * @param carray character array
   * @return hashcode
   */
  public int hash2(char carray[]) {
    int hash = 5381;

    /* hash 33 + c */
    for (int i = 0; i < carray.length; i++) {
      char d = carray[i];
      hash = ((hash << 5) + hash) + d & 0x00FF;
      hash = ((hash << 5) + hash) + d >> 8;
    }

    return hash;
  }
}
