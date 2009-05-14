/**
 * Copyright 2009 www.imdict.net
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

package org.apache.lucene.analysis.cn.smart.hhmm;

import java.io.UnsupportedEncodingException;

public abstract class AbstractDictionary {
  /**
   * 第一个汉字为“啊”，他前面有15个区，共15*94个字符
   */
  public static final int GB2312_FIRST_CHAR = 1410;

  /**
   * GB2312字符集中01~87的字符集才可能有效，共8178个
   */
  public static final int GB2312_CHAR_NUM = 87 * 94;

  /**
   * 词库文件中收录了6768个汉字的词频统计
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
  // GB2312 字符集的区位分布表：
  // 区号 字数 字符类别
  // 01 94 一般符号
  // 02 72 顺序号码
  // 03 94 拉丁字母
  // 04 83 日文假名
  // 05 86 Katakana
  // 06 48 希腊字母
  // 07 66 俄文字母
  // 08 63 汉语拼音符号
  // 09 76 图形符号
  // 10-15 备用区
  // 16-55 3755 一级汉字，以拼音为序
  // 56-87 3008 二级汉字，以笔划为序
  // 88-94 备用区
  // ======================================================

  /**
   * GB2312 共收录有 7445 个字符，其中简化汉字 6763 个，字母和符号 682 个。
   * 
   * GB2312 将所收录的字符分为 94 个区，编号为 01 区至 94 区；每个区收录 94 个字符，编号为 01 位至 94
   * 位，01为起始与0xA1，94位处于0xFE。GB2312 的每一个字符都由与其唯一对应的区号和位号所确定。例如：汉字“啊”，编号为 16 区 01
   * 位。
   */
  /**
   * @param ccid
   * @return
   */
  public String getCCByGB2312Id(int ccid) {
    if (ccid < 0 || ccid > WordDictionary.GB2312_CHAR_NUM)
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
   * 根据输入的Unicode字符，获取它的GB2312编码或者ascii编码，
   * 
   * @param ch 输入的GB2312中文字符或者ASCII字符(128个)
   * @return ch在GB2312中的位置，-1表示该字符不认识
   */
  public short getGB2312Id(char ch) {
    try {
      byte[] buffer = Character.toString(ch).getBytes("GB2312");
      if (buffer.length != 2) {
        // 正常情况下buffer应该是两个字节，否则说明ch不属于GB2312编码，故返回'?'，此时说明不认识该字符
        return -1;
      }
      int b0 = (int) (buffer[0] & 0x0FF) - 161; // 编码从A1开始，因此减去0xA1=161
      int b1 = (int) (buffer[1] & 0x0FF) - 161; // 第一个字符和最后一个字符没有汉字，因此每个区只收16*6-2=94个汉字
      return (short) (b0 * 94 + b1);
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return -1;
  }

  /**
   * 改进的32位FNV hash算法，用作本程序中的第一hash函数.第一和第二hash函数用来联合计算hash表， 使其均匀分布，
   * 并能避免因hash表过密而导致的长时间计算的问题
   * 
   * @param c 待hash的Unicode字符
   * @return c的哈希值
   * @see Utility.hash2()
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
   * @see Utility.hash1(char[])
   * @param carray
   * @return
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
   * djb2哈希算法，用作本程序中的第二hash函数
   * 
   * djb2 hash algorithm，this algorithm (k=33) was first reported by dan
   * bernstein many years ago in comp.lang.c. another version of this algorithm
   * (now favored by bernstein) uses xor: hash(i) = hash(i - 1) * 33 ^ str[i];
   * the magic of number 33 (why it works better than many other constants,
   * prime or not) has never been adequately explained.
   * 
   * @param c
   * @return
   */
  public int hash2(char c) {
    int hash = 5381;

    /* hash 33 + c */
    hash = ((hash << 5) + hash) + c & 0x00FF;
    hash = ((hash << 5) + hash) + c >> 8;

    return hash;
  }

  /**
   * @see Utility.hash2(char[])
   * @param carray
   * @return
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
