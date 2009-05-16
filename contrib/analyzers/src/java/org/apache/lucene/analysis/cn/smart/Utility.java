/**
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

package org.apache.lucene.analysis.cn.smart;

public class Utility {

  public static final char[] STRING_CHAR_ARRAY = new String("未##串")
      .toCharArray();

  public static final char[] NUMBER_CHAR_ARRAY = new String("未##数")
      .toCharArray();

  public static final char[] START_CHAR_ARRAY = new String("始##始")
      .toCharArray();

  public static final char[] END_CHAR_ARRAY = new String("末##末").toCharArray();

  public static final char[] COMMON_DELIMITER = new char[] { ',' };

  /**
   * 需要跳过的符号，例如制表符，回车，换行等等。
   */
  public static final String SPACES = " 　\t\r\n";

  public static final int MAX_FREQUENCE = 2079997 + 80000;

  /**
   * 比较两个整数数组的大小, 分别从数组的一定位置开始逐个比较, 当依次相等且都到达末尾时, 返回相等, 否则未到达末尾的大于到达末尾的;
   * 当未到达末尾时有一位不相等, 该位置数值大的数组大于小的
   * 
   * @param larray
   * @param lstartIndex larray的起始位置
   * @param rarray
   * @param rstartIndex rarray的起始位置
   * @return 0表示相等，1表示larray > rarray, -1表示larray < rarray
   */
  public static int compareArray(char[] larray, int lstartIndex, char[] rarray,
      int rstartIndex) {

    if (larray == null) {
      if (rarray == null || rstartIndex >= rarray.length)
        return 0;
      else
        return -1;
    } else {
      // larray != null
      if (rarray == null) {
        if (lstartIndex >= larray.length)
          return 0;
        else
          return 1;
      }
    }

    int li = lstartIndex, ri = rstartIndex;
    while (li < larray.length && ri < rarray.length && larray[li] == rarray[ri]) {
      li++;
      ri++;
    }
    if (li == larray.length) {
      if (ri == rarray.length) {
        // 两者一直相等到末尾，因此返回相等，也就是结果0
        return 0;
      } else {
        // 此时不可能ri>rarray.length因此只有ri<rarray.length
        // 表示larray已经结束，rarray没有结束，因此larray < rarray，返回-1
        return -1;
      }
    } else {
      // 此时不可能li>larray.length因此只有li < larray.length，表示li没有到达larray末尾
      if (ri == rarray.length) {
        // larray没有结束，但是rarray已经结束，因此larray > rarray
        return 1;
      } else {
        // 此时不可能ri>rarray.length因此只有ri < rarray.length
        // 表示larray和rarray都没有结束，因此按下一个数的大小判断
        if (larray[li] > rarray[ri])
          return 1;
        else
          return -1;
      }
    }
  }

  /**
   * 根据前缀来判断两个字符数组的大小，当前者为后者的前缀时，表示相等，当不为前缀时，按照普通字符串方式比较
   * 
   * @param shortArray
   * @param shortIndex
   * @param longArray
   * @param longIndex
   * @return
   */
  public static int compareArrayByPrefix(char[] shortArray, int shortIndex,
      char[] longArray, int longIndex) {

    // 空数组是所有数组的前缀，不考虑index
    if (shortArray == null)
      return 0;
    else if (longArray == null)
      return (shortIndex < shortArray.length) ? 1 : 0;

    int si = shortIndex, li = longIndex;
    while (si < shortArray.length && li < longArray.length
        && shortArray[si] == longArray[li]) {
      si++;
      li++;
    }
    if (si == shortArray.length) {
      // shortArray 是 longArray的prefix
      return 0;
    } else {
      // 此时不可能si>shortArray.length因此只有si <
      // shortArray.length，表示si没有到达shortArray末尾

      // shortArray没有结束，但是longArray已经结束，因此shortArray > longArray
      if (li == longArray.length)
        return 1;
      else
        // 此时不可能li>longArray.length因此只有li < longArray.length
        // 表示shortArray和longArray都没有结束，因此按下一个数的大小判断
        return (shortArray[si] > longArray[li]) ? 1 : -1;
    }
  }

  public static int getCharType(char ch) {
    // 最多的是汉字
    if (ch >= 0x4E00 && ch <= 0x9FA5)
      return CharType.HANZI;
    if ((ch >= 0x0041 && ch <= 0x005A) || (ch >= 0x0061 && ch <= 0x007A))
      return CharType.LETTER;
    if (ch >= 0x0030 && ch <= 0x0039)
      return CharType.DIGIT;
    if (ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' || ch == '　')
      return CharType.SPACE_LIKE;
    // 最前面的其它的都是标点符号了
    if ((ch >= 0x0021 && ch <= 0x00BB) || (ch >= 0x2010 && ch <= 0x2642)
        || (ch >= 0x3001 && ch <= 0x301E))
      return CharType.DELIMITER;

    // 全角字符区域
    if ((ch >= 0xFF21 && ch <= 0xFF3A) || (ch >= 0xFF41 && ch <= 0xFF5A))
      return CharType.FULLWIDTH_LETTER;
    if (ch >= 0xFF10 && ch <= 0xFF19)
      return CharType.FULLWIDTH_DIGIT;
    if (ch >= 0xFE30 && ch <= 0xFF63)
      return CharType.DELIMITER;
    return CharType.OTHER;

  }
}
