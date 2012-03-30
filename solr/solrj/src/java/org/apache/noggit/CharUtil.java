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

package org.apache.noggit;


/**
 * @author yonik
 * @version $Id: CharUtil.java 479919 2006-11-28 05:53:55Z yonik $
 */
public class CharUtil {

  // belongs in number utils or charutil?
  public long parseLong(char[] arr, int start, int end) {
    long x = 0;
    boolean negative = arr[start] == '-';
    for (int i=negative ? start+1 : start; i<end; i++) {
      // If constructing the largest negative number, this will overflow
      // to the largest negative number.  This is OK since the negation of
      // the largest negative number is itself in two's complement.
      x = x * 10 + (arr[i] - '0');
    }
    // could replace conditional-move with multiplication of sign... not sure
    // which is faster.
    return negative ? -x : x;
  }


  public int compare(char[] a, int a_start, int a_end, char[] b, int b_start, int b_end) {
    int a_len = a_end - a_start;
    int b_len = b_end - b_start;
    int len = Math.min(a_len,b_len);
    while (--len>=0) {
      int c = a[a_start] - b[b_start];
      if (c!=0) return c;
      a_start++; b_start++;
    }
    return a_len-b_len;
  }

}
