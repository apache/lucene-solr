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

package org.apache.lucene.spatial;

import org.apache.lucene.analysis.NumericTokenStream; // for javadocs
import org.apache.lucene.search.NumericRangeQuery; // for javadocs
import org.apache.lucene.util.NumericUtils; // for javadocs

/**
 * TODO -- when solr moves NumberUtils to lucene, this should be redundant
 * 
 * This is a copy of solr's number utils with only the functions we use...
 * 
 * @deprecated TODO: This helper class will be removed soonly.
 * For new indexes use {@link NumericUtils} instead, which provides a sortable
 * binary representation (prefix encoded) of numeric values.
 * To index and efficiently query numeric values use {@link NumericTokenStream}
 * and {@link NumericRangeQuery}.
 */
@Deprecated
public class NumberUtils {


  public static String long2sortableStr(long val) {
    char[] arr = new char[5];
    long2sortableStr(val,arr,0);
    return new String(arr,0,5);
  }

  public static String double2sortableStr(double val) {
    long f = Double.doubleToRawLongBits(val);
    if (f<0) f ^= 0x7fffffffffffffffL;
    return long2sortableStr(f);
  }

  public static double SortableStr2double(String val) {
    long f = SortableStr2long(val,0,6);
    if (f<0) f ^= 0x7fffffffffffffffL;
    return Double.longBitsToDouble(f);
  }
  
  public static int long2sortableStr(long val, char[] out, int offset) {
    val += Long.MIN_VALUE;
    out[offset++] = (char)(val >>>60);
    out[offset++] = (char)(val >>>45 & 0x7fff);
    out[offset++] = (char)(val >>>30 & 0x7fff);
    out[offset++] = (char)(val >>>15 & 0x7fff);
    out[offset] = (char)(val & 0x7fff);
    return 5;
  }

  public static long SortableStr2long(String sval, int offset, int len) {
    long val = (long)(sval.charAt(offset++)) << 60;
    val |= ((long)sval.charAt(offset++)) << 45;
    val |= ((long)sval.charAt(offset++)) << 30;
    val |= sval.charAt(offset++) << 15;
    val |= sval.charAt(offset);
    val -= Long.MIN_VALUE;
    return val;
  }
}
