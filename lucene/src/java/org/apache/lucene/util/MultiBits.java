package org.apache.lucene.util;

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

import java.util.List;

/**
 * Concatenates multiple Bits together, on every lookup.
 *
 * <p><b>NOTE</b>: This is very costly, as every lookup must
 * do a binary search to locate the right sub-reader.
 *
 * @lucene.experimental
 */

public final class MultiBits implements Bits {
  private final Bits[] subs;

  // length is 1+subs.length (the last entry has the maxDoc):
  private final int[] starts;

  public MultiBits(List<Bits> bits, List<Integer> starts) {
    assert starts.size() == 1+bits.size();
    this.subs = bits.toArray(Bits.EMPTY_ARRAY);
    this.starts = new int[starts.size()];
    for(int i=0;i<this.starts.length;i++) {
      this.starts[i] = starts.get(i);
    }
  }

  private boolean checkLength(int reader, int doc) {
    final int length = starts[1+reader]-starts[reader];
    assert doc - starts[reader] < length: "doc=" + doc + " reader=" + reader + " starts[reader]=" + starts[reader] + " length=" + length;
    return true;
  }

  public boolean get(int doc) {
    final int reader = ReaderUtil.subIndex(doc, starts);
    assert reader != -1;
    final Bits bits = subs[reader];
    if (bits == null) {
      return false;
    } else {
      assert checkLength(reader, doc);
      return bits.get(doc-starts[reader]);
    }
  }

  public final static class SubResult {
    public boolean matches;
    public Bits result;
  }

  private final SubResult subResult = new SubResult();

  public SubResult getMatchingSub(ReaderUtil.Slice slice) {
    int reader = ReaderUtil.subIndex(slice.start, starts);
    assert reader != -1;
    assert reader < subs.length: "slice=" + slice + " starts[-1]=" + starts[starts.length-1];
    if (starts[reader] == slice.start && starts[1+reader] == slice.start+slice.length) {
      subResult.matches = true;
      subResult.result = subs[reader];
    } else {
      subResult.matches = false;
    }
    return subResult;
  }

  public int length() {
    return starts[starts.length-1];
  }
}
