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
package org.apache.lucene.index;

import java.util.List;
import org.apache.lucene.util.Bits;

/**
 * Concatenates multiple Bits together, on every lookup.
 *
 * <p><b>NOTE</b>: This is very costly, as every lookup must do a binary search to locate the right
 * sub-reader.
 *
 * @lucene.experimental
 */
public final class MultiBits implements Bits {
  private final Bits[] subs;

  // length is 1+subs.length (the last entry has the maxDoc):
  private final int[] starts;

  private final boolean defaultValue;

  private MultiBits(Bits[] subs, int[] starts, boolean defaultValue) {
    assert starts.length == 1 + subs.length;
    this.subs = subs;
    this.starts = starts;
    this.defaultValue = defaultValue;
  }

  /**
   * Returns a single {@link Bits} instance for this reader, merging live Documents on the fly. This
   * method will return null if the reader has no deletions.
   *
   * <p><b>NOTE</b>: this is a very slow way to access live docs. For example, each Bits access will
   * require a binary search. It's better to get the sub-readers and iterate through them yourself.
   */
  public static Bits getLiveDocs(IndexReader reader) {
    if (reader.hasDeletions()) {
      final List<LeafReaderContext> leaves = reader.leaves();
      final int size = leaves.size();
      assert size > 0 : "A reader with deletions must have at least one leave";
      if (size == 1) {
        return leaves.get(0).reader().getLiveDocs();
      }
      final Bits[] liveDocs = new Bits[size];
      final int[] starts = new int[size + 1];
      for (int i = 0; i < size; i++) {
        // record all liveDocs, even if they are null
        final LeafReaderContext ctx = leaves.get(i);
        liveDocs[i] = ctx.reader().getLiveDocs();
        starts[i] = ctx.docBase;
      }
      starts[size] = reader.maxDoc();
      return new MultiBits(liveDocs, starts, true);
    } else {
      return null;
    }
  }

  private boolean checkLength(int reader, int doc) {
    final int length = starts[1 + reader] - starts[reader];
    assert doc - starts[reader] < length
        : "doc="
            + doc
            + " reader="
            + reader
            + " starts[reader]="
            + starts[reader]
            + " length="
            + length;
    return true;
  }

  @Override
  public boolean get(int doc) {
    final int reader = ReaderUtil.subIndex(doc, starts);
    assert reader != -1;
    final Bits bits = subs[reader];
    if (bits == null) {
      return defaultValue;
    } else {
      assert checkLength(reader, doc);
      return bits.get(doc - starts[reader]);
    }
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(subs.length).append(" subs: ");
    for (int i = 0; i < subs.length; i++) {
      if (i != 0) {
        b.append("; ");
      }
      if (subs[i] == null) {
        b.append("s=").append(starts[i]).append(" l=null");
      } else {
        b.append("s=")
            .append(starts[i])
            .append(" l=")
            .append(subs[i].length())
            .append(" b=")
            .append(subs[i]);
      }
    }
    b.append(" end=").append(starts[subs.length]);
    return b.toString();
  }

  @Override
  public int length() {
    return starts[starts.length - 1];
  }
}
