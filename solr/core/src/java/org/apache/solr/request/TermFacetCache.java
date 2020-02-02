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
package org.apache.solr.request;

import java.io.IOException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.LongValues;
import org.apache.solr.search.QueryResultKey;

/**
 *
 */
public class TermFacetCache {

  public static final String NAME = "termFacetCache";
  public static final int DEFAULT_THRESHOLD = 5000;


  public static final class FacetCacheKey {

    private final QueryResultKey qrk;
    private final String fieldName;

    public FacetCacheKey(QueryResultKey qrk, String fieldName) {
      this.qrk = qrk;
      this.fieldName = fieldName;
    }

    @Override
    public int hashCode() {
      return qrk.hashCode() ^ fieldName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      FacetCacheKey other = (FacetCacheKey)obj;
      return fieldName.equals(other.fieldName) && qrk.equals(other.qrk);
    }

  }

  public static final class SegmentCacheEntry {

    public final byte[] counts;
    public final int[] topLevelCounts;

    public SegmentCacheEntry(byte[] counts) {
      this.counts = counts;
      this.topLevelCounts = null;
    }

    public SegmentCacheEntry(int[] topLevelCounts) {
      this.counts = null;
      this.topLevelCounts = topLevelCounts;
    }

  }

  public static interface CacheUpdater {
    boolean incrementFromCachedSegment(LongValues toGlobal);
    void updateLeaf(int[] leafCounts);
    void updateTopLevel();
  }

  public static final byte[] encodeCounts(int[] segCounts, ByteBuffersDataOutput cachedSegCountsBuilder) {
    try {
      for (int c : segCounts) {
        cachedSegCountsBuilder.writeVInt(c);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ByteBuffersDataOutput.class + " should not throw IOException in practice", ex);
    }
    return cachedSegCountsBuilder.toArrayCopy();
  }

  public static void mergeCachedSegmentCounts(int[] counts, byte[] cachedSegCounts, LongValues ordMap) {
    ByteArrayDataInput segCounts = new ByteArrayDataInput(cachedSegCounts);
    if (!segCounts.eof()) {
      int ord = 0;
      int count = segCounts.readVInt();
      while (!segCounts.eof()) {
        if (count != 0) {
          counts[(int)ordMap.get(ord)] += count;
        }
        ord++;
        count = segCounts.readVInt();
      }
      // missing count
      counts[counts.length - 1] += count;
    }
  }

}
