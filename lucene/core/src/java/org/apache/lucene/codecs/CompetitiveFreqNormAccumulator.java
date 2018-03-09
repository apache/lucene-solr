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
package org.apache.lucene.codecs;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This class accumulates the (freq, norm) pairs that may produce competitive scores.
 */
public final class CompetitiveFreqNormAccumulator {

  // We speed up accumulation for common norm values by first computing
  // the max freq for all norms in -128..127
  private final int[] maxFreqs;
  private boolean dirty;
  private final TreeSet<FreqAndNorm> freqNormPairs;

  /** Sole constructor. */
  public CompetitiveFreqNormAccumulator() {
    maxFreqs = new int[256];
    Comparator<FreqAndNorm> comparator = new Comparator<CompetitiveFreqNormAccumulator.FreqAndNorm>() {
      @Override
      public int compare(FreqAndNorm o1, FreqAndNorm o2) {
        // greater freqs compare greater
        int cmp = Integer.compare(o1.freq, o2.freq);
        if (cmp == 0) {
          // greater norms compare lower
          cmp = Long.compareUnsigned(o2.norm, o1.norm);
        }
        return cmp;
      }
    };
    freqNormPairs = new TreeSet<>(comparator);
  }

  /** Reset to the same state it was in after creation. */
  public void clear() {
    Arrays.fill(maxFreqs, 0);
    dirty = false;
    freqNormPairs.clear();
  }

  /**
   * A (freq, norm) pair.
   */
  public static class FreqAndNorm {
    /** Doc-term frequency. */
    public final int freq;
    /** Normalization factor. */
    public final long norm;

    /** Sole constructor. */
    public FreqAndNorm(int freq, long norm) {
      this.freq = freq;
      this.norm = norm;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj instanceof FreqAndNorm == false) {
        return false;
      }
      FreqAndNorm that = (FreqAndNorm) obj;
      return freq == that.freq && norm == that.norm;
    }

    @Override
    public int hashCode() {
      int h = getClass().hashCode();
      h = 31 * h + freq;
      h = 31 * h + Long.hashCode(norm);
      return h;
    }

    @Override
    public String toString() {
      return "{" + freq + "," + norm + "}";
    }
  }

  /** Accumulate a (freq,norm) pair, updating this structure if there is no
   *  equivalent or more competitive entry already. */
  public void add(int freq, long norm) {
    if (norm >= Byte.MIN_VALUE && norm <= Byte.MAX_VALUE) {
      int index = Byte.toUnsignedInt((byte) norm);
      maxFreqs[index] = Math.max(maxFreqs[index], freq); 
      dirty = true;
    } else {
      add(new FreqAndNorm(freq, norm));
    }
  }

  /** Merge {@code acc} into this. */
  public void addAll(CompetitiveFreqNormAccumulator acc) {
    for (FreqAndNorm entry : acc.getCompetitiveFreqNormPairs()) {
      add(entry);
    }
  }

  /** Get the set of competitive freq and norm pairs, orderer by increasing freq and norm. */
  public SortedSet<FreqAndNorm> getCompetitiveFreqNormPairs() {
    if (dirty) {
      for (int i = 0; i < maxFreqs.length; ++i) {
        if (maxFreqs[i] > 0) {
          add(new FreqAndNorm(maxFreqs[i], (byte) i));
          maxFreqs[i] = 0;
        }
      }
      dirty = false;
    }
    return Collections.unmodifiableSortedSet(freqNormPairs);
  }

  private void add(FreqAndNorm newEntry) {
    FreqAndNorm next = freqNormPairs.ceiling(newEntry);
    if (next == null) {
      // nothing is more competitive
      freqNormPairs.add(newEntry);
    } else if (Long.compareUnsigned(next.norm, newEntry.norm) <= 0) {
      // we already have this entry or more competitive entries in the tree
      return;
    } else {
      // some entries have a greater freq but a less competitive norm, so we
      // don't know which one will trigger greater scores, still add to the tree
      freqNormPairs.add(newEntry);
    }

    for (Iterator<FreqAndNorm> it = freqNormPairs.headSet(newEntry, false).descendingIterator(); it.hasNext(); ) {
      FreqAndNorm entry = it.next();
      if (Long.compareUnsigned(entry.norm, newEntry.norm) >= 0) {
        // less competitive
        it.remove();
      } else {
        // lesser freq but better norm, further entries are not comparable
        break;
      }
    }
  }

  @Override
  public String toString() {
    return getCompetitiveFreqNormPairs().toString();
  }
}
