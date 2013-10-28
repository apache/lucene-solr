package org.apache.lucene.analysis.ko.dic;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.BytesReader;

class HangulDictionary {
  private final FST<Byte> fst;
  private final byte[] metadata;
  private final FST.Arc<Byte> rootCache[]; // ~140kb
  
  static final int RECORD_SIZE = 15;
  
  public HangulDictionary(FST<Byte> fst, byte[] metadata) {
    this.fst = fst;
    this.metadata = metadata;
    try {
      rootCache = cacheRootArcs();
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }
  }
  
  @SuppressWarnings({"rawtypes","unchecked"})
  private FST.Arc<Byte>[] cacheRootArcs() throws IOException {
    FST.Arc<Byte> rootCache[] = new FST.Arc[1+(0xD7AF-0xAC00)];
    FST.Arc<Byte> firstArc = new FST.Arc<Byte>();
    fst.getFirstArc(firstArc);
    FST.Arc<Byte> arc = new FST.Arc<Byte>();
    final FST.BytesReader fstReader = fst.getBytesReader();
    // TODO: jump to AC00, readNextRealArc to ceiling? (just be careful we don't add bugs)
    for (int i = 0; i < rootCache.length; i++) {
      if (fst.findTargetArc(0xAC00 + i, firstArc, arc, fstReader) != null) {
        rootCache[i] = new FST.Arc<Byte>().copyFrom(arc);
      }
    }
    return rootCache;
  }
  
  private FST.Arc<Byte> findTargetArc(int ch, FST.Arc<Byte> follow, FST.Arc<Byte> arc, boolean useCache, FST.BytesReader fstReader) throws IOException {
    if (useCache && ch >= 0xAC00 && ch <= 0xD7AF) {
      assert ch != FST.END_LABEL;
      final FST.Arc<Byte> result = rootCache[ch - 0xAC00];
      if (result == null) {
        return null;
      } else {
        arc.copyFrom(result);
        return arc;
      }
    } else {
      return fst.findTargetArc(ch, follow, arc, fstReader);
    }
  }
  
  /** looks up word class for a word (exact match) */
  Byte lookup(CharSequence key) {
    // TODO: why is does this thing lookup empty strings?
    if (key.length() == 0) {
      return null;
    }
    final FST.Arc<Byte> arc = fst.getFirstArc(new FST.Arc<Byte>());

    final BytesReader fstReader = fst.getBytesReader();

    // Accumulate output as we go
    byte output = 0;
    for (int i = 0; i < key.length(); i++) {
      try {
        if (findTargetArc(key.charAt(i), arc, arc, i == 0, fstReader) == null) {
          return null;
        }
      } catch (IOException bogus) {
        throw new RuntimeException();
      }
      output += arc.output;
    }

    if (arc.isFinal()) {
      return (byte) (output + arc.nextFinalOutput);
    } else {
      return null;
    }
  }
  
  /** looks up features for word class */
  char getFlags(byte clazz) {
    int off = clazz * RECORD_SIZE;
    return (char)((metadata[off] << 8) | (metadata[off+1] & 0xff));
  }
  
  /** return list of compounds for key and word class.
   * this retrieves the splits for the class and applies them to the key */
  CompoundEntry[] getCompounds(String word, byte clazz) {
    int off = clazz * RECORD_SIZE;
    int numSplits = metadata[off+2];
    assert numSplits > 0;
    CompoundEntry compounds[] = new CompoundEntry[numSplits+1];
    int last = 0;
    for (int i = 0; i < numSplits; i++) {
      int split = metadata[off+3+i];
      compounds[i] = new CompoundEntry(word.substring(last, split), true);
      last = split;
    }
    compounds[numSplits] = new CompoundEntry(word.substring(last), true);
    return compounds;
  }
  
  /** return list of compounds for key and word class.
   * this retrieves the decompounded data for this irregular class */
  CompoundEntry[] getIrregularCompounds(byte clazz) {
    int off = clazz * RECORD_SIZE;
    int numChars = metadata[off+2];
    // TODO: more efficient
    List<CompoundEntry> compounds = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numChars; i++) {
      int idx = off+3+(i<<1);
      char next = (char)((metadata[idx] << 8) | (metadata[idx+1] & 0xff));
      if (next == ',') {
        compounds.add(new CompoundEntry(sb.toString(), true));
        sb.setLength(0);
      } else {
        sb.append(next);
      }
    }
    compounds.add(new CompoundEntry(sb.toString(), true));
    return compounds.toArray(new CompoundEntry[compounds.size()]);
  }
  
  /** walks the fst for prefix and returns true if it his no dead end */
  boolean hasPrefix(CharSequence key) {
    final FST.Arc<Byte> arc = fst.getFirstArc(new FST.Arc<Byte>());

    final BytesReader fstReader = fst.getBytesReader();

    for (int i = 0; i < key.length(); i++) {
      try {
        if (findTargetArc(key.charAt(i), arc, arc, i == 0, fstReader) == null) {
          return false;
        }
      } catch (IOException bogus) {
        throw new RuntimeException();
      }
    }
    return true;
  }
  
  /** looks up word class for a word (exact match) */
  int longestMatch(CharSequence key, int flags) {
    final FST.Arc<Byte> arc = fst.getFirstArc(new FST.Arc<Byte>());

    final BytesReader fstReader = fst.getBytesReader();

    // Accumulate output as we go
    byte output = 0;
    int max = 0;
    for (int i = 0; i < key.length(); i++) {
      try {
        if (findTargetArc(key.charAt(i), arc, arc, i == 0, fstReader) == null) {
          return max;
        }
      } catch (IOException bogus) {
        throw new RuntimeException();
      }
      output += arc.output;
      if (arc.isFinal()) {
        byte clazz = (byte) (output + arc.nextFinalOutput);
        if ((getFlags(clazz) & flags) != 0) {
          max = Math.max(max, i+1);
        }
      }
    }
    return max;
  }
}
