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
  final FST<Byte> fst;
  final byte[] metadata;
  
  static final int RECORD_SIZE = 15;
  
  public HangulDictionary(FST<Byte> fst, byte[] metadata) {
    this.fst = fst;
    this.metadata = metadata;
  }
  
  Byte lookup(String key) {
    // TODO: why is does this thing lookup empty strings?
    if (key.length() == 0) {
      return null;
    }
    final FST.Arc<Byte> arc = fst.getFirstArc(new FST.Arc<Byte>());

    final BytesReader fstReader = fst.getBytesReader();

    // Accumulate output as we go
    Byte output = fst.outputs.getNoOutput();
    for (int i = 0; i < key.length(); i++) {
      try {
        if (fst.findTargetArc(key.charAt(i), arc, arc, fstReader) == null) {
          return null;
        }
      } catch (IOException bogus) {
        throw new RuntimeException();
      }
      // we shouldnt need this accumulation?!
      output = fst.outputs.add(output, arc.output);
    }

    if (arc.isFinal()) {
      return fst.outputs.add(output, arc.nextFinalOutput);
    } else {
      return null;
    }
  }
  
  char getFlags(byte b) {
    int off = b * RECORD_SIZE;
    return (char)((metadata[off] << 8) | (metadata[off+1] & 0xff));
  }
  
  WordEntry decodeEntry(String key, byte b) {
    return decodeEntry(key, b, getFlags(b));
  }
  
  WordEntry decodeEntry(String key, byte b, char flags) {
    if ((flags & WordEntry.COMPOUND_IRREGULAR) != 0) {
      return new WordEntry(key, flags, getIrregularCompounds(key, b));
    } else if ((flags & WordEntry.COMPOUND) != 0) {
      return new WordEntry(key, flags, getCompounds(key, b));
    } else {
      return new WordEntry(key, flags, null);
    }
  }
  
  List<CompoundEntry> getCompounds(String word, byte b) {
    int off = b * RECORD_SIZE;
    int numSplits = metadata[off+2];
    assert numSplits > 0;
    List<CompoundEntry> compounds = new ArrayList<>(numSplits+1);
    int last = 0;
    for (int i = 0; i < numSplits; i++) {
      int split = metadata[off+3+i];
      compounds.add(new CompoundEntry(word.substring(last, split), true));
      last = split;
    }
    compounds.add(new CompoundEntry(word.substring(last), true));
    return compounds;
  }
  
  List<CompoundEntry> getIrregularCompounds(String word, byte b) {
    int off = b * RECORD_SIZE;
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
    return compounds;
  }
}
