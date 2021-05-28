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
package org.apache.lucene.analysis.hunspell;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.CharSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.Util;

/** ICONV or OCONV replacement table */
class ConvTable {
  private final FST<CharsRef> fst;
  private final FixedBitSet firstCharHashes;
  private final int mod;

  ConvTable(TreeMap<String, String> mappings) {
    mod = Math.max(256, Integer.highestOneBit(mappings.size()) << 1);
    firstCharHashes = new FixedBitSet(mod);

    try {
      Outputs<CharsRef> outputs = CharSequenceOutputs.getSingleton();
      Builder<CharsRef> fstCompiler = new Builder<>(FST.INPUT_TYPE.BYTE2, outputs);
      IntsRefBuilder scratchInts = new IntsRefBuilder();
      for (Map.Entry<String, String> entry : mappings.entrySet()) {
        String key = entry.getKey();
        assert key.length() > 0;
        firstCharHashes.set(key.charAt(0) % mod);
        Util.toUTF16(key, scratchInts);
        fstCompiler.add(scratchInts.get(), new CharsRef(entry.getValue()));
      }

      fst = fstCompiler.finish();
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }
  }

  void applyMappings(StringBuilder sb) {
    FST.BytesReader bytesReader = null;
    FST.Arc<CharsRef> firstArc = null;
    FST.Arc<CharsRef> arc = null;

    int longestMatch;
    CharsRef longestOutput;

    for (int i = 0; i < sb.length(); i++) {
      if (!mightReplaceChar(sb.charAt(i))) {
        continue;
      }

      if (firstArc == null) {
        firstArc = fst.getFirstArc(new FST.Arc<>());
        bytesReader = fst.getBytesReader();
        arc = new FST.Arc<>();
      }
      arc.copyFrom(firstArc);
      CharsRef output = fst.outputs.getNoOutput();
      longestMatch = -1;
      longestOutput = null;

      for (int j = i; j < sb.length(); j++) {
        char ch = sb.charAt(j);

        try {
          if (fst.findTargetArc(ch, arc, arc, bytesReader) == null) {
            break;
          }
          output = fst.outputs.add(output, arc.output());
        } catch (IOException bogus) {
          throw new RuntimeException(bogus);
        }
        if (arc.isFinal()) {
          longestOutput = fst.outputs.add(output, arc.nextFinalOutput());
          longestMatch = j;
        }
      }

      if (longestMatch >= 0) {
        sb.delete(i, longestMatch + 1);
        sb.insert(i, longestOutput);
        i += (longestOutput.length - 1);
      }
    }
  }

  boolean mightReplaceChar(char c) {
    return firstCharHashes.get(c % mod);
  }
}
