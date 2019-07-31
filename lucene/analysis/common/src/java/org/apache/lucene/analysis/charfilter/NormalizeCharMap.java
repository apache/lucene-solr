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
package org.apache.lucene.analysis.charfilter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.CharSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.Util;

// TODO: save/load?

/**
 * Holds a map of String input to String output, to be used
 * with {@link MappingCharFilter}.  Use the {@link Builder}
 * to create this.
 */
public class NormalizeCharMap {

  final FST<CharsRef> map;
  final Map<Character,FST.Arc<CharsRef>> cachedRootArcs = new HashMap<>();

  // Use the builder to create:
  private NormalizeCharMap(FST<CharsRef> map) {
    this.map = map;
    if (map != null) {
      try {
        // Pre-cache root arcs:
        final FST.Arc<CharsRef> scratchArc = new FST.Arc<>();
        final FST.BytesReader fstReader = map.getBytesReader();
        map.getFirstArc(scratchArc);
        if (FST.targetHasArcs(scratchArc)) {
          map.readFirstRealTargetArc(scratchArc.target(), scratchArc, fstReader);
          while(true) {
            assert scratchArc.label() != FST.END_LABEL;
            cachedRootArcs.put(Character.valueOf((char) scratchArc.label()), new FST.Arc<CharsRef>().copyFrom(scratchArc));
            if (scratchArc.isLast()) {
              break;
            }
            map.readNextRealArc(scratchArc, fstReader);
          }
        }
        //System.out.println("cached " + cachedRootArcs.size() + " root arcs");
      } catch (IOException ioe) {
        // Bogus FST IOExceptions!!  (will never happen)
        throw new RuntimeException(ioe);
      }
    }
  }

  /**
   * Builds an NormalizeCharMap.
   * <p>
   * Call add() until you have added all the mappings, then call build() to get a NormalizeCharMap
   * @lucene.experimental
   */
  public static class Builder {

    private final Map<String,String> pendingPairs = new TreeMap<>();

    /** Records a replacement to be applied to the input
     *  stream.  Whenever <code>singleMatch</code> occurs in
     *  the input, it will be replaced with
     *  <code>replacement</code>.
     *
     * @param match input String to be replaced
     * @param replacement output String
     * @throws IllegalArgumentException if
     * <code>match</code> is the empty string, or was
     * already previously added
     */
    public void add(String match, String replacement) {
      if (match.length() == 0 ){
        throw new IllegalArgumentException("cannot match the empty string");
      }
      if (pendingPairs.containsKey(match)) {
        throw new IllegalArgumentException("match \"" + match + "\" was already added");
      }
      pendingPairs.put(match, replacement);
    }

    /** Builds the NormalizeCharMap; call this once you
     *  are done calling {@link #add}. */
    public NormalizeCharMap build() {

      final FST<CharsRef> map;
      try {
        final Outputs<CharsRef> outputs = CharSequenceOutputs.getSingleton();
        final org.apache.lucene.util.fst.Builder<CharsRef> builder = new org.apache.lucene.util.fst.Builder<>(FST.INPUT_TYPE.BYTE2, outputs);
        final IntsRefBuilder scratch = new IntsRefBuilder();
        for(Map.Entry<String,String> ent : pendingPairs.entrySet()) {
          builder.add(Util.toUTF16(ent.getKey(), scratch),
                      new CharsRef(ent.getValue()));
        }
        map = builder.finish();
        pendingPairs.clear();
      } catch (IOException ioe) {
        // Bogus FST IOExceptions!!  (will never happen)
        throw new RuntimeException(ioe);
      }

      return new NormalizeCharMap(map);
    }
  }
}
