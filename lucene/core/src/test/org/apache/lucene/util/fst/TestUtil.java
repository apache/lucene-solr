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
package org.apache.lucene.util.fst;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;

public class TestUtil extends LuceneTestCase {

  public void testBinarySearch() throws Exception {
    // Create a node with 8 arcs spanning (z-A) and ensure it is encoded as a packed array
    // requiring binary search.
    List<String> letters = Arrays.asList("A", "E", "J", "K", "L", "O", "T", "z");
    FST<Object> fst = buildFST(letters, true, false);
    FST.Arc<Object> arc = fst.getFirstArc(new FST.Arc<>());
    arc = fst.readFirstTargetArc(arc, arc, fst.getBytesReader());
    for (int i = 0; i < letters.size(); i++) {
      assertEquals(i, Util.binarySearch(fst, arc, letters.get(i).charAt(0)));
    }
    // before the first
    assertEquals(-1, Util.binarySearch(fst, arc, ' '));
    // after the last
    assertEquals(-1 - letters.size(), Util.binarySearch(fst, arc, '~'));
    assertEquals(-2, Util.binarySearch(fst, arc, 'B'));
    assertEquals(-2, Util.binarySearch(fst, arc, 'C'));
    assertEquals(-7, Util.binarySearch(fst, arc, 'P'));
  }

  public void testReadCeilArcPackedArray() throws Exception {
    List<String> letters = Arrays.asList("A", "E", "J", "K", "L", "O", "T", "z");
    verifyReadCeilArc(letters, true, false);
  }

  public void testReadCeilArcArrayWithGaps() throws Exception {
    List<String> letters = Arrays.asList("A", "E", "J", "K", "L", "O", "T");
    verifyReadCeilArc(letters, true, true);
  }

  public void testReadCeilArcList() throws Exception {
    List<String> letters = Arrays.asList("A", "E", "J", "K", "L", "O", "T", "z");
    verifyReadCeilArc(letters, false, false);
  }

  private void verifyReadCeilArc(List<String> letters, boolean allowArrayArcs, boolean allowDirectAddressing) throws Exception {
    FST<Object> fst = buildFST(letters, allowArrayArcs, allowDirectAddressing);
    FST.Arc<Object> first = fst.getFirstArc(new FST.Arc<>());
    FST.Arc<Object> arc = new FST.Arc<>();
    FST.BytesReader in = fst.getBytesReader();
    for (String letter : letters) {
      char c = letter.charAt(0);
      arc = Util.readCeilArc(c, fst, first, arc, in);
      assertNotNull(arc);
      assertEquals(c, arc.label());
    }
    // before the first
    assertEquals('A', Util.readCeilArc(' ', fst, first, arc, in).label());
    // after the last
    assertNull(Util.readCeilArc('~', fst, first, arc, in));
    // in the middle
    assertEquals('J', Util.readCeilArc('F', fst, first, arc, in).label());
    // no following arcs
    assertNull(Util.readCeilArc('Z', fst, arc, arc, in));
  }

  private FST<Object> buildFST(List<String> words, boolean allowArrayArcs, boolean allowDirectAddressing) throws Exception {
    final Outputs<Object> outputs = NoOutputs.getSingleton();
    final Builder<Object> b = new Builder<>(FST.INPUT_TYPE.BYTE1, 0, 0, true, true, Integer.MAX_VALUE, outputs, allowArrayArcs, 15);
    if (!allowDirectAddressing) {
      b.setDirectAddressingMaxOversizingFactor(-1f);
    }

    for (String word : words) {
      b.add(Util.toIntsRef(new BytesRef(word), new IntsRefBuilder()), outputs.getNoOutput());
    }
    return b.finish();
  }

  private List<String> createRandomDictionary(int width, int depth) {
    return createRandomDictionary(new ArrayList<>(), new StringBuilder(), width, depth);
  }

  private List<String> createRandomDictionary(List<String> dict, StringBuilder buf, int width, int depth) {
    char c = (char) random().nextInt(128);
    assert width < Character.MIN_SURROGATE / 8 - 128; // avoid surrogate chars
    int len = buf.length();
    for (int i = 0; i < width; i++) {
      buf.append(c);
      if (depth > 0) {
        createRandomDictionary(dict, buf, width, depth - 1);
      } else {
        dict.add(buf.toString());
      }
      c += random().nextInt(8);
      buf.setLength(len);
    }
    return dict;
  }

}
