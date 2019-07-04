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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;

import org.junit.Before;

public class TestFstDirect extends LuceneTestCase {

  private List<String> words;

  @Before
  public void before() {
    words = new ArrayList<>();
  }

  public void testDenseWithGap() throws Exception {
    //words.addAll(Arrays.asList("apple", "berry", "cherry", "damson", "fig", "grape"));
    words.addAll(Arrays.asList("ah", "bi", "cj", "dk", "fl", "gm"));
    final BytesRefFSTEnum<Object> fstEnum = new BytesRefFSTEnum<>(buildFST(words));
    for (String word : words) {
      assertNotNull(word + " not found", fstEnum.seekExact(new BytesRef(word)));
    }
  }


  private FST<Object> buildFST(List<String> words) throws Exception {
    long start = System.nanoTime();
    final Outputs<Object> outputs = NoOutputs.getSingleton();
    final Builder<Object> b = new Builder<>(FST.INPUT_TYPE.BYTE1, 0, 0, true, true, Integer.MAX_VALUE, outputs, true, 15);

    for (String word : words) {
      b.add(Util.toIntsRef(new BytesRef(word), new IntsRefBuilder()), outputs.getNoOutput());
    }
    FST<Object> fst = b.finish();
    long t = System.nanoTime();
    printf("Built FST of %d bytes in %d ms", fst.ramBytesUsed(), nsToMs(t - start));
    return fst;
  }

  private static void printf(String format, Object ... values) {
    System.out.println(String.format(Locale.ROOT, format, values));
  }

  private static long nsToMs(long ns) {
    return ns / 1_000_000;
  }

  public static void main(String... args) throws Exception {
    byte[] buf = Files.readAllBytes(Paths.get(args[0]));
    DataInput in = new ByteArrayDataInput(buf);
    FST<BytesRef> fst = new FST<>(in, ByteSequenceOutputs.getSingleton());
    BytesRefFSTEnum<BytesRef> fstEnum = new BytesRefFSTEnum<>(fst);
    int sparseArrayArcCount = 0, directArrayArcCount = 0, listArcCount = 0;
    while(fstEnum.next() != null) {
      if (fstEnum.arcs[fstEnum.upto].bytesPerArc == 0) {
        listArcCount ++;
      } else if (fstEnum.arcs[fstEnum.upto].arcIdx == Integer.MIN_VALUE) {
        directArrayArcCount ++;
      } else {
        sparseArrayArcCount ++;
      }
    }
    System.out.println("direct arcs = " + directArrayArcCount + ", sparse arcs = " + sparseArrayArcCount +
                       " list arcs = " + listArcCount);
  }

}
