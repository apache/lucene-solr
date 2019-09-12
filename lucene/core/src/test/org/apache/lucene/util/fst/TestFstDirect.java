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


public class TestFstDirect extends LuceneTestCase {

  public void testDenseWithGap() throws Exception {
    List<String> words = Arrays.asList("ah", "bi", "cj", "dk", "fl", "gm");
    List<BytesRef> entries = new ArrayList<>();
    for (String word : words) {
      entries.add(new BytesRef(word.getBytes("ascii")));
    }
    final BytesRefFSTEnum<Object> fstEnum = new BytesRefFSTEnum<>(buildFST(entries));
    for (BytesRef entry : entries) {
      assertNotNull(entry.utf8ToString() + " not found", fstEnum.seekExact(entry));
    }
  }

  public void testDeDupTails() throws Exception {
    List<BytesRef> entries = new ArrayList<>();
    for (int i = 0; i < 1000000; i += 4) {
      byte[] b = new byte[3];
      int val = i;
      for (int j = b.length - 1; j >= 0; --j) {
        b[j] = (byte) (val & 0xff);
        val >>= 8;
      }
      entries.add(new BytesRef(b));
    }
    long size = buildFST(entries).ramBytesUsed();
    // Size is 1664 when we use only list-encoding.  We were previously failing to ever de-dup
    // arrays-with-gaps, which led this case to blow up.
    assertTrue(size < 3000);
    //printf("fst size = %d bytes", size);
  }

  private FST<Object> buildFST(List<BytesRef> entries) throws Exception {
    final Outputs<Object> outputs = NoOutputs.getSingleton();
    final Builder<Object> b = new Builder<>(FST.INPUT_TYPE.BYTE1, 0, 0, true, true, Integer.MAX_VALUE, outputs, true, 15);
    BytesRef last = null;
    for (BytesRef entry : entries) {
      if (entry.equals(last) == false) {
        b.add(Util.toIntsRef(entry, new IntsRefBuilder()), outputs.getNoOutput());
      }
      last = entry;
    }
    FST<Object> fst = b.finish();
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
      if (fstEnum.arcs[fstEnum.upto].bytesPerArc() == 0) {
        listArcCount ++;
      } else if (fstEnum.arcs[fstEnum.upto].arcIdx() == Integer.MIN_VALUE) {
        directArrayArcCount ++;
      } else {
        sparseArrayArcCount ++;
      }
    }
    System.out.println("direct arcs = " + directArrayArcCount + ", sparse arcs = " + sparseArrayArcCount +
                       " list arcs = " + listArcCount);
  }

}
