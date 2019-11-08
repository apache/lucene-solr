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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;

public class TestFstDirectAddressing extends LuceneTestCase {

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
    // Size is 1648 when we use only list-encoding. We were previously failing to ever de-dup
    // direct addressing, which led this case to blow up.
    assertTrue(size <= 1080);
    //printf("fst size = %d bytes", size);
  }

  public void testWorstCaseForDirectAddressing() throws Exception {
    final int NUM_WORDS = 1000000;
    final double RAM_BYTES_USED_NO_DIRECT_ADDRESSING = 3.84d * 1024d * 1024d;
    final double MEMORY_INCREASE_LIMIT_PERCENT = 12.5d;

    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    Builder<Long> builder = new Builder<>(FST.INPUT_TYPE.BYTE1, 0, 0, true, true, Integer.MAX_VALUE, outputs, true, 15)
        // Use this parameter to try & test a change.
        // Set -1 to disable direct addressing and get the reference FST size without it.
        //.setDirectAddressingMaxOversizingFactor(-1f);
        .setDirectAddressingMaxOversizingFactor(Builder.DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR);

    // Generate words with specially crafted bytes.
    Set<BytesRef> wordSet = new HashSet<>();
    for (int i = 0; i < NUM_WORDS; ++i) {
      byte[] b = new byte[5];
      random().nextBytes(b);
      for (int j = 0; j < b.length; ++j) {
        b[j] &= 0xfc; // Make this byte a multiple of 4.
      }
      wordSet.add(new BytesRef(b));
    }

    // Sort words.
    List<BytesRef> wordList = new ArrayList<>(wordSet);
    Collections.sort(wordList);

    // Add words.
    IntsRefBuilder intsRefBuilder = new IntsRefBuilder();
    for (BytesRef word : wordList) {
      builder.add(Util.toIntsRef(word, intsRefBuilder), outputs.getNoOutput());
    }

    // Build FST.
    long ramBytesUsed = builder.finish().ramBytesUsed();
    double directAddressingMemoryIncreasePercent = (ramBytesUsed / RAM_BYTES_USED_NO_DIRECT_ADDRESSING - 1) * 100;

    // Print stats.
//    System.out.println("directAddressingMaxOversizingFactor = " + builder.getDirectAddressingMaxOversizingFactor());
//    System.out.println("ramBytesUsed = "
//        + String.format(Locale.ENGLISH, "%.2f MB", ramBytesUsed / 1024d / 1024d)
//        + String.format(Locale.ENGLISH, " (%.2f %% increase with direct addressing)", directAddressingMemoryIncreasePercent));
//    System.out.println("num nodes = " + builder.nodeCount);
//    long fixedLengthArcNodeCount = builder.directAddressingNodeCount + builder.binarySearchNodeCount;
//    System.out.println("num fixed-length-arc nodes = " + fixedLengthArcNodeCount
//        + String.format(Locale.ENGLISH, " (%.2f %% of all nodes)",
//        ((double) fixedLengthArcNodeCount / builder.nodeCount * 100)));
//    System.out.println("num binary-search nodes = " + (builder.binarySearchNodeCount)
//        + String.format(Locale.ENGLISH, " (%.2f %% of fixed-length-arc nodes)",
//        ((double) (builder.binarySearchNodeCount) / fixedLengthArcNodeCount * 100)));
//    System.out.println("num direct-addressing nodes = " + (builder.directAddressingNodeCount)
//        + String.format(Locale.ENGLISH, " (%.2f %% of fixed-length-arc nodes)",
//        ((double) (builder.directAddressingNodeCount) / fixedLengthArcNodeCount * 100)));

    // Verify the FST size does not exceed a limit.
    // This limit is a reference. It could be modified if the FST encoding is expected to change.
    // This limit is determined by running this test with Builder.DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR above.
    assertTrue("FST size exceeds limit, size = " + ramBytesUsed
            + ", increase = " + directAddressingMemoryIncreasePercent + " %"
            + ", limit = " + MEMORY_INCREASE_LIMIT_PERCENT + " %",
        directAddressingMemoryIncreasePercent < MEMORY_INCREASE_LIMIT_PERCENT);
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

  public static void main(String... args) throws Exception {
    byte[] buf = Files.readAllBytes(Paths.get(args[0]));
    DataInput in = new ByteArrayDataInput(buf);
    FST<BytesRef> fst = new FST<>(in, ByteSequenceOutputs.getSingleton());
    BytesRefFSTEnum<BytesRef> fstEnum = new BytesRefFSTEnum<>(fst);
    int binarySearchArcCount = 0, directAddressingArcCount = 0, listArcCount = 0;
    while(fstEnum.next() != null) {
      if (fstEnum.arcs[fstEnum.upto].bytesPerArc() == 0) {
        listArcCount ++;
      } else if (fstEnum.arcs[fstEnum.upto].nodeFlags() == FST.ARCS_FOR_DIRECT_ADDRESSING) {
        directAddressingArcCount ++;
      } else {
        binarySearchArcCount ++;
      }
    }
    System.out.println("direct addressing arcs = " + directAddressingArcCount
        + ", binary search arcs = " + binarySearchArcCount
        + " list arcs = " + listArcCount);
  }

}
