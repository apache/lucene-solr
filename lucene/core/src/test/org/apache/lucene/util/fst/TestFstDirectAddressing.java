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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
      entries.add(new BytesRef(word.getBytes(StandardCharsets.US_ASCII)));
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
    // This test will fail if there is more than 1% size increase with direct addressing.
    assertTrue("FST size = " + size + " B", size <= 1648 * 1.01d);
  }

  public void testWorstCaseForDirectAddressing() throws Exception {
    // This test will fail if there is more than 1% memory increase with direct addressing in this worst case.
    final double MEMORY_INCREASE_LIMIT_PERCENT = 1d;
    final int NUM_WORDS = 1000000;

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
    List<BytesRef> wordList = new ArrayList<>(wordSet);
    Collections.sort(wordList);

    // Disable direct addressing and measure the FST size.
    Builder<Object> builder = createBuilder(-1f);
    FST<Object> fst = buildFST(wordList, builder);
    long ramBytesUsedNoDirectAddressing = fst.ramBytesUsed();

    // Enable direct addressing and measure the FST size.
    builder = createBuilder(Builder.DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR);
    fst = buildFST(wordList, builder);
    long ramBytesUsed = fst.ramBytesUsed();

    // Compute the size increase in percents.
    double directAddressingMemoryIncreasePercent = ((double) ramBytesUsed / ramBytesUsedNoDirectAddressing - 1) * 100;

//    printStats(builder, ramBytesUsed, directAddressingMemoryIncreasePercent);

    // Verify the FST size does not exceed the limit.
    assertTrue("FST size exceeds limit, size = " + ramBytesUsed
            + ", increase = " + directAddressingMemoryIncreasePercent + " %"
            + ", limit = " + MEMORY_INCREASE_LIMIT_PERCENT + " %",
        directAddressingMemoryIncreasePercent < MEMORY_INCREASE_LIMIT_PERCENT);
  }

  private static void printStats(Builder<Object> builder, long ramBytesUsed, double directAddressingMemoryIncreasePercent) {
    System.out.println("directAddressingMaxOversizingFactor = " + builder.getDirectAddressingMaxOversizingFactor());
    System.out.println("ramBytesUsed = "
        + String.format(Locale.ENGLISH, "%.2f MB", ramBytesUsed / 1024d / 1024d)
        + String.format(Locale.ENGLISH, " (%.2f %% increase with direct addressing)", directAddressingMemoryIncreasePercent));
    System.out.println("num nodes = " + builder.nodeCount);
    long fixedLengthArcNodeCount = builder.directAddressingNodeCount + builder.binarySearchNodeCount;
    System.out.println("num fixed-length-arc nodes = " + fixedLengthArcNodeCount
        + String.format(Locale.ENGLISH, " (%.2f %% of all nodes)",
        ((double) fixedLengthArcNodeCount / builder.nodeCount * 100)));
    System.out.println("num binary-search nodes = " + (builder.binarySearchNodeCount)
        + String.format(Locale.ENGLISH, " (%.2f %% of fixed-length-arc nodes)",
        ((double) (builder.binarySearchNodeCount) / fixedLengthArcNodeCount * 100)));
    System.out.println("num direct-addressing nodes = " + (builder.directAddressingNodeCount)
        + String.format(Locale.ENGLISH, " (%.2f %% of fixed-length-arc nodes)",
        ((double) (builder.directAddressingNodeCount) / fixedLengthArcNodeCount * 100)));
  }

  private static Builder<Object> createBuilder(float directAddressingMaxOversizingFactor) {
    return new Builder<>(FST.INPUT_TYPE.BYTE1, 0, 0, true, true, Integer.MAX_VALUE, NoOutputs.getSingleton(), true, 15)
        .setDirectAddressingMaxOversizingFactor(directAddressingMaxOversizingFactor);
  }

  private FST<Object> buildFST(List<BytesRef> entries) throws Exception {
    return buildFST(entries, createBuilder(Builder.DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR));
  }

  private static FST<Object> buildFST(List<BytesRef> entries, Builder<Object> builder) throws Exception {
    BytesRef last = null;
    for (BytesRef entry : entries) {
      if (entry.equals(last) == false) {
        builder.add(Util.toIntsRef(entry, new IntsRefBuilder()), NoOutputs.getSingleton().getNoOutput());
      }
      last = entry;
    }
    return builder.finish();
  }

  public static void main(String... args) throws Exception {
    if (args.length < 2) {
      throw new IllegalArgumentException("Missing argument");
    }
    if (args[0].equals("-countFSTArcs")) {
      countFSTArcs(args[1]);
    } else if (args[0].equals("-measureFSTOversizing")) {
      measureFSTOversizing(args[1]);
    } else {
      throw new IllegalArgumentException("Invalid argument " + args[0]);
    }
  }

  private static void countFSTArcs(String FSTFilePath) throws IOException {
    byte[] buf = Files.readAllBytes(Paths.get(FSTFilePath));
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

  private static void measureFSTOversizing(String wordsFilePath) throws Exception {
    final int MAX_NUM_WORDS = 1000000;

    // Read real english words.
    List<BytesRef> wordList = new ArrayList<>();
    try (BufferedReader reader = Files.newBufferedReader(Paths.get(wordsFilePath))) {
      while (wordList.size() < MAX_NUM_WORDS) {
        String word = reader.readLine();
        if (word == null) {
          break;
        }
        wordList.add(new BytesRef(word));
      }
    }
    Collections.sort(wordList);

    // Disable direct addressing and measure the FST size.
    Builder<Object> builder = createBuilder(-1f);
    FST<Object> fst = buildFST(wordList, builder);
    long ramBytesUsedNoDirectAddressing = fst.ramBytesUsed();

    // Enable direct addressing and measure the FST size.
    builder = createBuilder(Builder.DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR);
    fst = buildFST(wordList, builder);
    long ramBytesUsed = fst.ramBytesUsed();

    // Compute the size increase in percents.
    double directAddressingMemoryIncreasePercent = ((double) ramBytesUsed / ramBytesUsedNoDirectAddressing - 1) * 100;

    printStats(builder, ramBytesUsed, directAddressingMemoryIncreasePercent);
  }
}
