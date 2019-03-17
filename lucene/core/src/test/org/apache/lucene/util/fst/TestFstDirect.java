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
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;

import org.junit.Before;
import org.junit.Ignore;

public class TestFstDirect extends LuceneTestCase {

  private static final int COUNT = 10_000_000;
  private List<String> words;
  private Set<String> dict;
  private Random random;

  @Before
  public void before() {
    words = new ArrayList<>();
    random = new Random(random().nextLong());
  }

  public void testDenseWithGap() throws Exception {
    //words.addAll(Arrays.asList("apple", "berry", "cherry", "damson", "fig", "grape"));
    words.addAll(Arrays.asList("ah", "bi", "cj", "dk", "fl", "gm"));
    final BytesRefFSTEnum<Object> fstEnum = new BytesRefFSTEnum<>(buildFST(words, true));
    for (String word : words) {
      assertNotNull(word + " not found", fstEnum.seekExact(new BytesRef(word)));
    }
  }

  @Ignore("for performance testing")
  public void testLookupIDs() throws Exception {
    for (int i = 0; i < 10000000; i++) {
      words.add(String.format(Locale.ROOT, "%09d", i));
    }
    FST<Object> baselineFST = buildFST(words, false);
    FST<Object> optoFST = buildFST(words,true);
    for (int i = 0; i < 10; i++) {
      long seed = random().nextLong();
      random.setSeed(seed);
      long timeOpto = timeLookups(optoFST);
      random.setSeed(seed);
      long timeBase = timeLookups(baselineFST);
      printf("Sought %d present terms in %d ms (baseline) vs %d ms (opto), a %d%% difference", COUNT, nsToMs(timeBase), nsToMs(timeOpto),
             -100 * (timeBase - timeOpto) / timeBase);
    }
  }

  @Ignore("for performance testing")
  public void testRandomTerms() throws Exception {
    for (int i = 0; i < 100000; i++) {
      words.add(randomString());
    }
    Collections.sort(words);
    FST<Object> baselineFST = buildFST(words, false);
    FST<Object> optoFST = buildFST(words,true);
    for (int i = 0; i < 10; i++) {
      long seed = random().nextLong();
      random.setSeed(seed);
      long timeOpto = timeLookups(optoFST);
      random.setSeed(seed);
      long timeBase = timeLookups(baselineFST);
      printf("Sought %d present terms in %d ms (baseline) vs %d ms (opto), a %d%% difference", COUNT, nsToMs(timeBase), nsToMs(timeOpto),
             -100 * (timeBase - timeOpto) / timeBase);
    }
  }

  @Ignore("requires english dictionary")
  public void testLookupEnglishTerms() throws Exception {
    FST<Object> baselineFST = buildEnglishFST(false);
    FST<Object> optoFST = buildFST(words,true);
    for (int i = 0; i < 10; i++) {
      long seed = random().nextLong();
      random.setSeed(seed);
      long timeOpto = timeLookups(optoFST);
      random.setSeed(seed);
      long timeBase = timeLookups(baselineFST);
      printf("Sought %d present terms in %d ms (baseline) vs %d ms (opto), a %d%% difference", COUNT, nsToMs(timeBase), nsToMs(timeOpto),
             -100 * (timeBase - timeOpto) / timeBase);
    }
  }

  private long timeLookups(FST<Object> fst) throws Exception {
    final BytesRefFSTEnum<Object> fstEnumOpto = new BytesRefFSTEnum<>(fst);
    long start = System.nanoTime();
    for (int i = 0; i < COUNT; i++) {
      assertNotNull(fstEnumOpto.seekExact(new BytesRef(words.get(random.nextInt(words.size())))));
    }
    return System.nanoTime() - start;
  }

  @Ignore("requires english dictionary")
  public void testLookupRandomStrings() throws Exception {
    dict = new HashSet<>(words);
    List<String> tokens = new ArrayList<>();
    for (int i = 0; i < 1_000_000; i++) {
      String s;
      do  {
        s = randomString();
      } while (dict.contains(s));
      tokens.add(s);
    }
    final FST<Object> fstBase = buildEnglishFST(false);
    final FST<Object> fstOpto = buildFST(words, true);
    long seed = random().nextLong();
    for (int i = 0; i < 10; i++) {
      random.setSeed(seed);
      long timeBase = timeLookupRandomStrings(fstBase, tokens);
      random.setSeed(seed);
      long timeOpto = timeLookupRandomStrings(fstOpto, tokens);
      printf("Sought %d absent terms in %d ms (base) / %d ms (opto), a %d%% change", COUNT, nsToMs(timeBase), nsToMs(timeOpto),
             -100 * (timeBase - timeOpto) / timeBase);
    }
  }

  private long timeLookupRandomStrings(FST<Object> fst, List<String> tokens) throws Exception {
    final BytesRefFSTEnum<Object> fstEnum = new BytesRefFSTEnum<>(fst);
    long start = System.nanoTime();
    for (int i = 0; i < COUNT; i++) {
      fstEnum.seekExact(new BytesRef(tokens.get(random.nextInt(tokens.size()))));
    }
    return System.nanoTime() - start;
  }

  private String randomString() {
    int len = random().nextInt(7) + 3;
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < len; i++) {
      buf.append(random().nextInt(26) + 'a');
    }
    return buf.toString();
  }

  private FST<Object> buildEnglishFST(boolean useDirectAddressing) throws Exception {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("WORDS"), "ASCII"))) {
      String line;
      while ((line = reader.readLine()) != null) {
        words.add(line);
      }
    }
    return buildFST(words, useDirectAddressing);
  }

  private FST<Object> buildFST(List<String> words, boolean useDirectAddressing) throws Exception {
    long start = System.nanoTime();
    final Outputs<Object> outputs = NoOutputs.getSingleton();
    final Builder<Object> b = new Builder<>(FST.INPUT_TYPE.BYTE1, 0, 0, true, true, Integer.MAX_VALUE, outputs, true, 15, useDirectAddressing);

    for (String word : words) {
      b.add(Util.toIntsRef(new BytesRef(word), new IntsRefBuilder()), outputs.getNoOutput());
    }
    FST<Object> fst = b.finish();
    long t = System.nanoTime();
    printf("Built FST of %d bytes in %d ms", fst.ramBytesUsed(), nsToMs(t - start));
    return fst;
  }

  static void printf(String format, Object ... values) {
    System.out.println(String.format(Locale.ROOT, format, values));
  }

  static long nsToMs(long ns) {
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
