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
package org.apache.lucene.analysis;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.English;

public class TestStopFilter extends BaseTokenStreamTestCase {

  private final static int MAX_NUMBER_OF_TOKENS = 50;
  
  // other StopFilter functionality is already tested by TestStopAnalyzer

  public void testExactCase() throws IOException {
    StringReader reader = new StringReader("Now is The Time");
    CharArraySet stopWords = new CharArraySet(asSet("is", "the", "Time"), false);
    final MockTokenizer in = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    in.setReader(reader);
    TokenStream stream = new StopFilter(in, stopWords);
    assertTokenStreamContents(stream, new String[] { "Now", "The" });
  }

  public void testStopFilter() throws IOException {
    StringReader reader = new StringReader("Now is The Time");
    String[] stopWords = new String[] { "is", "the", "Time" };
    CharArraySet stopSet = StopFilter.makeStopSet(stopWords);
    final MockTokenizer in = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    in.setReader(reader);
    TokenStream stream = new StopFilter(in, stopSet);
    assertTokenStreamContents(stream, new String[] { "Now", "The" });
  }


  private static void logStopwords(String name, Collection<String> stopwords){
    log(String.format(Locale.ROOT, "stopword list [%s]: %s", name, stopwords.isEmpty() ? "Empty" : stopwords.toString()));
  }

  /**
   * Randomly generate a document and a list of stopwords to apply
   * @param numberOfTokens max number of tokens in the document
   * @param sb will contain the text at the end of the method
   * @param stopwords will contain the list of the stopwords at the end of the method
   * @param stopwordPositions will contain the position of the stopwords at the end of the method
   */
  private static void generateTestSetWithStopwordsAndStopwordPositions(int numberOfTokens, StringBuilder sb, List<String> stopwords, List<Integer> stopwordPositions){
    Random rand = random();
    for (int i = 0; i < numberOfTokens; i++) {
      String token = English.intToEnglish(i).trim();
      sb.append(token).append(' ');
      if (i == 0 || rand.nextBoolean()) {
        // with probability 0.5 will tell if this is a stopword or
        // no - adding always the first token to make sure that the
        // list of stopwords is not empty;
        stopwords.add(token);
        stopwordPositions.add(i);
      }
    }
    log("Number of tokens : "+numberOfTokens);
    log("Document : "+sb.toString());
    logStopwords("Stopwords", stopwords);
  }

  /**
   * Check that the positions of the terms in a document keep into account the fact
   * that some of the words were filtered by the StopwordFilter
   */
  public void testTokenPositionWithStopwordFilter() throws IOException {
    // at least 1 token
    final int numberOfTokens = random().nextInt(MAX_NUMBER_OF_TOKENS-1)+1;
    StringBuilder sb = new StringBuilder();
    List<String> stopwords = new ArrayList<>(numberOfTokens);
    List<Integer> stopwordPositions = new ArrayList<>(numberOfTokens);
    generateTestSetWithStopwordsAndStopwordPositions(numberOfTokens, sb, stopwords, stopwordPositions);

    CharArraySet stopSet = StopFilter.makeStopSet(stopwords);
    logStopwords("All stopwords", stopwords);
    // with increments
    StringReader reader = new StringReader(sb.toString());
    final MockTokenizer in = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    in.setReader(reader);
    StopFilter stopfilter = new StopFilter(in, stopSet);
    doTestStopwordsPositions(stopfilter, stopwordPositions, numberOfTokens);
  }

  /**
   * Check that the positions of the terms in a document keep into account the fact
   * that some of the words were filtered by two StopwordFilters concatenated together.
   */
  public void testTokenPositionsWithConcatenatedStopwordFilters() throws IOException {
    // at least 1 token
    final int numberOfTokens = random().nextInt(MAX_NUMBER_OF_TOKENS-1)+1;
    StringBuilder sb = new StringBuilder();
    List<String> stopwords = new ArrayList<>(numberOfTokens);
    List<Integer> stopwordPositions = new ArrayList<>();
    generateTestSetWithStopwordsAndStopwordPositions(numberOfTokens, sb, stopwords, stopwordPositions);

    // we want to make sure that concatenating two list of stopwords
    // produce the same results of using one unique list of stopwords.
    // So we first generate a list of stopwords:
    // e.g.: [a, b, c, d, e]
    // and then we split the list in two disjoint partitions
    // e.g. [a, c, e] [b, d]
    int partition = random().nextInt(stopwords.size());
    Collections.shuffle(stopwords, random());
    final List<String> stopwordsRandomPartition = stopwords.subList(0, partition);
    final Set<String> stopwordsRemaining = new HashSet<>(stopwords);
    stopwordsRemaining.removeAll(stopwordsRandomPartition); // remove the first partition from all the stopwords

    CharArraySet firstStopSet = StopFilter.makeStopSet(stopwordsRandomPartition);
    logStopwords("Stopwords-first", stopwordsRandomPartition);
    CharArraySet secondStopSet = StopFilter.makeStopSet(new ArrayList<>(stopwordsRemaining), false);
    logStopwords("Stopwords-second", stopwordsRemaining);

    Reader reader = new StringReader(sb.toString());
    final MockTokenizer in1 = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    in1.setReader(reader);

    // Here we create a stopFilter with the stopwords in the first partition and then we
    // concatenate it with the stopFilter created with the stopwords in the second partition
    StopFilter stopFilter = new StopFilter(in1, firstStopSet); // first part of the set
    StopFilter concatenatedStopFilter = new StopFilter(stopFilter, secondStopSet); // two stop filters concatenated!

    // ... and finally we check that the positions of the filtered tokens matched using the concatenated
    // stopFilters match the positions of the filtered tokens using the unique original list of stopwords
    doTestStopwordsPositions(concatenatedStopFilter, stopwordPositions, numberOfTokens);
  }

  // LUCENE-3849: make sure after .end() we see the "ending" posInc
  public void testEndStopword() throws Exception {
    CharArraySet stopSet = StopFilter.makeStopSet("of");
    final MockTokenizer in = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    in.setReader(new StringReader("test of"));
    StopFilter stopfilter = new StopFilter(in, stopSet);
    assertTokenStreamContents(stopfilter, new String[] { "test" },
                              new int[] {0},
                              new int[] {4},
                              null,
                              new int[] {1},
                              null,
                              7,
                              1,
                              null,
                              true,
                              null);
  }

  private void doTestStopwordsPositions(StopFilter stopfilter, List<Integer> stopwordPositions, final int numberOfTokens) throws IOException {
    CharTermAttribute termAtt = stopfilter.getAttribute(CharTermAttribute.class);
    PositionIncrementAttribute posIncrAtt = stopfilter.getAttribute(PositionIncrementAttribute.class);
    stopfilter.reset();
    log("Test stopwords positions:");
    for (int i=0; i<numberOfTokens; i++) {
      if (stopwordPositions.contains(i)){
        // if i is in stopwordPosition it is a stopword and we skip this position
        continue;
      }
      assertTrue(stopfilter.incrementToken());
      log(String.format(Locale.ROOT, "token %d: %s", i, termAtt.toString()));
      String token = English.intToEnglish(i).trim();
      assertEquals(String.format(Locale.ROOT, "expecting token %d to be %s", i, token), token, termAtt.toString());
    }
    assertFalse(stopfilter.incrementToken());
    stopfilter.end();
    stopfilter.close();
    log("----------");
  }
  
  // print debug info depending on VERBOSE
  private static void log(String s) {
    if (VERBOSE) {
      System.out.println(s);
    }
  }
}
