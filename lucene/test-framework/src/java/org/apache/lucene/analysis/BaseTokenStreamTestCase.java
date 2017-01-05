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
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Rethrow;
import org.apache.lucene.util.TestUtil;

/** 
 * Base class for all Lucene unit tests that use TokenStreams. 
 * <p>
 * When writing unit tests for analysis components, it's highly recommended
 * to use the helper methods here (especially in conjunction with {@link MockAnalyzer} or
 * {@link MockTokenizer}), as they contain many assertions and checks to 
 * catch bugs.
 * 
 * @see MockAnalyzer
 * @see MockTokenizer
 */
public abstract class BaseTokenStreamTestCase extends LuceneTestCase {
  // some helpers to test Analyzers and TokenStreams:
  
  /**
   * Attribute that records if it was cleared or not.  This is used 
   * for testing that clearAttributes() was called correctly.
   */
  public static interface CheckClearAttributesAttribute extends Attribute {
    boolean getAndResetClearCalled();
  }

  /**
   * Attribute that records if it was cleared or not.  This is used 
   * for testing that clearAttributes() was called correctly.
   */
  public static final class CheckClearAttributesAttributeImpl extends AttributeImpl implements CheckClearAttributesAttribute {
    private boolean clearCalled = false;
    
    @Override
    public boolean getAndResetClearCalled() {
      try {
        return clearCalled;
      } finally {
        clearCalled = false;
      }
    }

    @Override
    public void clear() {
      clearCalled = true;
    }

    @Override
    public boolean equals(Object other) {
      return (
        other instanceof CheckClearAttributesAttributeImpl &&
        ((CheckClearAttributesAttributeImpl) other).clearCalled == this.clearCalled
      );
    }

    @Override
    public int hashCode() {
      return 76137213 ^ Boolean.valueOf(clearCalled).hashCode();
    }
    
    @Override
    public void copyTo(AttributeImpl target) {
      ((CheckClearAttributesAttributeImpl) target).clear();
    }

    @Override
    public void reflectWith(AttributeReflector reflector) {
      reflector.reflect(CheckClearAttributesAttribute.class, "clearCalled", clearCalled);
    }
  }

  // offsetsAreCorrect also validates:
  //   - graph offsets are correct (all tokens leaving from
  //     pos X have the same startOffset; all tokens
  //     arriving to pos Y have the same endOffset)
  //   - offsets only move forwards (startOffset >=
  //     lastStartOffset)
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[],
                                               int posLengths[], Integer finalOffset, Integer finalPosInc, boolean[] keywordAtts,
                                               boolean offsetsAreCorrect) throws IOException {
    assertNotNull(output);
    CheckClearAttributesAttribute checkClearAtt = ts.addAttribute(CheckClearAttributesAttribute.class);
    
    CharTermAttribute termAtt = null;
    if (output.length > 0) {
      assertTrue("has no CharTermAttribute", ts.hasAttribute(CharTermAttribute.class));
      termAtt = ts.getAttribute(CharTermAttribute.class);
    }
    
    OffsetAttribute offsetAtt = null;
    if (startOffsets != null || endOffsets != null || finalOffset != null) {
      assertTrue("has no OffsetAttribute", ts.hasAttribute(OffsetAttribute.class));
      offsetAtt = ts.getAttribute(OffsetAttribute.class);
    }
    
    TypeAttribute typeAtt = null;
    if (types != null) {
      assertTrue("has no TypeAttribute", ts.hasAttribute(TypeAttribute.class));
      typeAtt = ts.getAttribute(TypeAttribute.class);
    }
    
    PositionIncrementAttribute posIncrAtt = null;
    if (posIncrements != null || finalPosInc != null) {
      assertTrue("has no PositionIncrementAttribute", ts.hasAttribute(PositionIncrementAttribute.class));
      posIncrAtt = ts.getAttribute(PositionIncrementAttribute.class);
    }

    PositionLengthAttribute posLengthAtt = null;
    if (posLengths != null) {
      assertTrue("has no PositionLengthAttribute", ts.hasAttribute(PositionLengthAttribute.class));
      posLengthAtt = ts.getAttribute(PositionLengthAttribute.class);
    }

    KeywordAttribute keywordAtt = null;
    if (keywordAtts != null) {
      assertTrue("has no KeywordAttribute", ts.hasAttribute(KeywordAttribute.class));
      keywordAtt = ts.getAttribute(KeywordAttribute.class);
    }
    
    // Maps position to the start/end offset:
    final Map<Integer,Integer> posToStartOffset = new HashMap<>();
    final Map<Integer,Integer> posToEndOffset = new HashMap<>();

    ts.reset();
    int pos = -1;
    int lastStartOffset = 0;
    for (int i = 0; i < output.length; i++) {
      // extra safety to enforce, that the state is not preserved and also assign bogus values
      ts.clearAttributes();
      termAtt.setEmpty().append("bogusTerm");
      if (offsetAtt != null) offsetAtt.setOffset(14584724,24683243);
      if (typeAtt != null) typeAtt.setType("bogusType");
      if (posIncrAtt != null) posIncrAtt.setPositionIncrement(45987657);
      if (posLengthAtt != null) posLengthAtt.setPositionLength(45987653);
      if (keywordAtt != null) keywordAtt.setKeyword((i&1) == 0);
      
      checkClearAtt.getAndResetClearCalled(); // reset it, because we called clearAttribute() before
      assertTrue("token "+i+" does not exist", ts.incrementToken());
      assertTrue("clearAttributes() was not called correctly in TokenStream chain", checkClearAtt.getAndResetClearCalled());
      
      assertEquals("term "+i, output[i], termAtt.toString());
      if (startOffsets != null) {
        assertEquals("startOffset " + i + " term=" + termAtt, startOffsets[i], offsetAtt.startOffset());
      }
      if (endOffsets != null) {
        assertEquals("endOffset " + i + " term=" + termAtt, endOffsets[i], offsetAtt.endOffset());
      }
      if (types != null) {
        assertEquals("type " + i + " term=" + termAtt, types[i], typeAtt.type());
      }
      if (posIncrements != null) {
        assertEquals("posIncrement " + i + " term=" + termAtt, posIncrements[i], posIncrAtt.getPositionIncrement());
      }
      if (posLengths != null) {
        assertEquals("posLength " + i + " term=" + termAtt, posLengths[i], posLengthAtt.getPositionLength());
      }
      if (keywordAtts != null) {
        assertEquals("keywordAtt " + i + " term=" + termAtt, keywordAtts[i], keywordAtt.isKeyword());
      }
      
      // we can enforce some basic things about a few attributes even if the caller doesn't check:
      if (offsetAtt != null) {
        final int startOffset = offsetAtt.startOffset();
        final int endOffset = offsetAtt.endOffset();
        if (finalOffset != null) {
          assertTrue("startOffset (= " + startOffset + ") must be <= finalOffset (= " + finalOffset + ") term=" + termAtt, startOffset <= finalOffset.intValue());
          assertTrue("endOffset must be <= finalOffset: got endOffset=" + endOffset + " vs finalOffset=" + finalOffset.intValue() + " term=" + termAtt,
                     endOffset <= finalOffset.intValue());
        }

        if (offsetsAreCorrect) {
          assertTrue("offsets must not go backwards startOffset=" + startOffset + " is < lastStartOffset=" + lastStartOffset + " term=" + termAtt, offsetAtt.startOffset() >= lastStartOffset);
          lastStartOffset = offsetAtt.startOffset();
        }

        if (offsetsAreCorrect && posLengthAtt != null && posIncrAtt != null) {
          // Validate offset consistency in the graph, ie
          // all tokens leaving from a certain pos have the
          // same startOffset, and all tokens arriving to a
          // certain pos have the same endOffset:
          final int posInc = posIncrAtt.getPositionIncrement();
          pos += posInc;

          final int posLength = posLengthAtt.getPositionLength();

          if (!posToStartOffset.containsKey(pos)) {
            // First time we've seen a token leaving from this position:
            posToStartOffset.put(pos, startOffset);
            //System.out.println("  + s " + pos + " -> " + startOffset);
          } else {
            // We've seen a token leaving from this position
            // before; verify the startOffset is the same:
            //System.out.println("  + vs " + pos + " -> " + startOffset);
            assertEquals(i + " inconsistent startOffset: pos=" + pos + " posLen=" + posLength + " token=" + termAtt, posToStartOffset.get(pos).intValue(), startOffset);
          }

          final int endPos = pos + posLength;

          if (!posToEndOffset.containsKey(endPos)) {
            // First time we've seen a token arriving to this position:
            posToEndOffset.put(endPos, endOffset);
            //System.out.println("  + e " + endPos + " -> " + endOffset);
          } else {
            // We've seen a token arriving to this position
            // before; verify the endOffset is the same:
            //System.out.println("  + ve " + endPos + " -> " + endOffset);
            assertEquals("inconsistent endOffset " + i + " pos=" + pos + " posLen=" + posLength + " token=" + termAtt, posToEndOffset.get(endPos).intValue(), endOffset);
          }
        }
      }
      if (posIncrAtt != null) {
        if (i == 0) {
          assertTrue("first posIncrement must be >= 1", posIncrAtt.getPositionIncrement() >= 1);
        } else {
          assertTrue("posIncrement must be >= 0", posIncrAtt.getPositionIncrement() >= 0);
        }
      }
      if (posLengthAtt != null) {
        assertTrue("posLength must be >= 1", posLengthAtt.getPositionLength() >= 1);
      }
    }

    if (ts.incrementToken()) {
      fail("TokenStream has more tokens than expected (expected count=" + output.length + "); extra token=" + termAtt);
    }

    // repeat our extra safety checks for end()
    ts.clearAttributes();
    if (termAtt != null) termAtt.setEmpty().append("bogusTerm");
    if (offsetAtt != null) offsetAtt.setOffset(14584724,24683243);
    if (typeAtt != null) typeAtt.setType("bogusType");
    if (posIncrAtt != null) posIncrAtt.setPositionIncrement(45987657);
    if (posLengthAtt != null) posLengthAtt.setPositionLength(45987653);
    
    checkClearAtt.getAndResetClearCalled(); // reset it, because we called clearAttribute() before

    ts.end();
    assertTrue("super.end()/clearAttributes() was not called correctly in end()", checkClearAtt.getAndResetClearCalled());
    
    if (finalOffset != null) {
      assertEquals("finalOffset", finalOffset.intValue(), offsetAtt.endOffset());
    }
    if (offsetAtt != null) {
      assertTrue("finalOffset must be >= 0", offsetAtt.endOffset() >= 0);
    }
    if (finalPosInc != null) {
      assertEquals("finalPosInc", finalPosInc.intValue(), posIncrAtt.getPositionIncrement());
    }

    ts.close();
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[],
                                               int posLengths[], Integer finalOffset, boolean[] keywordAtts,
                                               boolean offsetsAreCorrect) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, types, posIncrements, posLengths, finalOffset, null, null, offsetsAreCorrect);
  }

  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[], int posLengths[], Integer finalOffset, boolean offsetsAreCorrect) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, types, posIncrements, posLengths, finalOffset, null, offsetsAreCorrect);
  }

  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[], int posLengths[], Integer finalOffset) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, types, posIncrements, posLengths, finalOffset, true);
  }

  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[], Integer finalOffset) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, types, posIncrements, null, finalOffset);
  }

  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[]) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, types, posIncrements, null, null);
  }

  public static void assertTokenStreamContents(TokenStream ts, String[] output) throws IOException {
    assertTokenStreamContents(ts, output, null, null, null, null, null, null);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, String[] types) throws IOException {
    assertTokenStreamContents(ts, output, null, null, types, null, null, null);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int[] posIncrements) throws IOException {
    assertTokenStreamContents(ts, output, null, null, null, posIncrements, null, null);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[]) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, null, null, null, null);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], Integer finalOffset) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, null, null, null, finalOffset);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], int[] posIncrements) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, null, posIncrements, null, null);
  }

  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], int[] posIncrements, Integer finalOffset) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, null, posIncrements, null, finalOffset);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], int[] posIncrements, int[] posLengths, Integer finalOffset) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, null, posIncrements, posLengths, finalOffset);
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[]) throws IOException {
    checkResetException(a, input);
    checkAnalysisConsistency(random(), a, true, input);
    assertTokenStreamContents(a.tokenStream("dummy", input), output, startOffsets, endOffsets, types, posIncrements, null, input.length());
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[], int posLengths[]) throws IOException {
    checkResetException(a, input);
    checkAnalysisConsistency(random(), a, true, input);
    assertTokenStreamContents(a.tokenStream("dummy", input), output, startOffsets, endOffsets, types, posIncrements, posLengths, input.length());
  }

  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[], int posLengths[], boolean offsetsAreCorrect) throws IOException {
    checkResetException(a, input);
    checkAnalysisConsistency(random(), a, true, input, offsetsAreCorrect);
    assertTokenStreamContents(a.tokenStream("dummy", input), output, startOffsets, endOffsets, types, posIncrements, posLengths, input.length(), offsetsAreCorrect);
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output) throws IOException {
    assertAnalyzesTo(a, input, output, null, null, null, null, null);
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, String[] types) throws IOException {
    assertAnalyzesTo(a, input, output, null, null, types, null, null);
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int[] posIncrements) throws IOException {
    assertAnalyzesTo(a, input, output, null, null, null, posIncrements, null);
  }

  public static void assertAnalyzesToPositions(Analyzer a, String input, String[] output, int[] posIncrements, int[] posLengths) throws IOException {
    assertAnalyzesTo(a, input, output, null, null, null, posIncrements, posLengths);
  }

  public static void assertAnalyzesToPositions(Analyzer a, String input, String[] output, String[] types, int[] posIncrements, int[] posLengths) throws IOException {
    assertAnalyzesTo(a, input, output, null, null, types, posIncrements, posLengths);
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[]) throws IOException {
    assertAnalyzesTo(a, input, output, startOffsets, endOffsets, null, null, null);
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[], int[] posIncrements) throws IOException {
    assertAnalyzesTo(a, input, output, startOffsets, endOffsets, null, posIncrements, null);
  }

  static void checkResetException(Analyzer a, String input) throws IOException {
    TokenStream ts = a.tokenStream("bogus", input);
    try {
      if (ts.incrementToken()) {
        //System.out.println(ts.reflectAsString(false));
        fail("didn't get expected exception when reset() not called");
      }
    } catch (IllegalStateException expected) {
      // ok
    } catch (Exception unexpected) {
      unexpected.printStackTrace(System.err);
      fail("got wrong exception when reset() not called: " + unexpected);
    } finally {
      // consume correctly
      ts.reset();
      while (ts.incrementToken()) {}
      ts.end();
      ts.close();
    }
    
    // check for a missing close()
    ts = a.tokenStream("bogus", input);
    ts.reset();
    while (ts.incrementToken()) {}
    ts.end();
    try {
      ts = a.tokenStream("bogus", input);
      fail("didn't get expected exception when close() not called");
    } catch (IllegalStateException expected) {
      // ok
    } finally {
      ts.close();
    }
  }

  // simple utility method for testing stemmers
  
  public static void checkOneTerm(Analyzer a, final String input, final String expected) throws IOException {
    assertAnalyzesTo(a, input, new String[]{expected});
  }
  
  /** utility method for blasting tokenstreams with data to make sure they don't do anything crazy */
  public static void checkRandomData(Random random, Analyzer a, int iterations) throws IOException {
    checkRandomData(random, a, iterations, 20, false, true);
  }

  /** utility method for blasting tokenstreams with data to make sure they don't do anything crazy */
  public static void checkRandomData(Random random, Analyzer a, int iterations, int maxWordLength) throws IOException {
    checkRandomData(random, a, iterations, maxWordLength, false, true);
  }
  
  /** 
   * utility method for blasting tokenstreams with data to make sure they don't do anything crazy 
   * @param simple true if only ascii strings will be used (try to avoid)
   */
  public static void checkRandomData(Random random, Analyzer a, int iterations, boolean simple) throws IOException {
    checkRandomData(random, a, iterations, 20, simple, true);
  }
  
  /** Asserts that the given stream has expected number of tokens. */
  public static void assertStreamHasNumberOfTokens(TokenStream ts, int expectedCount) throws IOException {
    ts.reset();
    int count = 0;
    while (ts.incrementToken()) {
      count++;
    }
    ts.end();
    assertEquals("wrong number of tokens", expectedCount, count);
  }
  
  static class AnalysisThread extends Thread {
    final int iterations;
    final int maxWordLength;
    final long seed;
    final Analyzer a;
    final boolean useCharFilter;
    final boolean simple;
    final boolean offsetsAreCorrect;
    final RandomIndexWriter iw;
    final CountDownLatch latch;

    // NOTE: not volatile because we don't want the tests to
    // add memory barriers (ie alter how threads
    // interact)... so this is just "best effort":
    public boolean failed;
    
    AnalysisThread(long seed, CountDownLatch latch, Analyzer a, int iterations, int maxWordLength, boolean useCharFilter, boolean simple, boolean offsetsAreCorrect, RandomIndexWriter iw) {
      this.seed = seed;
      this.a = a;
      this.iterations = iterations;
      this.maxWordLength = maxWordLength;
      this.useCharFilter = useCharFilter;
      this.simple = simple;
      this.offsetsAreCorrect = offsetsAreCorrect;
      this.iw = iw;
      this.latch = latch;
    }
    
    @Override
    public void run() {
      boolean success = false;
      try {
        latch.await();
        // see the part in checkRandomData where it replays the same text again
        // to verify reproducability/reuse: hopefully this would catch thread hazards.
        checkRandomData(new Random(seed), a, iterations, maxWordLength, useCharFilter, simple, offsetsAreCorrect, iw);
        success = true;
      } catch (Exception e) {
        Rethrow.rethrow(e);
      } finally {
        failed = !success;
      }
    }
  };
  
  public static void checkRandomData(Random random, Analyzer a, int iterations, int maxWordLength, boolean simple) throws IOException {
    checkRandomData(random, a, iterations, maxWordLength, simple, true);
  }

  public static void checkRandomData(Random random, Analyzer a, int iterations, int maxWordLength, boolean simple, boolean offsetsAreCorrect) throws IOException {
    checkResetException(a, "best effort");
    long seed = random.nextLong();
    boolean useCharFilter = random.nextBoolean();
    Directory dir = null;
    RandomIndexWriter iw = null;
    final String postingsFormat =  TestUtil.getPostingsFormat("dummy");
    boolean codecOk = iterations * maxWordLength < 100000 ||
        !(postingsFormat.equals("Memory") ||
            postingsFormat.equals("SimpleText"));
    if (rarely(random) && codecOk) {
      dir = newFSDirectory(createTempDir("bttc"));
      iw = new RandomIndexWriter(new Random(seed), dir, a);
    }
    boolean success = false;
    try {
      checkRandomData(new Random(seed), a, iterations, maxWordLength, useCharFilter, simple, offsetsAreCorrect, iw);
      // now test with multiple threads: note we do the EXACT same thing we did before in each thread,
      // so this should only really fail from another thread if it's an actual thread problem
      int numThreads = TestUtil.nextInt(random, 2, 4);
      final CountDownLatch startingGun = new CountDownLatch(1);
      AnalysisThread threads[] = new AnalysisThread[numThreads];
      for (int i = 0; i < threads.length; i++) {
        threads[i] = new AnalysisThread(seed, startingGun, a, iterations, maxWordLength, useCharFilter, simple, offsetsAreCorrect, iw);
      }
      for (int i = 0; i < threads.length; i++) {
        threads[i].start();
      }
      startingGun.countDown();
      for (int i = 0; i < threads.length; i++) {
        try {
          threads[i].join();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      for (int i = 0; i < threads.length; i++) {
        if (threads[i].failed) {
          throw new RuntimeException("some thread(s) failed");
        }
      }
      if (iw != null) {
        iw.close();
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(dir);
      } else {
        IOUtils.closeWhileHandlingException(dir); // checkindex
      }
    }
  }

  private static void checkRandomData(Random random, Analyzer a, int iterations, int maxWordLength, boolean useCharFilter, boolean simple, boolean offsetsAreCorrect, RandomIndexWriter iw) throws IOException {

    final LineFileDocs docs = new LineFileDocs(random);
    Document doc = null;
    Field field = null, currentField = null;
    StringReader bogus = new StringReader("");
    if (iw != null) {
      doc = new Document();
      FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
      if (random.nextBoolean()) {
        ft.setStoreTermVectors(true);
        ft.setStoreTermVectorOffsets(random.nextBoolean());
        ft.setStoreTermVectorPositions(random.nextBoolean());
        if (ft.storeTermVectorPositions()) {
          ft.setStoreTermVectorPayloads(random.nextBoolean());
        }
      }
      if (random.nextBoolean()) {
        ft.setOmitNorms(true);
      }
      switch(random.nextInt(4)) {
        case 0: ft.setIndexOptions(IndexOptions.DOCS); break;
        case 1: ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS); break;
        case 2: ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS); break;
        default:
          if (offsetsAreCorrect) {
            ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
          } else {
            ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
          }
      }
      currentField = field = new Field("dummy", bogus, ft);
      doc.add(currentField);
    }
    
    try {
      for (int i = 0; i < iterations; i++) {
        String text;

        if (random.nextInt(10) == 7) {
          // real data from linedocs
          text = docs.nextDoc().get("body");
          if (text.length() > maxWordLength) {
            
            // Take a random slice from the text...:
            int startPos = random.nextInt(text.length() - maxWordLength);
            if (startPos > 0 && Character.isLowSurrogate(text.charAt(startPos))) {
              // Take care not to split up a surrogate pair:
              startPos--;
              assert Character.isHighSurrogate(text.charAt(startPos));
            }
            int endPos = startPos + maxWordLength - 1;
            if (Character.isHighSurrogate(text.charAt(endPos))) {
              // Take care not to split up a surrogate pair:
              endPos--;
            }
            text = text.substring(startPos, 1+endPos);
          }
        } else {
          // synthetic
          text = TestUtil.randomAnalysisString(random, maxWordLength, simple);
        }

        try {
          checkAnalysisConsistency(random, a, useCharFilter, text, offsetsAreCorrect, currentField);
          if (iw != null) {
            if (random.nextInt(7) == 0) {
              // pile up a multivalued field
              IndexableFieldType ft = field.fieldType();
              currentField = new Field("dummy", bogus, ft);
              doc.add(currentField);
            } else {
              iw.addDocument(doc);
              if (doc.getFields().size() > 1) {
                // back to 1 field
                currentField = field;
                doc.removeFields("dummy");
                doc.add(currentField);
              }
            }
          }
        } catch (Throwable t) {
          // TODO: really we should pass a random seed to
          // checkAnalysisConsistency then print it here too:
          System.err.println("TEST FAIL: useCharFilter=" + useCharFilter + " text='" + escape(text) + "'");
          Rethrow.rethrow(t);
        }
      }
    } finally {
      IOUtils.closeWhileHandlingException(docs);
    }
  }

  public static String escape(String s) {
    int charUpto = 0;
    final StringBuilder sb = new StringBuilder();
    while (charUpto < s.length()) {
      final int c = s.charAt(charUpto);
      if (c == 0xa) {
        // Strangely, you cannot put \ u000A into Java
        // sources (not in a comment nor a string
        // constant)...:
        sb.append("\\n");
      } else if (c == 0xd) {
        // ... nor \ u000D:
        sb.append("\\r");
      } else if (c == '"') {
        sb.append("\\\"");
      } else if (c == '\\') {
        sb.append("\\\\");
      } else if (c >= 0x20 && c < 0x80) {
        sb.append((char) c);
      } else {
        // TODO: we can make ascii easier to read if we
        // don't escape...
        sb.append(String.format(Locale.ROOT, "\\u%04x", c));
      }
      charUpto++;
    }
    return sb.toString();
  }

  public static void checkAnalysisConsistency(Random random, Analyzer a, boolean useCharFilter, String text) throws IOException {
    checkAnalysisConsistency(random, a, useCharFilter, text, true);
  }

  public static void checkAnalysisConsistency(Random random, Analyzer a, boolean useCharFilter, String text, boolean offsetsAreCorrect) throws IOException {
    checkAnalysisConsistency(random, a, useCharFilter, text, offsetsAreCorrect, null);
  }
  
  private static void checkAnalysisConsistency(Random random, Analyzer a, boolean useCharFilter, String text, boolean offsetsAreCorrect, Field field) throws IOException {

    if (VERBOSE) {
      System.out.println(Thread.currentThread().getName() + ": NOTE: BaseTokenStreamTestCase: get first token stream now text=" + text);
    }

    int remainder = random.nextInt(10);
    Reader reader = new StringReader(text);
    TokenStream ts = a.tokenStream("dummy", useCharFilter ? new MockCharFilter(reader, remainder) : reader);
    CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
    OffsetAttribute offsetAtt = ts.getAttribute(OffsetAttribute.class);
    PositionIncrementAttribute posIncAtt = ts.getAttribute(PositionIncrementAttribute.class);
    PositionLengthAttribute posLengthAtt = ts.getAttribute(PositionLengthAttribute.class);
    TypeAttribute typeAtt = ts.getAttribute(TypeAttribute.class);
    List<String> tokens = new ArrayList<>();
    List<String> types = new ArrayList<>();
    List<Integer> positions = new ArrayList<>();
    List<Integer> positionLengths = new ArrayList<>();
    List<Integer> startOffsets = new ArrayList<>();
    List<Integer> endOffsets = new ArrayList<>();
    ts.reset();

    // First pass: save away "correct" tokens
    while (ts.incrementToken()) {
      assertNotNull("has no CharTermAttribute", termAtt);
      tokens.add(termAtt.toString());
      if (typeAtt != null) types.add(typeAtt.type());
      if (posIncAtt != null) positions.add(posIncAtt.getPositionIncrement());
      if (posLengthAtt != null) positionLengths.add(posLengthAtt.getPositionLength());
      if (offsetAtt != null) {
        startOffsets.add(offsetAtt.startOffset());
        endOffsets.add(offsetAtt.endOffset());
      }
    }
    ts.end();
    ts.close();

    // verify reusing is "reproducable" and also get the normal tokenstream sanity checks
    if (!tokens.isEmpty()) {

      // KWTokenizer (for example) can produce a token
      // even when input is length 0:
      if (text.length() != 0) {

        // (Optional) second pass: do something evil:
        final int evilness = random.nextInt(50);
        if (evilness == 17) {
          if (VERBOSE) {
            System.out.println(Thread.currentThread().getName() + ": NOTE: BaseTokenStreamTestCase: re-run analysis w/ exception");
          }
          // Throw an errant exception from the Reader:

          MockReaderWrapper evilReader = new MockReaderWrapper(random, new StringReader(text));
          evilReader.throwExcAfterChar(random.nextInt(text.length()+1));
          reader = evilReader;

          try {
            // NOTE: some Tokenizers go and read characters
            // when you call .setReader(Reader), eg
            // PatternTokenizer.  This is a bit
            // iffy... (really, they should only
            // pull from the Reader when you call
            // .incremenToken(), I think?), but we
            // currently allow it, so, we must call
            // a.tokenStream inside the try since we may
            // hit the exc on init:
            ts = a.tokenStream("dummy", useCharFilter ? new MockCharFilter(reader, remainder) : reader);
            ts.reset();
            while (ts.incrementToken());
            fail("did not hit exception");
          } catch (RuntimeException re) {
            assertTrue(MockReaderWrapper.isMyEvilException(re));
          }
          try {
            ts.end();
          } catch (IllegalStateException ise) {
            // Catch & ignore MockTokenizer's
            // anger...
            if (ise.getMessage().contains("end() called in wrong state=")) {
              // OK
            } else {
              throw ise;
            }
          }
          ts.close();
        } else if (evilness == 7) {
          // Only consume a subset of the tokens:
          final int numTokensToRead = random.nextInt(tokens.size());
          if (VERBOSE) {
            System.out.println(Thread.currentThread().getName() + ": NOTE: BaseTokenStreamTestCase: re-run analysis, only consuming " + numTokensToRead + " of " + tokens.size() + " tokens");
          }

          reader = new StringReader(text);
          ts = a.tokenStream("dummy", useCharFilter ? new MockCharFilter(reader, remainder) : reader);
          ts.reset();
          for(int tokenCount=0;tokenCount<numTokensToRead;tokenCount++) {
            assertTrue(ts.incrementToken());
          }
          try {
            ts.end();
          } catch (IllegalStateException ise) {
            // Catch & ignore MockTokenizer's
            // anger...
            if (ise.getMessage().contains("end() called in wrong state=")) {
              // OK
            } else {
              throw ise;
            }
          }
          ts.close();
        }
      }
    }

    // Final pass: verify clean tokenization matches
    // results from first pass:

    if (VERBOSE) {
      System.out.println(Thread.currentThread().getName() + ": NOTE: BaseTokenStreamTestCase: re-run analysis; " + tokens.size() + " tokens");
    }
    reader = new StringReader(text);

    long seed = random.nextLong();
    random = new Random(seed);
    if (random.nextInt(30) == 7) {
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": NOTE: BaseTokenStreamTestCase: using spoon-feed reader");
      }

      reader = new MockReaderWrapper(random, reader);
    }

    ts = a.tokenStream("dummy", useCharFilter ? new MockCharFilter(reader, remainder) : reader);
    if (typeAtt != null && posIncAtt != null && posLengthAtt != null && offsetAtt != null) {
      // offset + pos + posLength + type
      assertTokenStreamContents(ts, 
                                tokens.toArray(new String[tokens.size()]),
                                toIntArray(startOffsets),
                                toIntArray(endOffsets),
                                types.toArray(new String[types.size()]),
                                toIntArray(positions),
                                toIntArray(positionLengths),
                                text.length(),
                                offsetsAreCorrect);
    } else if (typeAtt != null && posIncAtt != null && offsetAtt != null) {
      // offset + pos + type
      assertTokenStreamContents(ts, 
                                tokens.toArray(new String[tokens.size()]),
                                toIntArray(startOffsets),
                                toIntArray(endOffsets),
                                types.toArray(new String[types.size()]),
                                toIntArray(positions),
                                null,
                                text.length(),
                                offsetsAreCorrect);
    } else if (posIncAtt != null && posLengthAtt != null && offsetAtt != null) {
      // offset + pos + posLength
      assertTokenStreamContents(ts, 
                                tokens.toArray(new String[tokens.size()]),
                                toIntArray(startOffsets),
                                toIntArray(endOffsets),
                                null,
                                toIntArray(positions),
                                toIntArray(positionLengths),
                                text.length(),
                                offsetsAreCorrect);
    } else if (posIncAtt != null && offsetAtt != null) {
      // offset + pos
      assertTokenStreamContents(ts, 
                                tokens.toArray(new String[tokens.size()]),
                                toIntArray(startOffsets),
                                toIntArray(endOffsets),
                                null,
                                toIntArray(positions),
                                null,
                                text.length(),
                                offsetsAreCorrect);
    } else if (offsetAtt != null) {
      // offset
      assertTokenStreamContents(ts, 
                                tokens.toArray(new String[tokens.size()]),
                                toIntArray(startOffsets),
                                toIntArray(endOffsets),
                                null,
                                null,
                                null,
                                text.length(),
                                offsetsAreCorrect);
    } else {
      // terms only
      assertTokenStreamContents(ts, 
                                tokens.toArray(new String[tokens.size()]));
    }

    a.normalize("dummy", text);
    // TODO: what can we do besides testing that the above method does not throw?

    if (field != null) {
      reader = new StringReader(text);
      random = new Random(seed);
      if (random.nextInt(30) == 7) {
        if (VERBOSE) {
          System.out.println(Thread.currentThread().getName() + ": NOTE: BaseTokenStreamTestCase: indexing using spoon-feed reader");
        }

        reader = new MockReaderWrapper(random, reader);
      }

      field.setReaderValue(useCharFilter ? new MockCharFilter(reader, remainder) : reader);
    }
  }

  protected String toDot(Analyzer a, String inputText) throws IOException {
    final StringWriter sw = new StringWriter();
    final TokenStream ts = a.tokenStream("field", inputText);
    ts.reset();
    new TokenStreamToDot(inputText, ts, new PrintWriter(sw)).toDot();
    return sw.toString();
  }

  protected void toDotFile(Analyzer a, String inputText, String localFileName) throws IOException {
    Writer w = Files.newBufferedWriter(Paths.get(localFileName), StandardCharsets.UTF_8);
    final TokenStream ts = a.tokenStream("field", inputText);
    ts.reset();
    new TokenStreamToDot(inputText, ts, new PrintWriter(w)).toDot();
    w.close();
  }
  
  static int[] toIntArray(List<Integer> list) {
    int ret[] = new int[list.size()];
    int offset = 0;
    for (Integer i : list) {
      ret[offset++] = i;
    }
    return ret;
  }

  protected static MockTokenizer whitespaceMockTokenizer(Reader input) throws IOException {
    MockTokenizer mockTokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    mockTokenizer.setReader(input);
    return mockTokenizer;
  }

  protected static MockTokenizer whitespaceMockTokenizer(String input) throws IOException {
    MockTokenizer mockTokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    mockTokenizer.setReader(new StringReader(input));
    return mockTokenizer;
  }

  protected static MockTokenizer keywordMockTokenizer(Reader input) throws IOException {
    MockTokenizer mockTokenizer = new MockTokenizer(MockTokenizer.KEYWORD, false);
    mockTokenizer.setReader(input);
    return mockTokenizer;
  }

  protected static MockTokenizer keywordMockTokenizer(String input) throws IOException {
    MockTokenizer mockTokenizer = new MockTokenizer(MockTokenizer.KEYWORD, false);
    mockTokenizer.setReader(new StringReader(input));
    return mockTokenizer;
  }
  
  /** Returns a random AttributeFactory impl */
  public static AttributeFactory newAttributeFactory(Random random) {
    switch (random.nextInt(3)) {
      case 0:
        return TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY;
      case 1:
        return Token.TOKEN_ATTRIBUTE_FACTORY;
      case 2:
        return AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY;
      default:
        throw new AssertionError("Please fix the Random.nextInt() call above");
    }
  }
  
  /** Returns a random AttributeFactory impl */
  public static AttributeFactory newAttributeFactory() {
    return newAttributeFactory(random());
  }
}
