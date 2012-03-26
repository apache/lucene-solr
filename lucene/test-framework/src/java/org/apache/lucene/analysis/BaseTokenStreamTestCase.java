package org.apache.lucene.analysis;

/**
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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util._TestUtil;

/** 
 * Base class for all Lucene unit tests that use TokenStreams. 
 * <p>
 * When writing unit tests for analysis components, its highly recommended
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
  }

  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[], int posLengths[], Integer finalOffset) throws IOException {
    assertNotNull(output);
    CheckClearAttributesAttribute checkClearAtt = ts.addAttribute(CheckClearAttributesAttribute.class);
    
    assertTrue("has no CharTermAttribute", ts.hasAttribute(CharTermAttribute.class));
    CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
    
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
    if (posIncrements != null) {
      assertTrue("has no PositionIncrementAttribute", ts.hasAttribute(PositionIncrementAttribute.class));
      posIncrAtt = ts.getAttribute(PositionIncrementAttribute.class);
    }

    PositionLengthAttribute posLengthAtt = null;
    if (posLengths != null) {
      assertTrue("has no PositionLengthAttribute", ts.hasAttribute(PositionLengthAttribute.class));
      posLengthAtt = ts.getAttribute(PositionLengthAttribute.class);
    }
    
    ts.reset();
    for (int i = 0; i < output.length; i++) {
      // extra safety to enforce, that the state is not preserved and also assign bogus values
      ts.clearAttributes();
      termAtt.setEmpty().append("bogusTerm");
      if (offsetAtt != null) offsetAtt.setOffset(14584724,24683243);
      if (typeAtt != null) typeAtt.setType("bogusType");
      if (posIncrAtt != null) posIncrAtt.setPositionIncrement(45987657);
      if (posLengthAtt != null) posLengthAtt.setPositionLength(45987653);
      
      checkClearAtt.getAndResetClearCalled(); // reset it, because we called clearAttribute() before
      assertTrue("token "+i+" does not exist", ts.incrementToken());
      assertTrue("clearAttributes() was not called correctly in TokenStream chain", checkClearAtt.getAndResetClearCalled());
      
      assertEquals("term "+i, output[i], termAtt.toString());
      if (startOffsets != null)
        assertEquals("startOffset "+i, startOffsets[i], offsetAtt.startOffset());
      if (endOffsets != null)
        assertEquals("endOffset "+i, endOffsets[i], offsetAtt.endOffset());
      if (types != null)
        assertEquals("type "+i, types[i], typeAtt.type());
      if (posIncrements != null)
        assertEquals("posIncrement "+i, posIncrements[i], posIncrAtt.getPositionIncrement());
      if (posLengths != null)
        assertEquals("posLength "+i, posLengths[i], posLengthAtt.getPositionLength());
      
      // we can enforce some basic things about a few attributes even if the caller doesn't check:
      if (offsetAtt != null) {
        assertTrue("startOffset must be >= 0", offsetAtt.startOffset() >= 0);
        assertTrue("endOffset must be >= 0", offsetAtt.endOffset() >= 0);
        assertTrue("endOffset must be >= startOffset, got startOffset=" + offsetAtt.startOffset() + ",endOffset=" + offsetAtt.endOffset(), 
            offsetAtt.endOffset() >= offsetAtt.startOffset());
        if (finalOffset != null) {
          assertTrue("startOffset must be <= finalOffset", offsetAtt.startOffset() <= finalOffset.intValue());
          assertTrue("endOffset must be <= finalOffset: got endOffset=" + offsetAtt.endOffset() + " vs finalOffset=" + finalOffset.intValue(),
                     offsetAtt.endOffset() <= finalOffset.intValue());
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
    assertFalse("TokenStream has more tokens than expected", ts.incrementToken());
    ts.end();
    if (finalOffset != null) {
      assertEquals("finalOffset ", finalOffset.intValue(), offsetAtt.endOffset());
    }
    if (offsetAtt != null) {
      assertTrue("finalOffset must be >= 0", offsetAtt.endOffset() >= 0);
    }
    ts.close();
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
    assertTokenStreamContents(a.tokenStream("dummy", new StringReader(input)), output, startOffsets, endOffsets, types, posIncrements, null, input.length());
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[], int posLengths[]) throws IOException {
    assertTokenStreamContents(a.tokenStream("dummy", new StringReader(input)), output, startOffsets, endOffsets, types, posIncrements, posLengths, input.length());
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
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[]) throws IOException {
    assertAnalyzesTo(a, input, output, startOffsets, endOffsets, null, null, null);
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[], int[] posIncrements) throws IOException {
    assertAnalyzesTo(a, input, output, startOffsets, endOffsets, null, posIncrements, null);
  }
  

  public static void assertAnalyzesToReuse(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[]) throws IOException {
    assertTokenStreamContents(a.tokenStream("dummy", new StringReader(input)), output, startOffsets, endOffsets, types, posIncrements, null, input.length());
  }
  
  public static void assertAnalyzesToReuse(Analyzer a, String input, String[] output) throws IOException {
    assertAnalyzesToReuse(a, input, output, null, null, null, null);
  }
  
  public static void assertAnalyzesToReuse(Analyzer a, String input, String[] output, String[] types) throws IOException {
    assertAnalyzesToReuse(a, input, output, null, null, types, null);
  }
  
  public static void assertAnalyzesToReuse(Analyzer a, String input, String[] output, int[] posIncrements) throws IOException {
    assertAnalyzesToReuse(a, input, output, null, null, null, posIncrements);
  }
  
  public static void assertAnalyzesToReuse(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[]) throws IOException {
    assertAnalyzesToReuse(a, input, output, startOffsets, endOffsets, null, null);
  }
  
  public static void assertAnalyzesToReuse(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[], int[] posIncrements) throws IOException {
    assertAnalyzesToReuse(a, input, output, startOffsets, endOffsets, null, posIncrements);
  }

  // simple utility method for testing stemmers
  
  public static void checkOneTerm(Analyzer a, final String input, final String expected) throws IOException {
    assertAnalyzesTo(a, input, new String[]{expected});
  }
  
  public static void checkOneTermReuse(Analyzer a, final String input, final String expected) throws IOException {
    assertAnalyzesToReuse(a, input, new String[]{expected});
  }
  
  /** utility method for blasting tokenstreams with data to make sure they don't do anything crazy */
  public static void checkRandomData(Random random, Analyzer a, int iterations) throws IOException {
    checkRandomData(random, a, iterations, 20, false);
  }
  
  /** utility method for blasting tokenstreams with data to make sure they don't do anything crazy */
  public static void checkRandomData(Random random, Analyzer a, int iterations, int maxWordLength) throws IOException {
    checkRandomData(random, a, iterations, maxWordLength, false);
  }
  
  /** 
   * utility method for blasting tokenstreams with data to make sure they don't do anything crazy 
   * @param simple true if only ascii strings will be used (try to avoid)
   */
  public static void checkRandomData(Random random, Analyzer a, int iterations, boolean simple) throws IOException {
    checkRandomData(random, a, iterations, 20, simple);
  }
  
  static class AnalysisThread extends Thread {
    final int iterations;
    final int maxWordLength;
    final Random random;
    final Analyzer a;
    final boolean simple;
    
    AnalysisThread(Random random, Analyzer a, int iterations, int maxWordLength, boolean simple) {
      this.random = random;
      this.a = a;
      this.iterations = iterations;
      this.maxWordLength = maxWordLength;
      this.simple = simple;
    }
    
    @Override
    public void run() {
      try {
        // see the part in checkRandomData where it replays the same text again
        // to verify reproducability/reuse: hopefully this would catch thread hazards.
        checkRandomData(random, a, iterations, maxWordLength, random.nextBoolean(), simple);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  };
  
  public static void checkRandomData(Random random, Analyzer a, int iterations, int maxWordLength, boolean simple) throws IOException {
    checkRandomData(random, a, iterations, maxWordLength, random.nextBoolean(), simple);
    // now test with multiple threads
    int numThreads = _TestUtil.nextInt(random, 4, 8);
    Thread threads[] = new Thread[numThreads];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new AnalysisThread(new Random(random.nextLong()), a, iterations, maxWordLength, simple);
    }
    for (int i = 0; i < threads.length; i++) {
      threads[i].start();
    }
    for (int i = 0; i < threads.length; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static void checkRandomData(Random random, Analyzer a, int iterations, int maxWordLength, boolean useCharFilter, boolean simple) throws IOException {

    final LineFileDocs docs = new LineFileDocs(random);

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
        text = randomAnalysisString(random, maxWordLength, simple);
      }

      try {
        checkAnalysisConsistency(random, a, useCharFilter, text);
      } catch (Throwable t) {
        System.err.println("TEST FAIL: useCharFilter=" + useCharFilter + " text='" + text + "'");
        throw new RuntimeException(t);
      }
    }
  }

  public static void checkAnalysisConsistency(Random random, Analyzer a, boolean useCharFilter, String text) throws IOException {

    if (VERBOSE) {
      System.out.println(Thread.currentThread().getName() + ": NOTE: BaseTokenStreamTestCase: get first token stream now text=" + text);
    }

    int remainder = random.nextInt(10);
    Reader reader = new StringReader(text);
    TokenStream ts = a.tokenStream("dummy", useCharFilter ? new MockCharFilter(reader, remainder) : reader);
    assertTrue("has no CharTermAttribute", ts.hasAttribute(CharTermAttribute.class));
    CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
    OffsetAttribute offsetAtt = ts.hasAttribute(OffsetAttribute.class) ? ts.getAttribute(OffsetAttribute.class) : null;
    PositionIncrementAttribute posIncAtt = ts.hasAttribute(PositionIncrementAttribute.class) ? ts.getAttribute(PositionIncrementAttribute.class) : null;
    PositionLengthAttribute posLengthAtt = ts.hasAttribute(PositionLengthAttribute.class) ? ts.getAttribute(PositionLengthAttribute.class) : null;
    TypeAttribute typeAtt = ts.hasAttribute(TypeAttribute.class) ? ts.getAttribute(TypeAttribute.class) : null;
    List<String> tokens = new ArrayList<String>();
    List<String> types = new ArrayList<String>();
    List<Integer> positions = new ArrayList<Integer>();
    List<Integer> positionLengths = new ArrayList<Integer>();
    List<Integer> startOffsets = new ArrayList<Integer>();
    List<Integer> endOffsets = new ArrayList<Integer>();
    ts.reset();

    // First pass: save away "correct" tokens
    while (ts.incrementToken()) {
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
            ts = a.tokenStream("dummy", useCharFilter ? new MockCharFilter(evilReader, remainder) : evilReader);
            ts.reset();
            while (ts.incrementToken());
            fail("did not hit exception");
          } catch (RuntimeException re) {
            assertTrue(MockReaderWrapper.isMyEvilException(re));
          }
          try {
            ts.end();
          } catch (AssertionError ae) {
            // Catch & ignore MockTokenizer's
            // anger...
            if ("end() called before incrementToken() returned false!".equals(ae.getMessage())) {
              // OK
            } else {
              throw ae;
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
          } catch (AssertionError ae) {
            // Catch & ignore MockTokenizer's
            // anger...
            if ("end() called before incrementToken() returned false!".equals(ae.getMessage())) {
              // OK
            } else {
              throw ae;
            }
          }
          ts.close();
        }
      }

      // Final pass: verify clean tokenization matches
      // results from first pass:

      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": NOTE: BaseTokenStreamTestCase: re-run analysis; " + tokens.size() + " tokens");
      }
      reader = new StringReader(text);

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
                                  text.length());
      } else if (typeAtt != null && posIncAtt != null && offsetAtt != null) {
        // offset + pos + type
        assertTokenStreamContents(ts, 
                                  tokens.toArray(new String[tokens.size()]),
                                  toIntArray(startOffsets),
                                  toIntArray(endOffsets),
                                  types.toArray(new String[types.size()]),
                                  toIntArray(positions),
                                  null,
                                  text.length());
      } else if (posIncAtt != null && posLengthAtt != null && offsetAtt != null) {
        // offset + pos + posLength
        assertTokenStreamContents(ts, 
                                  tokens.toArray(new String[tokens.size()]),
                                  toIntArray(startOffsets),
                                  toIntArray(endOffsets),
                                  null,
                                  toIntArray(positions),
                                  toIntArray(positionLengths),
                                  text.length());
      } else if (posIncAtt != null && offsetAtt != null) {
        // offset + pos
        assertTokenStreamContents(ts, 
                                  tokens.toArray(new String[tokens.size()]),
                                  toIntArray(startOffsets),
                                  toIntArray(endOffsets),
                                  null,
                                  toIntArray(positions),
                                  null,
                                  text.length());
      } else if (offsetAtt != null) {
        // offset
        assertTokenStreamContents(ts, 
                                  tokens.toArray(new String[tokens.size()]),
                                  toIntArray(startOffsets),
                                  toIntArray(endOffsets),
                                  null,
                                  null,
                                  null,
                                  text.length());
      } else {
        // terms only
        assertTokenStreamContents(ts, 
                                  tokens.toArray(new String[tokens.size()]));
      }
    }
  }
  
  private static String randomAnalysisString(Random random, int maxLength, boolean simple) {
    assert maxLength >= 0;
    
    // sometimes just a purely random string
    if (random.nextInt(31) == 0) {
      return randomSubString(random, random.nextInt(maxLength), simple);
    }
    
    // otherwise, try to make it more realistic with 'words' since most tests use MockTokenizer
    // first decide how big the string will really be: 0..n
    maxLength = random.nextInt(maxLength);
    int avgWordLength = _TestUtil.nextInt(random, 3, 8);
    StringBuilder sb = new StringBuilder();
    while (sb.length() < maxLength) {
      if (sb.length() > 0) {
        sb.append(' ');
      }
      int wordLength = -1;
      while (wordLength < 0) {
        wordLength = (int) (random.nextGaussian() * 3 + avgWordLength);
      }
      wordLength = Math.min(wordLength, maxLength - sb.length());
      sb.append(randomSubString(random, wordLength, simple));
    }
    return sb.toString();
  }
  
  private static String randomSubString(Random random, int wordLength, boolean simple) {
    if (wordLength == 0) {
      return "";
    }
    
    int evilness = _TestUtil.nextInt(random, 0, 20);
    
    StringBuilder sb = new StringBuilder();
    while (sb.length() < wordLength) {;
      if (simple) { 
        sb.append(random.nextBoolean() ? _TestUtil.randomSimpleString(random, wordLength) : _TestUtil.randomHtmlishString(random, wordLength));
      } else {
        if (evilness < 10) {
          sb.append(_TestUtil.randomSimpleString(random, wordLength));
        } else if (evilness < 15) {
          assert sb.length() == 0; // we should always get wordLength back!
          sb.append(_TestUtil.randomRealisticUnicodeString(random, wordLength, wordLength));
        } else if (evilness == 16) {
          sb.append(_TestUtil.randomHtmlishString(random, wordLength));
        } else if (evilness == 17) {
          // gives a lot of punctuation
          sb.append(_TestUtil.randomRegexpishString(random, wordLength));
        } else {
          sb.append(_TestUtil.randomUnicodeString(random, wordLength));
        }
      }
    }
    if (sb.length() > wordLength) {
      sb.setLength(wordLength);
      if (Character.isHighSurrogate(sb.charAt(wordLength-1))) {
        sb.setLength(wordLength-1);
      }
    }
    return sb.toString();
  }

  protected String toDot(Analyzer a, String inputText) throws IOException {
    final StringWriter sw = new StringWriter();
    final TokenStream ts = a.tokenStream("field", new StringReader(inputText));
    ts.reset();
    new TokenStreamToDot(inputText, ts, new PrintWriter(sw)).toDot();
    return sw.toString();
  }

  protected void toDotFile(Analyzer a, String inputText, String localFileName) throws IOException {
    Writer w = new OutputStreamWriter(new FileOutputStream(localFileName), "UTF-8");
    final TokenStream ts = a.tokenStream("field", new StringReader(inputText));
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
}
