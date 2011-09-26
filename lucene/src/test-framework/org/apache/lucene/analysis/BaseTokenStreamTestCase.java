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

import java.io.StringReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
 
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.LuceneTestCase;
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
  
  public static interface CheckClearAttributesAttribute extends Attribute {
    boolean getAndResetClearCalled();
  }

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

  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[], Integer finalOffset) throws IOException {
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
    
    ts.reset();
    for (int i = 0; i < output.length; i++) {
      // extra safety to enforce, that the state is not preserved and also assign bogus values
      ts.clearAttributes();
      termAtt.setEmpty().append("bogusTerm");
      if (offsetAtt != null) offsetAtt.setOffset(14584724,24683243);
      if (typeAtt != null) typeAtt.setType("bogusType");
      if (posIncrAtt != null) posIncrAtt.setPositionIncrement(45987657);
      
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
      
      // we can enforce some basic things about a few attributes even if the caller doesn't check:
      if (offsetAtt != null) {
        assertTrue("startOffset must be >= 0", offsetAtt.startOffset() >= 0);
        assertTrue("endOffset must be >= 0", offsetAtt.endOffset() >= 0);
        assertTrue("endOffset must be >= startOffset", offsetAtt.endOffset() >= offsetAtt.startOffset());
      }
      if (posIncrAtt != null) {
        assertTrue("posIncrement must be >= 0", posIncrAtt.getPositionIncrement() >= 0);
      }
    }
    assertFalse("end of stream", ts.incrementToken());
    ts.end();
    if (finalOffset != null)
      assertEquals("finalOffset ", finalOffset.intValue(), offsetAtt.endOffset());
    if (offsetAtt != null) {
      assertTrue("finalOffset must be >= 0", offsetAtt.endOffset() >= 0);
    }
    ts.close();
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[]) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, types, posIncrements, null);
  }

  public static void assertTokenStreamContents(TokenStream ts, String[] output) throws IOException {
    assertTokenStreamContents(ts, output, null, null, null, null, null);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, String[] types) throws IOException {
    assertTokenStreamContents(ts, output, null, null, types, null, null);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int[] posIncrements) throws IOException {
    assertTokenStreamContents(ts, output, null, null, null, posIncrements, null);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[]) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, null, null, null);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], Integer finalOffset) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, null, null, finalOffset);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], int[] posIncrements) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, null, posIncrements, null);
  }

  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], int[] posIncrements, Integer finalOffset) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, null, posIncrements, finalOffset);
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[]) throws IOException {
    assertTokenStreamContents(a.reusableTokenStream("dummy", new StringReader(input)), output, startOffsets, endOffsets, types, posIncrements, input.length());
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output) throws IOException {
    assertAnalyzesTo(a, input, output, null, null, null, null);
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, String[] types) throws IOException {
    assertAnalyzesTo(a, input, output, null, null, types, null);
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int[] posIncrements) throws IOException {
    assertAnalyzesTo(a, input, output, null, null, null, posIncrements);
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[]) throws IOException {
    assertAnalyzesTo(a, input, output, startOffsets, endOffsets, null, null);
  }
  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[], int[] posIncrements) throws IOException {
    assertAnalyzesTo(a, input, output, startOffsets, endOffsets, null, posIncrements);
  }
  

  public static void assertAnalyzesToReuse(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[]) throws IOException {
    assertTokenStreamContents(a.reusableTokenStream("dummy", new StringReader(input)), output, startOffsets, endOffsets, types, posIncrements, input.length());
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
  
  // simple utility method for blasting tokenstreams with data to make sure they don't do anything crazy

  public static void checkRandomData(Random random, Analyzer a, int iterations) throws IOException {
    checkRandomData(random, a, iterations, 20);
  }

  public static void checkRandomData(Random random, Analyzer a, int iterations, int maxWordLength) throws IOException {
    for (int i = 0; i < iterations; i++) {
      String text;
      switch(_TestUtil.nextInt(random, 0, 3)) {
        case 0: 
          text = _TestUtil.randomSimpleString(random);
          break;
        case 1:
          text = _TestUtil.randomRealisticUnicodeString(random, maxWordLength);
          break;
        default:
          text = _TestUtil.randomUnicodeString(random, maxWordLength);
      }

      if (VERBOSE) {
        System.out.println("NOTE: BaseTokenStreamTestCase: get first token stream now text=" + text);
      }

      TokenStream ts = a.reusableTokenStream("dummy", new StringReader(text));
      assertTrue("has no CharTermAttribute", ts.hasAttribute(CharTermAttribute.class));
      CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
      OffsetAttribute offsetAtt = ts.hasAttribute(OffsetAttribute.class) ? ts.getAttribute(OffsetAttribute.class) : null;
      PositionIncrementAttribute posIncAtt = ts.hasAttribute(PositionIncrementAttribute.class) ? ts.getAttribute(PositionIncrementAttribute.class) : null;
      TypeAttribute typeAtt = ts.hasAttribute(TypeAttribute.class) ? ts.getAttribute(TypeAttribute.class) : null;
      List<String> tokens = new ArrayList<String>();
      List<String> types = new ArrayList<String>();
      List<Integer> positions = new ArrayList<Integer>();
      List<Integer> startOffsets = new ArrayList<Integer>();
      List<Integer> endOffsets = new ArrayList<Integer>();
      ts.reset();
      while (ts.incrementToken()) {
        tokens.add(termAtt.toString());
        if (typeAtt != null) types.add(typeAtt.type());
        if (posIncAtt != null) positions.add(posIncAtt.getPositionIncrement());
        if (offsetAtt != null) {
          startOffsets.add(offsetAtt.startOffset());
          endOffsets.add(offsetAtt.endOffset());
        }
      }
      ts.end();
      ts.close();
      // verify reusing is "reproducable" and also get the normal tokenstream sanity checks
      if (!tokens.isEmpty()) {
        if (VERBOSE) {
          System.out.println("NOTE: BaseTokenStreamTestCase: re-run analysis");
        }
        if (typeAtt != null && posIncAtt != null && offsetAtt != null) {
          // offset + pos + type
          assertAnalyzesToReuse(a, text, 
            tokens.toArray(new String[tokens.size()]),
            toIntArray(startOffsets),
            toIntArray(endOffsets),
            types.toArray(new String[types.size()]),
            toIntArray(positions));
        } else if (posIncAtt != null && offsetAtt != null) {
          // offset + pos
          assertAnalyzesToReuse(a, text, 
              tokens.toArray(new String[tokens.size()]),
              toIntArray(startOffsets),
              toIntArray(endOffsets),
              toIntArray(positions));
        } else if (offsetAtt != null) {
          // offset
          assertAnalyzesToReuse(a, text, 
              tokens.toArray(new String[tokens.size()]),
              toIntArray(startOffsets),
              toIntArray(endOffsets));
        } else {
          // terms only
          assertAnalyzesToReuse(a, text, 
              tokens.toArray(new String[tokens.size()]));
        }
      }
    }
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
