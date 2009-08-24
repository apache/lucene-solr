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

import java.util.Set;
import java.io.StringReader;
import java.io.IOException;
 
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util.LuceneTestCase;

/** 
 * Base class for all Lucene unit tests that use TokenStreams.  
 * <p>
 * This class runs all tests twice, one time with {@link TokenStream#setOnlyUseNewAPI} <code>false</code>
 * and after that one time with <code>true</code>.
 */
public abstract class BaseTokenStreamTestCase extends LuceneTestCase {

  private boolean onlyUseNewAPI = false;
  private final Set testWithNewAPI;
  
  public BaseTokenStreamTestCase() {
    super();
    this.testWithNewAPI = null; // run all tests also with onlyUseNewAPI
  }

  public BaseTokenStreamTestCase(String name) {
    super(name);
    this.testWithNewAPI = null; // run all tests also with onlyUseNewAPI
  }

  public BaseTokenStreamTestCase(Set testWithNewAPI) {
    super();
    this.testWithNewAPI = testWithNewAPI;
  }

  public BaseTokenStreamTestCase(String name, Set testWithNewAPI) {
    super(name);
    this.testWithNewAPI = testWithNewAPI;
  }

  // @Override
  protected void setUp() throws Exception {
    super.setUp();
    TokenStream.setOnlyUseNewAPI(onlyUseNewAPI);
  }

  // @Override
  public void runBare() throws Throwable {
    // Do the test with onlyUseNewAPI=false (default)
    try {
      onlyUseNewAPI = false;
      super.runBare();
    } catch (Throwable e) {
      System.out.println("Test failure of '"+getName()+"' occurred with onlyUseNewAPI=false");
      throw e;
    }

    if (testWithNewAPI == null || testWithNewAPI.contains(getName())) {
      // Do the test again with onlyUseNewAPI=true
      try {
        onlyUseNewAPI = true;
        super.runBare();
      } catch (Throwable e) {
        System.out.println("Test failure of '"+getName()+"' occurred with onlyUseNewAPI=true");
        throw e;
      }
    }
  }
  
  // some helpers to test Analyzers and TokenStreams:
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[]) throws IOException {
    assertNotNull(output);
    assertTrue("has TermAttribute", ts.hasAttribute(TermAttribute.class));
    TermAttribute termAtt = (TermAttribute) ts.getAttribute(TermAttribute.class);
    
    OffsetAttribute offsetAtt = null;
    if (startOffsets != null || endOffsets != null) {
      assertTrue("has OffsetAttribute", ts.hasAttribute(OffsetAttribute.class));
      offsetAtt = (OffsetAttribute) ts.getAttribute(OffsetAttribute.class);
    }
    
    TypeAttribute typeAtt = null;
    if (types != null) {
      assertTrue("has TypeAttribute", ts.hasAttribute(TypeAttribute.class));
      typeAtt = (TypeAttribute) ts.getAttribute(TypeAttribute.class);
    }
    
    PositionIncrementAttribute posIncrAtt = null;
    if (posIncrements != null) {
      assertTrue("has PositionIncrementAttribute", ts.hasAttribute(PositionIncrementAttribute.class));
      posIncrAtt = (PositionIncrementAttribute) ts.getAttribute(PositionIncrementAttribute.class);
    }
    
    ts.reset();
    for (int i = 0; i < output.length; i++) {
      assertTrue("token "+i+" exists", ts.incrementToken());
      assertEquals("term "+i, output[i], termAtt.term());
      if (startOffsets != null)
        assertEquals("startOffset "+i, startOffsets[i], offsetAtt.startOffset());
      if (endOffsets != null)
        assertEquals("endOffset "+i, endOffsets[i], offsetAtt.endOffset());
      if (types != null)
        assertEquals("type "+i, types[i], typeAtt.type());
      if (posIncrements != null)
        assertEquals("posIncrement "+i, posIncrements[i], posIncrAtt.getPositionIncrement());
    }
    assertFalse("end of stream", ts.incrementToken());
    ts.close();
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output) throws IOException {
    assertTokenStreamContents(ts, output, null, null, null, null);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, String[] types) throws IOException {
    assertTokenStreamContents(ts, output, null, null, types, null);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int[] posIncrements) throws IOException {
    assertTokenStreamContents(ts, output, null, null, null, posIncrements);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[]) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, null, null);
  }
  
  public static void assertTokenStreamContents(TokenStream ts, String[] output, int startOffsets[], int endOffsets[], int[] posIncrements) throws IOException {
    assertTokenStreamContents(ts, output, startOffsets, endOffsets, null, posIncrements);
  }

  
  public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[]) throws IOException {
    assertTokenStreamContents(a.tokenStream("dummy", new StringReader(input)), output, startOffsets, endOffsets, types, posIncrements);
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
    assertTokenStreamContents(a.reusableTokenStream("dummy", new StringReader(input)), output, startOffsets, endOffsets, types, posIncrements);
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
  
}
