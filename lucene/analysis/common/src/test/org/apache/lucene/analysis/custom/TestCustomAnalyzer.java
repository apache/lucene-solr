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
package org.apache.lucene.analysis.custom;


import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilterFactory;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizerFactory;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilterFactory;
import org.apache.lucene.analysis.standard.ClassicTokenizerFactory;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.SetOnce.AlreadySetException;
import org.apache.lucene.util.Version;

public class TestCustomAnalyzer extends BaseTokenStreamTestCase {
  
  // Test some examples (TODO: we only check behavior, we may need something like TestRandomChains...)

  public void testWhitespaceFactoryWithFolding() throws Exception {
    CustomAnalyzer a = CustomAnalyzer.builder()
        .withTokenizer(WhitespaceTokenizerFactory.class)
        .addTokenFilter(ASCIIFoldingFilterFactory.class, "preserveOriginal", "true")
        .addTokenFilter(LowerCaseFilterFactory.class)
        .build();
    
    assertSame(WhitespaceTokenizerFactory.class, a.getTokenizerFactory().getClass());
    assertEquals(Collections.emptyList(), a.getCharFilterFactories());
    List<TokenFilterFactory> tokenFilters = a.getTokenFilterFactories();
    assertEquals(2, tokenFilters.size());
    assertSame(ASCIIFoldingFilterFactory.class, tokenFilters.get(0).getClass());
    assertSame(LowerCaseFilterFactory.class, tokenFilters.get(1).getClass());
    assertEquals(0, a.getPositionIncrementGap("dummy"));
    assertEquals(1, a.getOffsetGap("dummy"));
    assertSame(Version.LATEST, a.getVersion());

    assertAnalyzesTo(a, "foo bar FOO BAR", 
        new String[] { "foo", "bar", "foo", "bar" },
        new int[]    { 1,     1,     1,     1});
    assertAnalyzesTo(a, "föó bär FÖÖ BAR", 
        new String[] { "foo", "föó", "bar", "bär", "foo", "föö", "bar" },
        new int[]    { 1,     0,     1,     0,     1,     0,     1});
    a.close();
  }

  public void testWhitespaceWithFolding() throws Exception {
    CustomAnalyzer a = CustomAnalyzer.builder()
        .withTokenizer("whitespace")
        .addTokenFilter("asciifolding", "preserveOriginal", "true")
        .addTokenFilter("lowercase")
        .build();
    
    assertSame(WhitespaceTokenizerFactory.class, a.getTokenizerFactory().getClass());
    assertEquals(Collections.emptyList(), a.getCharFilterFactories());
    List<TokenFilterFactory> tokenFilters = a.getTokenFilterFactories();
    assertEquals(2, tokenFilters.size());
    assertSame(ASCIIFoldingFilterFactory.class, tokenFilters.get(0).getClass());
    assertSame(LowerCaseFilterFactory.class, tokenFilters.get(1).getClass());
    assertEquals(0, a.getPositionIncrementGap("dummy"));
    assertEquals(1, a.getOffsetGap("dummy"));
    assertSame(Version.LATEST, a.getVersion());

    assertAnalyzesTo(a, "foo bar FOO BAR", 
        new String[] { "foo", "bar", "foo", "bar" },
        new int[]    { 1,     1,     1,     1});
    assertAnalyzesTo(a, "föó bär FÖÖ BAR", 
        new String[] { "foo", "föó", "bar", "bär", "foo", "föö", "bar" },
        new int[]    { 1,     0,     1,     0,     1,     0,     1});
    a.close();
  }

  public void testFactoryHtmlStripClassicFolding() throws Exception {
    CustomAnalyzer a = CustomAnalyzer.builder()
        .withDefaultMatchVersion(Version.LUCENE_5_0_0)
        .addCharFilter(HTMLStripCharFilterFactory.class)
        .withTokenizer(ClassicTokenizerFactory.class)
        .addTokenFilter(ASCIIFoldingFilterFactory.class, "preserveOriginal", "true")
        .addTokenFilter(LowerCaseFilterFactory.class)
        .withPositionIncrementGap(100)
        .withOffsetGap(1000)
        .build();
    
    assertSame(ClassicTokenizerFactory.class, a.getTokenizerFactory().getClass());
    List<CharFilterFactory> charFilters = a.getCharFilterFactories();
    assertEquals(1, charFilters.size());
    assertEquals(HTMLStripCharFilterFactory.class, charFilters.get(0).getClass());
    List<TokenFilterFactory> tokenFilters = a.getTokenFilterFactories();
    assertEquals(2, tokenFilters.size());
    assertSame(ASCIIFoldingFilterFactory.class, tokenFilters.get(0).getClass());
    assertSame(LowerCaseFilterFactory.class, tokenFilters.get(1).getClass());
    assertEquals(100, a.getPositionIncrementGap("dummy"));
    assertEquals(1000, a.getOffsetGap("dummy"));
    assertSame(Version.LUCENE_5_0_0, a.getVersion());

    assertAnalyzesTo(a, "<p>foo bar</p> FOO BAR", 
        new String[] { "foo", "bar", "foo", "bar" },
        new int[]    { 1,     1,     1,     1});
    assertAnalyzesTo(a, "<p><b>föó</b> bär     FÖÖ BAR</p>", 
        new String[] { "foo", "föó", "bar", "bär", "foo", "föö", "bar" },
        new int[]    { 1,     0,     1,     0,     1,     0,     1});
    a.close();
  }
  
  public void testHtmlStripClassicFolding() throws Exception {
    CustomAnalyzer a = CustomAnalyzer.builder()
        .withDefaultMatchVersion(Version.LUCENE_5_0_0)
        .addCharFilter("htmlstrip")
        .withTokenizer("classic")
        .addTokenFilter("asciifolding", "preserveOriginal", "true")
        .addTokenFilter("lowercase")
        .withPositionIncrementGap(100)
        .withOffsetGap(1000)
        .build();
    
    assertSame(ClassicTokenizerFactory.class, a.getTokenizerFactory().getClass());
    List<CharFilterFactory> charFilters = a.getCharFilterFactories();
    assertEquals(1, charFilters.size());
    assertEquals(HTMLStripCharFilterFactory.class, charFilters.get(0).getClass());
    List<TokenFilterFactory> tokenFilters = a.getTokenFilterFactories();
    assertEquals(2, tokenFilters.size());
    assertSame(ASCIIFoldingFilterFactory.class, tokenFilters.get(0).getClass());
    assertSame(LowerCaseFilterFactory.class, tokenFilters.get(1).getClass());
    assertEquals(100, a.getPositionIncrementGap("dummy"));
    assertEquals(1000, a.getOffsetGap("dummy"));
    assertSame(Version.LUCENE_5_0_0, a.getVersion());

    assertAnalyzesTo(a, "<p>foo bar</p> FOO BAR", 
        new String[] { "foo", "bar", "foo", "bar" },
        new int[]    { 1,     1,     1,     1});
    assertAnalyzesTo(a, "<p><b>föó</b> bär     FÖÖ BAR</p>", 
        new String[] { "foo", "föó", "bar", "bär", "foo", "föö", "bar" },
        new int[]    { 1,     0,     1,     0,     1,     0,     1});
    a.close();
  }
  
  public void testStopWordsFromClasspath() throws Exception {
    CustomAnalyzer a = CustomAnalyzer.builder()
        .withTokenizer(WhitespaceTokenizerFactory.class)
        .addTokenFilter("stop",
            "ignoreCase", "true",
            "words", "org/apache/lucene/analysis/custom/teststop.txt",
            "format", "wordset")
        .build();
    
    assertSame(WhitespaceTokenizerFactory.class, a.getTokenizerFactory().getClass());
    assertEquals(Collections.emptyList(), a.getCharFilterFactories());
    List<TokenFilterFactory> tokenFilters = a.getTokenFilterFactories();
    assertEquals(1, tokenFilters.size());
    assertSame(StopFilterFactory.class, tokenFilters.get(0).getClass());
    assertEquals(0, a.getPositionIncrementGap("dummy"));
    assertEquals(1, a.getOffsetGap("dummy"));
    assertSame(Version.LATEST, a.getVersion());

    assertAnalyzesTo(a, "foo Foo Bar", new String[0]);
    a.close();
  }
  
  public void testStopWordsFromClasspathWithMap() throws Exception {
    Map<String,String> stopConfig1 = new HashMap<>();
    stopConfig1.put("ignoreCase", "true");
    stopConfig1.put("words", "org/apache/lucene/analysis/custom/teststop.txt");
    stopConfig1.put("format", "wordset");
    
    Map<String,String> stopConfig2 = new HashMap<>(stopConfig1);
    Map<String,String> stopConfigImmutable = Collections.unmodifiableMap(new HashMap<>(stopConfig1));

    CustomAnalyzer a = CustomAnalyzer.builder()
        .withTokenizer("whitespace")
        .addTokenFilter("stop", stopConfig1)
        .build();
    assertTrue(stopConfig1.isEmpty());
    assertAnalyzesTo(a, "foo Foo Bar", new String[0]);
    
    a = CustomAnalyzer.builder()
        .withTokenizer(WhitespaceTokenizerFactory.class)
        .addTokenFilter(StopFilterFactory.class, stopConfig2)
        .build();
    assertTrue(stopConfig2.isEmpty());
    assertAnalyzesTo(a, "foo Foo Bar", new String[0]);
    
    // try with unmodifiableMap, should fail
    try {
      CustomAnalyzer.builder()
          .withTokenizer("whitespace")
          .addTokenFilter("stop", stopConfigImmutable)
          .build();
      fail();
    } catch (UnsupportedOperationException e) {
      // pass
    }
    a.close();
  }
  
  public void testStopWordsFromFile() throws Exception {
    CustomAnalyzer a = CustomAnalyzer.builder(this.getDataPath(""))
        .withTokenizer("whitespace")
        .addTokenFilter("stop",
            "ignoreCase", "true",
            "words", "teststop.txt",
            "format", "wordset")
        .build();
    assertAnalyzesTo(a, "foo Foo Bar", new String[0]);
    a.close();
  }
  
  public void testStopWordsFromFileAbsolute() throws Exception {
    CustomAnalyzer a = CustomAnalyzer.builder(Paths.get("."))
        .withTokenizer("whitespace")
        .addTokenFilter("stop",
            "ignoreCase", "true",
            "words", this.getDataPath("teststop.txt").toString(),
            "format", "wordset")
        .build();
    assertAnalyzesTo(a, "foo Foo Bar", new String[0]);
    a.close();
  }
  
  // Now test misconfigurations:

  public void testIncorrectOrder() throws Exception {
    try {
      CustomAnalyzer.builder()
          .addCharFilter("htmlstrip")
          .withDefaultMatchVersion(Version.LATEST)
          .withTokenizer("whitespace")
          .build();
      fail();
    } catch (IllegalStateException e) {
      // pass
    }
  }

  public void testMissingSPI() throws Exception {
    try {
      CustomAnalyzer.builder()
          .withTokenizer("foobar_nonexistent")
          .build();
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("SPI"));
      assertTrue(e.getMessage().contains("does not exist"));
    }
  }

  public void testSetTokenizerTwice() throws Exception {
    try {
      CustomAnalyzer.builder()
          .withTokenizer("whitespace")
          .withTokenizer(StandardTokenizerFactory.class)
          .build();
      fail();
    } catch (AlreadySetException e) {
      // pass
    }
  }

  public void testSetMatchVersionTwice() throws Exception {
    try {
      CustomAnalyzer.builder()
          .withDefaultMatchVersion(Version.LATEST)
          .withDefaultMatchVersion(Version.LATEST)
          .withTokenizer("standard")
          .build();
      fail();
    } catch (AlreadySetException e) {
      // pass
    }
  }

  public void testSetPosIncTwice() throws Exception {
    try {
      CustomAnalyzer.builder()
          .withPositionIncrementGap(2)
          .withPositionIncrementGap(3)
          .withTokenizer("standard")
          .build();
      fail();
    } catch (AlreadySetException e) {
      // pass
    }
  }

  public void testSetOfsGapTwice() throws Exception {
    try {
      CustomAnalyzer.builder()
          .withOffsetGap(2)
          .withOffsetGap(3)
          .withTokenizer("standard")
          .build();
      fail();
    } catch (AlreadySetException e) {
      // pass
    }
  }

  public void testNoTokenizer() throws Exception {
    try {
      CustomAnalyzer.builder().build();
      fail();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().equals("You have to set at least a tokenizer."));
    }
  }

  public void testNullTokenizer() throws Exception {
    try {
      CustomAnalyzer.builder()
        .withTokenizer((String) null)
        .build();
      fail();
    } catch (NullPointerException e) {
      // pass
    }
  }

  public void testNullTokenizerFactory() throws Exception {
    try {
      CustomAnalyzer.builder()
        .withTokenizer((Class<TokenizerFactory>) null)
        .build();
      fail();
    } catch (NullPointerException e) {
      // pass
    }
  }

  public void testNullParamKey() throws Exception {
    try {
      CustomAnalyzer.builder()
        .withTokenizer("whitespace", null, "foo")
        .build();
      fail();
    } catch (NullPointerException e) {
      // pass
    }
  }

  public void testNullMatchVersion() throws Exception {
    try {
      CustomAnalyzer.builder()
        .withDefaultMatchVersion(null)
        .withTokenizer("whitespace")
        .build();
      fail();
    } catch (NullPointerException e) {
      // pass
    }
  }

}
