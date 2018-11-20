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


import java.io.IOException;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilterFactory;
import org.apache.lucene.analysis.charfilter.MappingCharFilterFactory;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizerFactory;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilterFactory;
import org.apache.lucene.analysis.reverse.ReverseStringFilterFactory;
import org.apache.lucene.analysis.standard.ClassicTokenizerFactory;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.BytesRef;
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
        .withDefaultMatchVersion(Version.LUCENE_7_0_0)
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
    assertSame(Version.LUCENE_7_0_0, a.getVersion());

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
        .withDefaultMatchVersion(Version.LUCENE_7_0_0)
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
    assertSame(Version.LUCENE_7_0_0, a.getVersion());

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
    expectThrows(UnsupportedOperationException.class, () -> {
      CustomAnalyzer.builder()
          .withTokenizer("whitespace")
          .addTokenFilter("stop", stopConfigImmutable)
          .build();
    });
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
    expectThrows(IllegalStateException.class, () -> {
      CustomAnalyzer.builder()
          .addCharFilter("htmlstrip")
          .withDefaultMatchVersion(Version.LATEST)
          .withTokenizer("whitespace")
          .build();
    });
  }

  public void testMissingSPI() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      CustomAnalyzer.builder()
          .withTokenizer("foobar_nonexistent")
          .build();
    });
    assertTrue(expected.getMessage().contains("SPI"));
    assertTrue(expected.getMessage().contains("does not exist"));
  }

  public void testSetTokenizerTwice() throws Exception {
    expectThrows(AlreadySetException.class, () -> {
      CustomAnalyzer.builder()
          .withTokenizer("whitespace")
          .withTokenizer(StandardTokenizerFactory.class)
          .build();
    });
  }

  public void testSetMatchVersionTwice() throws Exception {
    expectThrows(AlreadySetException.class, () -> {
      CustomAnalyzer.builder()
          .withDefaultMatchVersion(Version.LATEST)
          .withDefaultMatchVersion(Version.LATEST)
          .withTokenizer("standard")
          .build();
    });
  }

  public void testSetPosIncTwice() throws Exception {
    expectThrows(AlreadySetException.class, () -> {
      CustomAnalyzer.builder()
          .withPositionIncrementGap(2)
          .withPositionIncrementGap(3)
          .withTokenizer("standard")
          .build();
    });
  }

  public void testSetOfsGapTwice() throws Exception {
    expectThrows(AlreadySetException.class, () -> {
      CustomAnalyzer.builder()
          .withOffsetGap(2)
          .withOffsetGap(3)
          .withTokenizer("standard")
          .build();
    });
  }

  public void testNoTokenizer() throws Exception {
    expectThrows(IllegalStateException.class, () -> {
      CustomAnalyzer.builder().build();
    });
  }

  public void testNullTokenizer() throws Exception {
    expectThrows(NullPointerException.class, () -> {
      CustomAnalyzer.builder()
        .withTokenizer((String) null)
        .build();
    });
  }

  public void testNullTokenizerFactory() throws Exception {
    expectThrows(NullPointerException.class, () -> {
      CustomAnalyzer.builder()
        .withTokenizer((Class<TokenizerFactory>) null)
        .build();
    });
  }

  public void testNullParamKey() throws Exception {
    expectThrows(NullPointerException.class, () -> {
      CustomAnalyzer.builder()
        .withTokenizer("whitespace", null, "foo")
        .build();
    });
  }

  public void testNullMatchVersion() throws Exception {
    expectThrows(NullPointerException.class, () -> {
      CustomAnalyzer.builder()
        .withDefaultMatchVersion(null)
        .withTokenizer("whitespace")
        .build();
    });
  }

  private static class DummyCharFilter extends CharFilter {

    private final char match, repl;

    public DummyCharFilter(Reader input, char match, char repl) {
      super(input);
      this.match = match;
      this.repl = repl;
    }

    @Override
    protected int correct(int currentOff) {
      return currentOff;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
      final int read = input.read(cbuf, off, len);
      for (int i = 0; i < read; ++i) {
        if (cbuf[off+i] == match) {
          cbuf[off+i] = repl;
        }
      }
      return read;
    }
    
  }

  public static class DummyCharFilterFactory extends CharFilterFactory {

    private final char match, repl;

    public DummyCharFilterFactory(Map<String,String> args) {
      this(args, '0', '1');
    }

    DummyCharFilterFactory(Map<String,String> args, char match, char repl) {
      super(args);
      this.match = match;
      this.repl = repl;
    }

    @Override
    public Reader create(Reader input) {
      return new DummyCharFilter(input, match, repl);
    }
    
  }

  public static class DummyMultiTermAwareCharFilterFactory extends DummyCharFilterFactory {

    public DummyMultiTermAwareCharFilterFactory(Map<String,String> args) {
      super(args);
    }

    @Override
    public Reader normalize(Reader input) {
      return create(input);
    }
  }

  public static class DummyTokenizerFactory extends TokenizerFactory {

    public DummyTokenizerFactory(Map<String,String> args) {
      super(args);
    }

    @Override
    public Tokenizer create(AttributeFactory factory) {
      return new LetterTokenizer(factory);
    }

  }

  public static class DummyTokenFilterFactory extends TokenFilterFactory {

    public DummyTokenFilterFactory(Map<String,String> args) {
      super(args);
    }

    @Override
    public TokenStream create(TokenStream input) {
      return input;
    }
    
  }

  public static class DummyMultiTermAwareTokenFilterFactory extends DummyTokenFilterFactory {

    public DummyMultiTermAwareTokenFilterFactory(Map<String,String> args) {
      super(args);
    }

    @Override
    public TokenStream normalize(TokenStream input) {
      return new ASCIIFoldingFilterFactory(Collections.emptyMap()).normalize(input);
    }
    
  }

  public void testNormalization() throws IOException {
    CustomAnalyzer analyzer1 = CustomAnalyzer.builder()
        // none of these components are multi-term aware so they should not be applied
        .withTokenizer(DummyTokenizerFactory.class, Collections.emptyMap())
        .addCharFilter(DummyCharFilterFactory.class, Collections.emptyMap())
        .addTokenFilter(DummyTokenFilterFactory.class, Collections.emptyMap())
        .build();
    assertEquals(new BytesRef("0À"), analyzer1.normalize("dummy", "0À"));

    CustomAnalyzer analyzer2 = CustomAnalyzer.builder()
        // this component in not multi-term aware so it should not be applied
        .withTokenizer(DummyTokenizerFactory.class, Collections.emptyMap())
        // these components are multi-term aware so they should be applied
        .addCharFilter(DummyMultiTermAwareCharFilterFactory.class, Collections.emptyMap())
        .addTokenFilter(DummyMultiTermAwareTokenFilterFactory.class, Collections.emptyMap())
        .build();
    assertEquals(new BytesRef("1A"), analyzer2.normalize("dummy", "0À"));
  }

  public void testNormalizationWithMultipleTokenFilters() throws IOException {
    CustomAnalyzer analyzer = CustomAnalyzer.builder()
        // none of these components are multi-term aware so they should not be applied
        .withTokenizer(WhitespaceTokenizerFactory.class, Collections.emptyMap())
        .addTokenFilter(LowerCaseFilterFactory.class, Collections.emptyMap())
        .addTokenFilter(ASCIIFoldingFilterFactory.class, Collections.emptyMap())
        .build();
    assertEquals(new BytesRef("a b e"), analyzer.normalize("dummy", "À B é"));
  }

  public void testNormalizationWithMultiplCharFilters() throws IOException {
    CustomAnalyzer analyzer = CustomAnalyzer.builder()
        // none of these components are multi-term aware so they should not be applied
        .withTokenizer(WhitespaceTokenizerFactory.class, Collections.emptyMap())
        .addCharFilter(MappingCharFilterFactory.class, new HashMap<>(Collections.singletonMap("mapping", "org/apache/lucene/analysis/custom/mapping1.txt")))
        .addCharFilter(MappingCharFilterFactory.class, new HashMap<>(Collections.singletonMap("mapping", "org/apache/lucene/analysis/custom/mapping2.txt")))
        .build();
    assertEquals(new BytesRef("e f c"), analyzer.normalize("dummy", "a b c"));
  }

  public void testConditions() throws IOException {
    CustomAnalyzer analyzer = CustomAnalyzer.builder()
        .withTokenizer("whitespace")
        .addTokenFilter("lowercase")
        .whenTerm(t -> t.toString().contains("o"))
          .addTokenFilter("uppercase")
          .addTokenFilter(ReverseStringFilterFactory.class)
        .endwhen()
        .addTokenFilter("asciifolding")
        .build();

    assertAnalyzesTo(analyzer, "Héllo world whaT's hãppening",
        new String[]{ "OLLEH", "DLROW", "what's", "happening" });
  }

  public void testConditionsWithResourceLoader() throws IOException {
    CustomAnalyzer analyzer = CustomAnalyzer.builder()
        .withTokenizer("whitespace")
        .addTokenFilter("lowercase")
        .when("protectedterm", "protected", "org/apache/lucene/analysis/custom/teststop.txt")
          .addTokenFilter("reversestring")
        .endwhen()
        .build();

    assertAnalyzesTo(analyzer, "FOO BAR BAZ",
        new String[]{ "foo", "bar", "zab" });
  }

  public void testConditionsWithWrappedResourceLoader() throws IOException {
    CustomAnalyzer analyzer = CustomAnalyzer.builder()
        .withTokenizer("whitespace")
        .addTokenFilter("lowercase")
        .whenTerm(t -> t.toString().contains("o") == false)
          .addTokenFilter("stop",
            "ignoreCase", "true",
            "words", "org/apache/lucene/analysis/custom/teststop.txt",
            "format", "wordset")
        .endwhen()
        .build();

    assertAnalyzesTo(analyzer, "foo bar baz", new String[]{ "foo", "baz" });
  }

}
