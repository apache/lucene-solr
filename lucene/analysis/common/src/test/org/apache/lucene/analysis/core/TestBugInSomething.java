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
package org.apache.lucene.analysis.core;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.MockCharFilter;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;


@SuppressCodecs("Direct")
public class TestBugInSomething extends BaseTokenStreamTestCase {
  public void test() throws Exception {
    final CharArraySet cas = new CharArraySet(3, false);
    cas.add("jjp");
    cas.add("wlmwoknt");
    cas.add("tcgyreo");
    
    final NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
    builder.add("mtqlpi", "");
    builder.add("mwoknt", "jjp");
    builder.add("tcgyreo", "zpfpajyws");
    final NormalizeCharMap map = builder.build();
    
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer t = new MockTokenizer(MockTokenFilter.ENGLISH_STOPSET, false, -65);
        TokenFilter f = new CommonGramsFilter(t, cas);
        return new TokenStreamComponents(t, f);
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        reader = new MockCharFilter(reader, 0);
        reader = new MappingCharFilter(map, reader);
        reader = new TestRandomChains.CheckThatYouDidntReadAnythingReaderWrapper(reader);
        return reader;
      }
    };
    checkAnalysisConsistency(random(), a, false, "wmgddzunizdomqyj");
    a.close();
  }
  
  CharFilter wrappedStream = new CharFilter(new StringReader("bogus")) {

    @Override
    public void mark(int readAheadLimit) {
      throw new UnsupportedOperationException("mark(int)");
    }

    @Override
    public boolean markSupported() {
      throw new UnsupportedOperationException("markSupported()");
    }

    @Override
    public int read() {
      throw new UnsupportedOperationException("read()");
    }

    @Override
    public int read(char[] cbuf) {
      throw new UnsupportedOperationException("read(char[])");
    }

    @Override
    public int read(CharBuffer target) {
      throw new UnsupportedOperationException("read(CharBuffer)");
    }

    @Override
    public boolean ready() {
      throw new UnsupportedOperationException("ready()");
    }

    @Override
    public void reset() {
      throw new UnsupportedOperationException("reset()");
    }

    @Override
    public long skip(long n) {
      throw new UnsupportedOperationException("skip(long)");
    }

    @Override
    public int correct(int currentOff) {
      throw new UnsupportedOperationException("correct(int)");
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException("close()");
    }

    @Override
    public int read(char[] arg0, int arg1, int arg2) {
      throw new UnsupportedOperationException("read(char[], int, int)");
    }
  };
  
  public void testWrapping() throws Exception {
    CharFilter cs = new TestRandomChains.CheckThatYouDidntReadAnythingReaderWrapper(wrappedStream);
    Exception expected = expectThrows(Exception.class, () -> {
      cs.mark(1);
    });
    assertEquals("mark(int)", expected.getMessage());
    
    expected = expectThrows(Exception.class, () -> {
      cs.markSupported();
    });
    assertEquals("markSupported()", expected.getMessage());
    
    expected = expectThrows(Exception.class, () -> {
      cs.read();
    });
    assertEquals("read()", expected.getMessage());
    
    expected = expectThrows(Exception.class, () -> {
      cs.read(new char[0]);
    });
    assertEquals("read(char[])", expected.getMessage());
    
    expected = expectThrows(Exception.class, () -> {
      cs.read(CharBuffer.wrap(new char[0]));
    });
    assertEquals("read(CharBuffer)", expected.getMessage());
    
    expected = expectThrows(Exception.class, () -> {
      cs.reset();
    });
    assertEquals("reset()", expected.getMessage());
    
    expected = expectThrows(Exception.class, () -> {
      cs.skip(1);
    });
    assertEquals("skip(long)", expected.getMessage());
    
    expected = expectThrows(Exception.class, () -> {
      cs.correctOffset(1);
    });
    assertEquals("correct(int)", expected.getMessage());
    
    expected = expectThrows(Exception.class, () -> {
      cs.close();
    });
    assertEquals("close()", expected.getMessage());
    
    expected = expectThrows(Exception.class, () -> {
      cs.read(new char[0], 0, 0);
    });
    assertEquals("read(char[], int, int)", expected.getMessage());
  }
  
  // todo: test framework?
  
  static final class SopTokenFilter extends TokenFilter {

    SopTokenFilter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        if (VERBOSE) System.out.println(input.getClass().getSimpleName() + "->" + this.reflectAsString(false));
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void end() throws IOException {
      super.end();
      if (VERBOSE) System.out.println(input.getClass().getSimpleName() + ".end()");
    }

    @Override
    public void close() throws IOException {
      super.close();
      if (VERBOSE) System.out.println(input.getClass().getSimpleName() + ".close()");
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      if (VERBOSE) System.out.println(input.getClass().getSimpleName() + ".reset()");
    }
  }
  
  // LUCENE-5269
  @Nightly
  public void testUnicodeShinglesAndNgrams() throws Exception {
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new EdgeNGramTokenizer(2, 94);
        //TokenStream stream = new SopTokenFilter(tokenizer);
        TokenStream stream = new ShingleFilter(tokenizer, 5);
        //stream = new SopTokenFilter(stream);
        stream = new NGramTokenFilter(stream, 55, 83, false);
        //stream = new SopTokenFilter(stream);
        return new TokenStreamComponents(tokenizer, stream);
      }  
    };
    checkRandomData(random(), analyzer, 2000);
    analyzer.close();
  }
  
  public void testCuriousWikipediaString() throws Exception {
    final CharArraySet protWords = new CharArraySet(new HashSet<>(
        Arrays.asList("rrdpafa", "pupmmlu", "xlq", "dyy", "zqrxrrck", "o", "hsrlfvcha")), false);
    final byte table[] = new byte[] { 
        -57, 26, 1, 48, 63, -23, 55, -84, 18, 120, -97, 103, 58, 13, 84, 89, 57, -13, -63, 
        5, 28, 97, -54, -94, 102, -108, -5, 5, 46, 40, 43, 78, 43, -72, 36, 29, 124, -106, 
        -22, -51, 65, 5, 31, -42, 6, -99, 97, 14, 81, -128, 74, 100, 54, -55, -25, 53, -71, 
        -98, 44, 33, 86, 106, -42, 47, 115, -89, -18, -26, 22, -95, -43, 83, -125, 105, -104,
        -24, 106, -16, 126, 115, -105, 97, 65, -33, 57, 44, -1, 123, -68, 100, 13, -41, -64, 
        -119, 0, 92, 94, -36, 53, -9, -102, -18, 90, 94, -26, 31, 71, -20
    };
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new WikipediaTokenizer();
        TokenStream stream = new SopTokenFilter(tokenizer);
        stream = new WordDelimiterFilter(stream, table, -50, protWords);
        stream = new SopTokenFilter(stream);
        return new TokenStreamComponents(tokenizer, stream);
      }  
    };
    checkAnalysisConsistency(random(), a, false, "B\u28c3\ue0f8[ \ud800\udfc2 </p> jb");
    a.close();
  }
}
