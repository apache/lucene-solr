package org.apache.lucene.analysis.core;

import java.io.Reader;
import java.io.StringReader;
import java.nio.CharBuffer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.MockCharFilter;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.util.CharArraySet;

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

public class TestBugInSomething extends BaseTokenStreamTestCase {
  public void test() throws Exception {
    final CharArraySet cas = new CharArraySet(TEST_VERSION_CURRENT, 3, false);
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
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer t = new MockTokenizer(new TestRandomChains.CheckThatYouDidntReadAnythingReaderWrapper(reader), MockTokenFilter.ENGLISH_STOPSET, false, -65);
        TokenFilter f = new CommonGramsFilter(TEST_VERSION_CURRENT, t, cas);
        return new TokenStreamComponents(t, f);
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        reader = new MockCharFilter(reader, 0);
        reader = new MappingCharFilter(map, reader);
        return reader;
      }
    };
    checkAnalysisConsistency(random(), a, false, "wmgddzunizdomqyj");
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
    try {
      cs.mark(1);
      fail();
    } catch (Exception e) {
      assertEquals("mark(int)", e.getMessage());
    }
    
    try {
      cs.markSupported();
      fail();
    } catch (Exception e) {
      assertEquals("markSupported()", e.getMessage());
    }
    
    try {
      cs.read();
      fail();
    } catch (Exception e) {
      assertEquals("read()", e.getMessage());
    }
    
    try {
      cs.read(new char[0]);
      fail();
    } catch (Exception e) {
      assertEquals("read(char[])", e.getMessage());
    }
    
    try {
      cs.read(CharBuffer.wrap(new char[0]));
      fail();
    } catch (Exception e) {
      assertEquals("read(CharBuffer)", e.getMessage());
    }
    
    try {
      cs.reset();
      fail();
    } catch (Exception e) {
      assertEquals("reset()", e.getMessage());
    }
    
    try {
      cs.skip(1);
      fail();
    } catch (Exception e) {
      assertEquals("skip(long)", e.getMessage());
    }
    
    try {
      cs.correctOffset(1);
      fail();
    } catch (Exception e) {
      assertEquals("correct(int)", e.getMessage());
    }
    
    try {
      cs.close();
      fail();
    } catch (Exception e) {
      assertEquals("close()", e.getMessage());
    }
    
    try {
      cs.read(new char[0], 0, 0);
      fail();
    } catch (Exception e) {
      assertEquals("read(char[], int, int)", e.getMessage());
    }
  }
}
