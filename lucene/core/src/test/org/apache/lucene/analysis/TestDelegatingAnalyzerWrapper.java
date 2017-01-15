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
import java.nio.charset.StandardCharsets;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestDelegatingAnalyzerWrapper extends LuceneTestCase {

  public void testDelegatesNormalization() {
    Analyzer analyzer1 = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    DelegatingAnalyzerWrapper w1 = new DelegatingAnalyzerWrapper(Analyzer.GLOBAL_REUSE_STRATEGY) {
      @Override
      protected Analyzer getWrappedAnalyzer(String fieldName) {
        return analyzer1;
      }
    };
    assertEquals(new BytesRef("Ab C"), w1.normalize("foo", "Ab C"));

    Analyzer analyzer2 = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true);
    DelegatingAnalyzerWrapper w2 = new DelegatingAnalyzerWrapper(Analyzer.GLOBAL_REUSE_STRATEGY) {
      @Override
      protected Analyzer getWrappedAnalyzer(String fieldName) {
        return analyzer2;
      }
    };
    assertEquals(new BytesRef("ab c"), w2.normalize("foo", "Ab C"));
  }

  public void testDelegatesAttributeFactory() throws Exception {
    Analyzer analyzer1 = new MockBytesAnalyzer();
    DelegatingAnalyzerWrapper w1 = new DelegatingAnalyzerWrapper(Analyzer.GLOBAL_REUSE_STRATEGY) {
      @Override
      protected Analyzer getWrappedAnalyzer(String fieldName) {
        return analyzer1;
      }
    };
    assertEquals(new BytesRef("Ab C".getBytes(StandardCharsets.UTF_16LE)), w1.normalize("foo", "Ab C"));
  }

  public void testDelegatesCharFilter() throws Exception {
    Analyzer analyzer1 = new Analyzer() {
      @Override
      protected Reader initReaderForNormalization(String fieldName, Reader reader) {
        return new DummyCharFilter(reader, 'b', 'z');
      }
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(attributeFactory(fieldName));
        return new TokenStreamComponents(tokenizer);
      }
    };
    DelegatingAnalyzerWrapper w1 = new DelegatingAnalyzerWrapper(Analyzer.GLOBAL_REUSE_STRATEGY) {
      @Override
      protected Analyzer getWrappedAnalyzer(String fieldName) {
        return analyzer1;
      }
    };
    assertEquals(new BytesRef("az c"), w1.normalize("foo", "ab c"));
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
}
