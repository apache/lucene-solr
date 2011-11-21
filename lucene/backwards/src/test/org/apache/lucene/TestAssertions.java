package org.apache.lucene;

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

import java.io.Reader;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;

public class TestAssertions extends LuceneTestCase {

  static class TestAnalyzer1 extends Analyzer {
    @Override
    public final TokenStream tokenStream(String s, Reader r) { return null; }
    @Override
    public final TokenStream reusableTokenStream(String s, Reader r) { return null; }
  }

  static final class TestAnalyzer2 extends Analyzer {
    @Override
    public TokenStream tokenStream(String s, Reader r) { return null; }
    @Override
    public TokenStream reusableTokenStream(String s, Reader r) { return null; }
  }

  static class TestAnalyzer3 extends Analyzer {
    @Override
    public TokenStream tokenStream(String s, Reader r) { return null; }
    @Override
    public TokenStream reusableTokenStream(String s, Reader r) { return null; }
  }

  static class TestAnalyzer4 extends Analyzer {
    @Override
    public final TokenStream tokenStream(String s, Reader r) { return null; }
    @Override
    public TokenStream reusableTokenStream(String s, Reader r) { return null; }
  }

  static class TestTokenStream1 extends TokenStream {
    @Override
    public final boolean incrementToken() { return false; }
  }

  static final class TestTokenStream2 extends TokenStream {
    @Override
    public boolean incrementToken() { return false; }
  }

  static class TestTokenStream3 extends TokenStream {
    @Override
    public boolean incrementToken() { return false; }
  }

  public void testTokenStreams() {
    new TestAnalyzer1();
    
    new TestAnalyzer2();
    
    boolean doFail = false;
    try {
      new TestAnalyzer3();
      doFail = true;
    } catch (AssertionError e) {
      // expected
    }
    assertFalse("TestAnalyzer3 should fail assertion", doFail);
    
    try {
      new TestAnalyzer4();
      doFail = true;
    } catch (AssertionError e) {
      // expected
    }
    assertFalse("TestAnalyzer4 should fail assertion", doFail);
    
    new TestTokenStream1();
    
    new TestTokenStream2();
    
    try {
      new TestTokenStream3();
      doFail = true;
    } catch (AssertionError e) {
      // expected
    }
    assertFalse("TestTokenStream3 should fail assertion", doFail);
  }

}
