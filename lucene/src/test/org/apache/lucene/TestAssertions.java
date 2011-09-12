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

import org.apache.lucene.analysis.ReusableAnalyzerBase;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;

public class TestAssertions extends LuceneTestCase {

  public void testBasics() {
    try {
      assert Boolean.FALSE.booleanValue();
      fail("assertions are not enabled!");
    } catch (AssertionError e) {
      assert Boolean.TRUE.booleanValue();
    }
  }
  
  static class TestAnalyzer1 extends ReusableAnalyzerBase {

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader aReader) {
      return null;
    }
  }

  static final class TestAnalyzer2 extends ReusableAnalyzerBase {

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader aReader) {
      return null;
    }
  }

  static class TestAnalyzer3 extends ReusableAnalyzerBase {

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader aReader) {
      return null;
    }
  }

  static class TestAnalyzer4 extends ReusableAnalyzerBase {

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader aReader) {
      return null;
    }
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
    
    try {
      new TestAnalyzer3();
      fail("TestAnalyzer3 should fail assertion");
    } catch (AssertionError e) {
    }
    
    try {
      new TestAnalyzer4();
      fail("TestAnalyzer4 should fail assertion");
    } catch (AssertionError e) {
    }
    
    new TestTokenStream1();
    
    new TestTokenStream2();
    
    try {
      new TestTokenStream3();
      fail("TestTokenStream3 should fail assertion");
    } catch (AssertionError e) {
    }
  }

}
