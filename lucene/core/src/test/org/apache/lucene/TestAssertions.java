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
package org.apache.lucene;


import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.LuceneTestCase;

/**
 * validate that assertions are enabled during tests
 */
public class TestAssertions extends LuceneTestCase {

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
    new TestTokenStream1();
    new TestTokenStream2();
    try {
      new TestTokenStream3();
      if (assertsAreEnabled) {
        fail("TestTokenStream3 should fail assertion");
      }
    } catch (AssertionError e) {
      // expected
    }
  }
}
