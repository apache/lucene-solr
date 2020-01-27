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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.util.LuceneTestCase;

public class TestAnalyzerWrapper extends LuceneTestCase {

  public void testSourceDelegation() throws IOException {

    AtomicBoolean sourceCalled = new AtomicBoolean(false);

    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(r -> {
          sourceCalled.set(true);
        }, new CannedTokenStream());
      }
    };

    Analyzer wrapped = new AnalyzerWrapper(analyzer.getReuseStrategy()) {
      @Override
      protected Analyzer getWrappedAnalyzer(String fieldName) {
        return analyzer;
      }

      @Override
      protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
        return new TokenStreamComponents(components.getSource(), new LowerCaseFilter(components.getTokenStream()));
      }
    };

    try (TokenStream ts = wrapped.tokenStream("", "text")) {
      assert ts != null;
      assertTrue(sourceCalled.get());
    }

  }

}
