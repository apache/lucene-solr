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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;

public class TestWhitespaceAnalyzer extends BaseTokenStreamTestCase {

  private static final String LONGTOKEN =
            "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
          + "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
          + "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";

  public void testDefaultMaximumTokenLength() throws IOException {
    try (Analyzer a = new WhitespaceAnalyzer()) {
      assertAnalyzesTo(a, LONGTOKEN + " extra", new String[]{
          "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
          + "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
          + "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstu",
          "vwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz", "extra"
      });
    }
  }

  public void testCustomMaximumTokenLength() throws IOException {
    try (Analyzer a = new WhitespaceAnalyzer(1024)) {
      assertAnalyzesTo(a, LONGTOKEN + " extra", new String[] { LONGTOKEN, "extra" });
    }
  }

}
