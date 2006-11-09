package org.apache.lucene.analysis;

import junit.framework.TestCase;
import java.io.StringReader;

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

public class TestPerFieldAnalzyerWrapper extends TestCase {
  public void testPerField() throws Exception {
    String text = "Qwerty";
    PerFieldAnalyzerWrapper analyzer =
              new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer());
    analyzer.addAnalyzer("special", new SimpleAnalyzer());

    TokenStream tokenStream = analyzer.tokenStream("field",
                                            new StringReader(text));
    Token token = tokenStream.next();
    assertEquals("WhitespaceAnalyzer does not lowercase",
                 "Qwerty",
                 token.termText());

    tokenStream = analyzer.tokenStream("special",
                                            new StringReader(text));
    token = tokenStream.next();
    assertEquals("SimpleAnalyzer lowercases",
                 "qwerty",
                 token.termText());
  }
}
