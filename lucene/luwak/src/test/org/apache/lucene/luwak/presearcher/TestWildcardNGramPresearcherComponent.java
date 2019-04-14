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

package org.apache.lucene.luwak.presearcher;

import java.io.IOException;
import java.util.Collections;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;

public class TestWildcardNGramPresearcherComponent extends BaseTokenStreamTestCase {

  public void testPresearcherComponent() throws IOException {

    PresearcherComponent comp
        = new WildcardNGramPresearcherComponent("FOO", 10, "__wibble__", Collections.singleton("field1"));

    try (Analyzer input = new WhitespaceAnalyzer()) {

      // field1 is in the excluded set, so nothing should happen
      assertTokenStreamContents(comp.filterDocumentTokens("field1", input.tokenStream("field1", "hello world")),
          new String[]{ "hello", "world" });

      // field2 is not excluded
      assertTokenStreamContents(comp.filterDocumentTokens("field2", input.tokenStream("field2", "harm alarm asdasasdasdasd")),
          new String[]{
              "harm", "harmFOO", "harFOO", "haFOO", "hFOO", "armFOO", "arFOO", "aFOO", "rmFOO", "rFOO", "mFOO", "FOO",
              "alarm", "alarmFOO", "alarFOO", "alaFOO", "alFOO", "larmFOO", "larFOO", "laFOO", "lFOO",
              "asdasasdasdasd", "__wibble__"
          });
    }
  }

}
