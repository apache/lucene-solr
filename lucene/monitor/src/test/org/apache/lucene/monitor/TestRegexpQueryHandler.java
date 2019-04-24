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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;

public class TestRegexpQueryHandler extends BaseTokenStreamTestCase {

  public void testTermStreamWrapping() throws IOException {

    CustomQueryHandler handler
        = new RegexpQueryHandler("FOO", 10, "__wibble__", Collections.singleton("field1"));

    try (Analyzer input = new WhitespaceAnalyzer()) {

      // field1 is in the excluded set, so nothing should happen
      assertTokenStreamContents(handler.wrapTermStream("field1", input.tokenStream("field1", "hello world")),
          new String[]{ "hello", "world" });

      // field2 is not excluded
      assertTokenStreamContents(handler.wrapTermStream("field2", input.tokenStream("field2", "harm alarm asdasasdasdasd")),
          new String[]{
              "harm", "harmFOO", "harFOO", "haFOO", "hFOO", "armFOO", "arFOO", "aFOO", "rmFOO", "rFOO", "mFOO", "FOO",
              "alarm", "alarmFOO", "alarFOO", "alaFOO", "alFOO", "larmFOO", "larFOO", "laFOO", "lFOO",
              "asdasasdasdasd", "__wibble__"
          });
    }
  }

  private Set<Term> collectTerms(Query q) {
    QueryAnalyzer builder = new QueryAnalyzer(Collections.singletonList(
        new RegexpQueryHandler("XX", 30, "WILDCARD", null)));
    QueryTree tree = builder.buildTree(q, TermWeightor.DEFAULT);
    Set<Term> terms = new HashSet<>();
    tree.collectTerms((f, b) -> terms.add(new Term(f, b)));
    return terms;
  }

  public void testRegexpExtractor() {

    Set<Term> expected = new HashSet<>(Arrays.asList(
        new Term("field", "califragilisticXX"),
        new Term("field", "WILDCARD")));
    assertEquals(expected, collectTerms(new RegexpQuery(new Term("field", "super.*califragilistic"))));

    expected = new HashSet<>(Arrays.asList(
        new Term("field", "hellXX"),
        new Term("field", "WILDCARD")));
    assertEquals(expected, collectTerms(new RegexpQuery(new Term("field", "hell."))));

    expected = new HashSet<>(Arrays.asList(
        new Term("field", "heXX"),
        new Term("field", "WILDCARD")));
    assertEquals(expected, collectTerms(new RegexpQuery(new Term("field", "hel?o"))));

  }

}
