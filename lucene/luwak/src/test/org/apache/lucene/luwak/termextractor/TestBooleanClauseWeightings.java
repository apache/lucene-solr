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

package org.apache.lucene.luwak.termextractor;

import java.util.Collections;
import java.util.Set;

import org.apache.lucene.luwak.termextractor.weights.TermWeightor;
import org.apache.lucene.luwak.termextractor.weights.TokenLengthNorm;
import org.apache.lucene.luwak.testutils.ParserUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.LuceneTestCase;

public class TestBooleanClauseWeightings extends LuceneTestCase {

  private static QueryAnalyzer treeBuilder = new QueryAnalyzer();

  public void testExactClausesPreferred() throws Exception {
    Query bq = ParserUtils.parse("+field2:[1 TO 2] +(field1:term1 field1:term2)");
    assertEquals(2, treeBuilder.collectTerms(bq, new TermWeightor()).size());
  }

  public void testLongerTermsPreferred() throws Exception {
    Query q = ParserUtils.parse("field1:(+a +supercalifragilisticexpialidocious +b)");
    Set<QueryTerm> expected
        = Collections.singleton(new QueryTerm("field1", "supercalifragilisticexpialidocious", QueryTerm.Type.EXACT));
    assertEquals(expected, treeBuilder.collectTerms(q, new TermWeightor(new TokenLengthNorm())));
  }

}
