package org.apache.lucene.queryParser;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;

/**
 * Tests QueryParser.
 */
public class TestQueryParser extends QueryParserTestBase {

  public QueryParser getParser(Analyzer a) throws Exception {
    if (a == null)
      a = new MockAnalyzer(random, MockTokenizer.SIMPLE, true);
    QueryParser qp = new QueryParser(TEST_VERSION_CURRENT, "field", a);
    qp.setDefaultOperator(QueryParser.OR_OPERATOR);
    return qp;
  }

}
