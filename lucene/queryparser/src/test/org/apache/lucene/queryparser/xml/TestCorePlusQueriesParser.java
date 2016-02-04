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
package org.apache.lucene.queryparser.xml;

import org.apache.lucene.search.Query;

import java.io.IOException;

public class TestCorePlusQueriesParser extends TestCoreParser {

  private CoreParser corePlusQueriesParser;

  public void testLikeThisQueryXML() throws Exception {
    Query q = parse("LikeThisQuery.xml");
    dumpResults("like this", q, 5);
  }

  public void testBoostingQueryXML() throws Exception {
    Query q = parse("BoostingQuery.xml");
    dumpResults("boosting ", q, 5);
  }

  public void testTermsFilterXML() throws Exception {
    Query q = parse("TermsFilterQuery.xml");
    dumpResults("Terms Filter", q, 5);
  }

  public void testBooleanFilterXML() throws ParserException, IOException {
    Query q = parse("BooleanFilter.xml");
    dumpResults("Boolean filter", q, 5);
  }

  //================= Helper methods ===================================

  @Override
  protected CoreParser coreParser() {
    if (corePlusQueriesParser == null) {
      corePlusQueriesParser = new CorePlusQueriesParser(
          super.defaultField(),
          super.analyzer());
    }
    return corePlusQueriesParser;
  }

}
