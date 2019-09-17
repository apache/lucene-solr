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

package org.apache.solr.search.join.another;

import java.io.IOException;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.join.BlockJoinParentQParser;
import org.junit.Assert;
import org.junit.BeforeClass;

import static org.apache.solr.search.join.BJQParserTest.createIndex;

public class BJQFilterAccessibleTest  extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema15.xml");
    createIndex();
  }

  public void testAbilityToCreateBJQfromAnotherPackage() throws IOException {
    try (SolrQueryRequest req = lrf.makeRequest()) {
      TermQuery childQuery = new TermQuery(new Term("child_s", "l"));
      Query parentQuery = new WildcardQuery(new Term("parent_s", "*"));
      ToParentBlockJoinQuery tpbjq = new ToParentBlockJoinQuery(childQuery, BlockJoinParentQParser.getCachedFilter(req,parentQuery).getFilter(), ScoreMode.Max);
      Assert.assertEquals(6, req.getSearcher().search(tpbjq,10).totalHits.value);
    }
  }
}
