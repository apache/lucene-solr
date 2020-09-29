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

package org.apache.solr.aqp;

import java.lang.invoke.MethodHandles;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MustTest extends AbstractAqpTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  @Test
  public void testMustQuery() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)

        .process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "1", "_text_", "foo bar")));
    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "2", "_text_", "bar")));

    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "11", "bill_txt", "foo bar")));
    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "12", "bill_txt", "bar")));
    getSolrClient().commit(coll);

    //  verify that term WITHOUT a plus sign is returned
    QueryResponse resp = getSolrClient().query(coll, params("q", "foo", "defType","advanced"));
    assertEquals(1, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));

    //  verify that term WITH a plus sign is returned
    resp = getSolrClient().query(coll, params("q", "+foo", "defType","advanced"));
    assertEquals(1, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));

    // verify fielded search with MUST using +
    resp = getSolrClient().query(coll, params("q", "+bill_txt:foo", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "11");


    //  verify that term in index NOT MARKED by a plus sign in query is not returned
    resp = getSolrClient().query(coll, params("q", "+foo", "defType","advanced"));
    int resultsSize = resp.getResults().size();
    for (int i = 0; i < resultsSize; i++) {
      assertNotEquals("2", resp.getResults().get(i).get("id"));
    }

    //  verify that multiple terms marked with plus sign will match a document
    //  containing all of the words
    resp = getSolrClient().query(coll, params("q", "+bar +foo", "defType","advanced"));
    assertEquals("1", resp.getResults().get(0).get("id"));

    //  verify that all terms marked with plus sign must be present in the document
    resp = getSolrClient().query(coll, params("q", "+baz +foo", "defType","advanced"));
    assertEquals(0, resp.getResults().size());

  }
}
