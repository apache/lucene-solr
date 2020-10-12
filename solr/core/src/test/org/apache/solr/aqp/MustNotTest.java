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

public class MustNotTest extends AbstractAqpTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testMustNotQuery() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 1, 1).process(getSolrClient());

    // adding some terms with same stem root as search term to see how MustNot
    // handles these
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", "foo bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", "bar foo")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "_text_", "bar none")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "_text_", "foo none")));

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "11", "bill_text_txt", "foo bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "12", "bill_text_txt", "bar foo")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "13", "bill_text_txt", "bar none")));
    getSolrClient().commit(coll);

    //
    QueryResponse resp = getSolrClient().query(coll, params("q", "_text_:* -_text_:foo", "q.op", "and"));
    assertEquals(1, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "3");

    // verify term marked with ! is not returned in returned
    resp = getSolrClient().query(coll, params("q", "_text_:* !foo", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "3");
     //verify term marked with ! is not returned in returned
    resp = getSolrClient().query(coll, params("q", "_text_:* !(foo)", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "3");
    // verify fielded search with MUST NOT
    resp = getSolrClient().query(coll, params("q", "bill_text_txt:* bill_text_txt:!(foo)", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "13");

    // verify fielded search with MUST NOT using
    resp = getSolrClient().query(coll, params("q", "bill_text_txt:* bill_text_txt:!foo", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "13");

    // verify fielded search with MUST NOT using
    resp = getSolrClient().query(coll, params("q", "bill_text_txt:*  !(bill_text_txt:foo)", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "13");

    // verify fielded search with MUST NOT using
    resp = getSolrClient().query(coll, params("q", "bill_text_txt:*  !bill_text_txt:foo", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "13");

  }

}
