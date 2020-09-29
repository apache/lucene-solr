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

/**
 * <pre>
 * Test searching for exact value
 *   congress:114 - only results from the 114^th^ Congress
 * Test Range searches with integers:
 *   congress:[113 TO 115] - 113^th^, 114^th^, 115th
 *   congress:{113 TO 115] - 114^th^, 115^th^
 *   congress:[113 TO 115} - 113^th^, 114^th^
 *   congress:{113 TO 115} - 114^th^
 * Test Range searches with floating point fields:
 *   something_f:[113 TO 115] - 113.0, 113.1, 114.0, 114.9, 115.0
 *   something_f:{113 TO 115] - 113.1, 114.0, 114.9, 115.0
 *   something_f:[113 TO 115} - 113.0, 113.1, 114.0, 114.9
 *   something_f:{113 TO 115} - 113.1, 114.0, 114.9
 * </pre>
 *
 *
 */
public class NumericSearchTest extends AbstractAqpTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testExactValueSearch() throws Exception {

    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "congress", 114)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "congress", 115)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "congress", 116)));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "congress:114", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "1");
    haveNone(resp, "2", "3");
  }

  @Test
  public void testRangeSearch() throws Exception {

    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "congress", "112")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "congress", "113")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "congress", "114")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "congress", "115")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "congress", "116")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "congress:[113 TO 115]", "defType", "advanced"));
    assertEquals(3, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "2", "3", "4");
    haveNone(resp, "1", "5");

    resp = getSolrClient().query(coll, params("q", "congress:{113 TO 115]", "defType", "advanced"));
    assertEquals(2, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "3", "4");
    haveNone(resp, "1", "2", "5");

    resp = getSolrClient().query(coll, params("q", "congress:[113 TO 115}", "defType", "advanced"));
    assertEquals(2, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "2", "3");
    haveNone(resp, "1", "4", "5");

    resp = getSolrClient().query(coll, params("q", "congress:{113 TO 115}", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "3");
    haveNone(resp, "1", "2", "4", "5");
  }

  @Test
  public void testFloatRangeSearch() throws Exception {

    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "congress", "99.9999")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "congress", "112.99999999")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "congress", "113.0")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "congress", "113.1")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "congress", "114.0")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "6", "congress", "114.000000001")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "7", "congress", "114.9")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "8", "congress", "115.0")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "9", "congress", "115.0000001")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "10", "congress", "116.0")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "congress:[113 TO 115]", "defType", "advanced"));
    assertEquals(6, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "3", "4", "5", "6", "7", "8");
    haveNone(resp, "1", "2", "9", "10");

    resp = getSolrClient().query(coll, params("q", "congress:{113 TO 115]", "defType", "advanced"));
    assertEquals(5, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "4", "5", "6", "7", "8");
    haveNone(resp, "1", "2", "3", "9", "10");

    resp = getSolrClient().query(coll, params("q", "congress:[113 TO 115}", "defType", "advanced"));
    assertEquals(5, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "3", "4", "5", "6", "7");
    haveNone(resp, "1", "2", "8", "9", "10");

    resp = getSolrClient().query(coll, params("q", "congress:{113 TO 115}", "defType", "advanced"));
    assertEquals(4, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "4", "5", "6", "7");
    haveNone(resp, "1", "2", "3", "8", "9", "10");

  }

}
