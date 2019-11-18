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
package org.apache.solr.client.solrj;

import java.util.Arrays;

import org.apache.solr.EmbeddedSolrServerTestBase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GetByIdTest extends EmbeddedSolrServerTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    getSolrClient().deleteByQuery("*:*");
    getSolrClient().add(Arrays.asList(
        sdoc("id", "1", "term_s", "Microsoft", "term2_s", "MSFT"),
        sdoc("id", "2", "term_s", "Apple", "term2_s", "AAPL"),
        sdoc("id", "3", "term_s", "Yahoo", "term2_s", "YHOO")));

    getSolrClient().commit(true, true);
  }

  @Test
  public void testGetId() throws Exception {
    SolrDocument rsp = getSolrClient().getById("0");
    assertNull(rsp);

    rsp = getSolrClient().getById("1");
    assertEquals("1", rsp.get("id"));
    assertEquals("Microsoft", rsp.get("term_s"));
    assertEquals("MSFT", rsp.get("term2_s"));

    rsp = getSolrClient().getById("2");
    assertEquals("2", rsp.get("id"));
    assertEquals("Apple", rsp.get("term_s"));
    assertEquals("AAPL", rsp.get("term2_s"));
  }

  @Test
  public void testGetIdWithParams() throws Exception {
    final SolrParams ID_FL_ONLY = params(CommonParams.FL, "id");

    SolrDocument rsp = getSolrClient().getById("0", ID_FL_ONLY);
    assertNull(rsp);

    rsp = getSolrClient().getById("1", ID_FL_ONLY);
    assertEquals("1", rsp.get("id"));
    assertNull("This field should have been removed from the response.", rsp.get("term_s"));
    assertNull("This field should have been removed from the response.", rsp.get("term2_s"));

    rsp = getSolrClient().getById("2", ID_FL_ONLY);
    assertEquals("2", rsp.get("id"));
    assertNull("This field should have been removed from the response.", rsp.get("term_s"));
    assertNull("This field should have been removed from the response.", rsp.get("term2_s"));
  }

  @Test
  public void testGetIds() throws Exception {
    SolrDocumentList rsp = getSolrClient().getById(Arrays.asList("0", "1", "2", "3", "4"));
    assertEquals(3, rsp.getNumFound());
    assertEquals("1", rsp.get(0).get("id"));
    assertEquals("Microsoft", rsp.get(0).get("term_s"));
    assertEquals("MSFT", rsp.get(0).get("term2_s"));

    assertEquals("2", rsp.get(1).get("id"));
    assertEquals("Apple", rsp.get(1).get("term_s"));
    assertEquals("AAPL", rsp.get(1).get("term2_s"));

    assertEquals("3", rsp.get(2).get("id"));
    assertEquals("Yahoo", rsp.get(2).get("term_s"));
    assertEquals("YHOO", rsp.get(2).get("term2_s"));
  }

  @Test
  public void testGetIdsWithParams() throws Exception {
    SolrDocumentList rsp = getSolrClient().getById(Arrays.asList("0", "1", "2"), params(CommonParams.FL, "id"));
    assertEquals(2, rsp.getNumFound());

    assertEquals("1", rsp.get(0).get("id"));
    assertNull("This field should have been removed from the response.", rsp.get(0).get("term_s"));
    assertNull("This field should have been removed from the response.", rsp.get(0).get("term2_s"));

    assertEquals("2", rsp.get(1).get("id"));
    assertNull("This field should have been removed from the response.", rsp.get(1).get("term_s"));
    assertNull("This field should have been removed from the response.", rsp.get(1).get("term2_s"));
  }
}
