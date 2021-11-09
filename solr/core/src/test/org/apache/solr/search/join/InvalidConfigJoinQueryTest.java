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
package org.apache.solr.search.join;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

public class InvalidConfigJoinQueryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void before() throws Exception {
    System.setProperty("solr.filterCache.async", "false");
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testInvalidFilterConfig() throws Exception {
    UpdateRequest req = new UpdateRequest();
    req.add(new SolrInputDocument("id", "0", "type_s", "org", "locid_s", "1"));
    req.add(new SolrInputDocument("id", "1", "type_s", "loc", "orgid_s", "0"));

    SolrClient client = new EmbeddedSolrServer(h.getCore());
    req.commit(client, null);

    assertThrows(SolrException.class, () -> assertJQ(req("q", "{!join from=id to=locid_s v=$q1}", "q1", "type_s:loc", "fl", "id", "sort", "id asc")));
  }
}
