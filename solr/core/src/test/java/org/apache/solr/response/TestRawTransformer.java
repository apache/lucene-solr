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
package org.apache.solr.response;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests Raw JSON output for fields when used with and without the unique key field.
 *
 * See SOLR-7993
 */
public class TestRawTransformer extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-doctransformers.xml", "schema.xml");
  }

  @After
  public void cleanup() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @Test
  public void testCustomTransformer() throws Exception {
    // Build a simple index
    int max = 10;
    for (int i = 0; i < max; i++) {
      SolrInputDocument sdoc = new SolrInputDocument();
      sdoc.addField("id", i);
      sdoc.addField("subject", "{poffL:[{offL:[{oGUID:\"79D5A31D-B3E4-4667-B812-09DF4336B900\",oID:\"OO73XRX\",prmryO:1,oRank:1,addTp:\"Office\",addCd:\"AA4GJ5T\",ad1:\"102 S 3rd St Ste 100\",city:\"Carson City\",st:\"MI\",zip:\"48811\",lat:43.176885,lng:-84.842919,phL:[\"(989) 584-1308\"],faxL:[\"(989) 584-6453\"]}]}]}");
      sdoc.addField("title", "title_" + i);
      updateJ(jsonAdd(sdoc), null);
    }
    assertU(commit());
    assertQ(req("q", "*:*"), "//*[@numFound='" + max + "']");

    SolrQueryRequest req = req("q", "*:*", "fl", "subject:[json]", "wt", "json");
    String strResponse = h.query(req);
    assertTrue("response does not contain right JSON encoding: " + strResponse,
        strResponse.contains("\"subject\":[{poffL:[{offL:[{oGUID:\"7"));

    req = req("q", "*:*", "fl", "id,subject", "wt", "json");
    strResponse = h.query(req);
    assertTrue("response does not contain right JSON encoding: " + strResponse,
        strResponse.contains("subject\":[\""));
  }

}

