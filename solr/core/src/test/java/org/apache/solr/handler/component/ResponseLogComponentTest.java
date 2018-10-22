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
package org.apache.solr.handler.component;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResponseLogComponentTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTest() throws Exception {
    initCore("solrconfig-response-log-component.xml","schema12.xml");
    assertNull(h.validateUpdate(adoc("id", "1", "subject", "aa")));
    assertNull(h.validateUpdate(adoc("id", "two", "subject", "aa")));
    assertNull(h.validateUpdate(adoc("id", "3", "subject", "aa")));
    assertU(commit());
  }

  @Test
  public void testToLogIds() throws Exception {
    SolrQueryRequest req = null;
    try {
      String handler="/withlog";
      req = req("indent","true", "qt","/withlog",  "q","aa", "rows","2",
          "fl","id,subject", "responseLog","true");
      SolrQueryResponse qr = h.queryAndResponse(handler, req);
      NamedList<Object> entries = qr.getToLog();
      String responseLog = (String) entries.get("responseLog");
      assertNotNull(responseLog);
      assertTrue(responseLog.matches("\\w+,\\w+"));
    } finally {
      req.close();
    }
  }

  @Test
  public void testToLogScores() throws Exception {
    SolrQueryRequest req = null;
    try {
      String handler="/withlog";
      req = req("indent","true", "qt","/withlog",  "q","aa", "rows","2",
          "fl","id,subject,score", "responseLog","true");
      SolrQueryResponse qr = h.queryAndResponse(handler, req);
      NamedList<Object> entries = qr.getToLog();
      String responseLog = (String) entries.get("responseLog");
      assertNotNull(responseLog);
      assertTrue(responseLog.matches("\\w+:\\d+\\.\\d+,\\w+:\\d+\\.\\d+"));
    } finally {
      req.close();
    }
  }
  
  @Test
  public void testDisabling() throws Exception {
    SolrQueryRequest req = null;
    try {
      String handler="/withlog";
      req = req("indent","true", "qt","/withlog",  "q","aa", "rows","2",
          "fl","id,subject", "responseLog","false");
      SolrQueryResponse qr = h.queryAndResponse(handler, req);
      NamedList<Object> entries = qr.getToLog();
      String responseLog = (String) entries.get("responseLog");
      assertNull(responseLog);
    } finally {
      req.close();
    }    
  }
}
