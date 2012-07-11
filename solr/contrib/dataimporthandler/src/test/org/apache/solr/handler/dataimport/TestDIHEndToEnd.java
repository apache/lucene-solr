package org.apache.solr.handler.dataimport;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.request.LocalSolrQueryRequest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDIHEndToEnd extends AbstractDIHJdbcTestCase {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-solrconfig-end-to-end.xml", "dataimport-schema.xml");
  }
  @Test
  public void testEndToEnd() throws Exception {
    LocalSolrQueryRequest request = lrf.makeRequest("command", "full-import",
        "clean", "true", "commit", "true", "synchronous", "true", "indent", "true");
    h.query("/dataimport-end-to-end", request);
    assertQ(req("*:*"), "//*[@numFound='20']");
    assertQ(req("COUNTRY_NAME:zealand"), "//*[@numFound='2']");
    assertQ(req("COUNTRY_NAME:niue"), "//*[@numFound='3']");
    
    //It would be nice if there was a way to get it to run transformers before putting 
    //data in the cache, then id=2 (person=Ethan, country=NU,NA,NE) could join...)
    //assertQ(req("COUNTRY_NAME:Netherlands"), "//*[@numFound='3']");
    
    assertQ(req("NAME:michael"), "//*[@numFound='1']");
    assertQ(req("SPORT_NAME:kayaking"), "//*[@numFound='2']");
    assertQ(req("SPORT_NAME:fishing"), "//*[@numFound='1']");
    
    request = lrf.makeRequest("indent", "true");
    String response = h.query("/dataimport-end-to-end", request);
    Matcher m = Pattern.compile(".str name..Total Requests made to DataSource..(\\d+)..str.").matcher(response);
    Assert.assertTrue(m.find() && m.groupCount()==1);
    int numRequests = Integer.parseInt(m.group(1));
    Assert.assertTrue(
        "The database should have been hit once each " +
        "for 'Person' & 'Country' and ~20 times for 'Sport'", numRequests<30);
  }
}
