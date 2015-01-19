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

package org.apache.solr.client.solrj.embedded;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *
 * @since solr 1.3
 */
public class TestSolrProperties extends AbstractEmbeddedSolrServerTestCase {
  protected static Logger log = LoggerFactory.getLogger(TestSolrProperties.class);

  private static final String SOLR_XML = "solr.xml";

  @Rule
  public TestRule solrTestRules = 
    RuleChain.outerRule(new SystemPropertiesRestoreRule());

  @Override
  protected File getSolrXml() throws Exception {
    return new File(SOLR_HOME, SOLR_XML);
  }

  protected SolrClient getSolrAdmin() {
    return new EmbeddedSolrServer(cores, "core0");
  }
  
  protected SolrClient getRenamedSolrAdmin() {
    return new EmbeddedSolrServer(cores, "renamed_core");
  }

  @Test
  public void testProperties() throws Exception {

    UpdateRequest up = new UpdateRequest();
    up.setAction(ACTION.COMMIT, true, true);
    up.deleteByQuery("*:*");
    up.process(getSolrCore0());
    up.process(getSolrCore1());
    up.clear();

    // Add something to each core
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "AAA");
    doc.setField("core0", "yup stopfra stopfrb stopena stopenb");

    // Add to core0
    up.add(doc);
    up.process(getSolrCore0());

    SolrTestCaseJ4.ignoreException("unknown field");

    // You can't add it to core1
    try {
      up.process(getSolrCore1());
      fail("Can't add core0 field to core1!");
    }
    catch (Exception ex) {
    }

    // Add to core1
    doc.setField("id", "BBB");
    doc.setField("core1", "yup stopfra stopfrb stopena stopenb");
    doc.removeField("core0");
    up.add(doc);
    up.process(getSolrCore1());

    // You can't add it to core1
    try {
      SolrTestCaseJ4.ignoreException("core0");
      up.process(getSolrCore0());
      fail("Can't add core1 field to core0!");
    }
    catch (Exception ex) {
    }
    
    SolrTestCaseJ4.resetExceptionIgnores();

    // now Make sure AAA is in 0 and BBB in 1
    SolrQuery q = new SolrQuery();
    QueryRequest r = new QueryRequest(q);
    q.setQuery("id:AAA");
    assertEquals(1, r.process(getSolrCore0()).getResults().size());
    assertEquals(0, r.process(getSolrCore1()).getResults().size());

    // Now test Changing the default core
    assertEquals(1, getSolrCore0().query(new SolrQuery("id:AAA")).getResults().size());
    assertEquals(0, getSolrCore0().query(new SolrQuery("id:BBB")).getResults().size());

    assertEquals(0, getSolrCore1().query(new SolrQuery("id:AAA")).getResults().size());
    assertEquals(1, getSolrCore1().query(new SolrQuery("id:BBB")).getResults().size());

    // Now test reloading it should have a newer open time
    String name = "core0";
    SolrClient coreadmin = getSolrAdmin();
    CoreAdminResponse mcr = CoreAdminRequest.getStatus(name, coreadmin);
    long before = mcr.getStartTime(name).getTime();
    CoreAdminRequest.reloadCore(name, coreadmin);

    mcr = CoreAdminRequest.getStatus(name, coreadmin);
    long after = mcr.getStartTime(name).getTime();
    assertTrue("should have more recent time: " + after + "," + before, after > before);

  }

}
