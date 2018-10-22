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
package org.apache.solr.update.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 */
public class UniqFieldsUpdateProcessorFactoryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml", "schema12.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testUniqFields() throws Exception {
    SolrCore core = h.getCore();
    UpdateRequestProcessorChain chained = core
      .getUpdateProcessingChain("uniq-fields");
    UniqFieldsUpdateProcessorFactory factory = ((UniqFieldsUpdateProcessorFactory) chained.getProcessors().get(0));
    assertNotNull(chained);

    addDoc(adoc("id", "1a", 
                "uniq", "value1", 
                "uniq", "value1", 
                "uniq", "value2"));
    addDoc(adoc("id", "2a", 
                "uniq2", "value1", 
                "uniq2", "value2", 
                "uniq2", "value1", 
                "uniq2", "value3", 
                "uniq", "value1", 
                "uniq", "value1"));
    addDoc(adoc("id", "1b", 
                "uniq3", "value1", 
                "uniq3", "value1"));
    addDoc(adoc("id", "1c", 
                "nouniq", "value1", 
                "nouniq", "value1", 
                "nouniq", "value2"));
    addDoc(adoc("id", "2c", 
                "nouniq", "value1", 
                "nouniq", "value1", 
                "nouniq", "value2", 
                "uniq2", "value1", 
                "uniq2", "value1"));

    assertU(commit());
    assertQ(req("id:1a"), "count(//*[@name='uniq']/*)=2",
        "//arr[@name='uniq']/str[1][.='value1']",
        "//arr[@name='uniq']/str[2][.='value2']");
    assertQ(req("id:2a"), "count(//*[@name='uniq2']/*)=3",
        "//arr[@name='uniq2']/str[1][.='value1']",
        "//arr[@name='uniq2']/str[2][.='value2']",
        "//arr[@name='uniq2']/str[3][.='value3']");
    assertQ(req("id:2a"), "count(//*[@name='uniq']/*)=1");
    assertQ(req("id:1b"), "count(//*[@name='uniq3'])=1");
    assertQ(req("id:1c"), "count(//*[@name='nouniq']/*)=3");
    assertQ(req("id:2c"), "count(//*[@name='nouniq']/*)=3");
    assertQ(req("id:2c"), "count(//*[@name='uniq2']/*)=1");

  }

  private void addDoc(String doc) throws Exception {
    Map<String, String[]> params = new HashMap<>();
    MultiMapSolrParams mmparams = new MultiMapSolrParams(params);
    params.put(UpdateParams.UPDATE_CHAIN, new String[] { "uniq-fields" });
    SolrQueryRequestBase req = new SolrQueryRequestBase(h.getCore(),
        (SolrParams) mmparams) {
    };

    UpdateRequestHandler handler = new UpdateRequestHandler();
    handler.init(null);
    ArrayList<ContentStream> streams = new ArrayList<>(2);
    streams.add(new ContentStreamBase.StringStream(doc));
    req.setContentStreams(streams);
    handler.handleRequestBody(req, new SolrQueryResponse());
    req.close();
  }
}
