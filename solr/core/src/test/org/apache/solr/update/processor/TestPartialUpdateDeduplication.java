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

import com.google.common.collect.Maps;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class TestPartialUpdateDeduplication extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-dedup-overwrites.xml", "schema15.xml");
  }

  @Test
  public void testPartialUpdates() throws Exception {
    SignatureUpdateProcessorFactoryTest.checkNumDocs(0);
    String chain = "dedupe";
    // partial update
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "2a");
    Map<String, Object> map = Maps.newHashMap();
    map.put("set", "Hello Dude man!");
    doc.addField("v_t", map);
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    boolean exception_ok = false;
    try {
      addDoc(req.getXML(), chain);
    } catch (Exception e) {
      exception_ok = true;
    }
    assertTrue("Should have gotten an exception with partial update on signature generating field",
        exception_ok);

    SignatureUpdateProcessorFactoryTest.checkNumDocs(0);
    addDoc(adoc("id", "2a", "v_t", "Hello Dude man!", "name", "ali babi'"), chain);
    doc = new SolrInputDocument();
    doc.addField("id", "2a");
    map = Maps.newHashMap();
    map.put("set", "name changed");
    doc.addField("name", map);
    req = new UpdateRequest();
    req.add(doc);
    addDoc(req.getXML(), chain);
    addDoc(commit(), chain);
    SignatureUpdateProcessorFactoryTest.checkNumDocs(1);
  }
}
