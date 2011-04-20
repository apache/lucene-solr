/**
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
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.XmlUpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 */
public class SignatureUpdateProcessorFactoryTest extends SolrTestCaseJ4 {

  /** modified by tests as needed */
  private String chain = "dedupe";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema12.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
    chain = "dedupe"; // set the default that most tests expect
  }

  void checkNumDocs(int n) {
    SolrQueryRequest req = req();
    try {
      assertEquals(n, req.getSearcher().getIndexReader().numDocs());
    } finally {
      req.close();
    }
  }

  @Test
  public void testDupeDetection() throws Exception {
    SolrCore core = h.getCore();
    UpdateRequestProcessorChain chained = core.getUpdateProcessingChain(
        "dedupe");
    SignatureUpdateProcessorFactory factory = ((SignatureUpdateProcessorFactory) chained
        .getFactories()[0]);
    factory.setEnabled(true);
    assertNotNull(chained);

    addDoc(adoc("id", "1a", "v_t", "Hello Dude man!", "name", "ali babi'"));
    addDoc(adoc("id", "2a", "name", "ali babi", "v_t", "Hello Dude man . -"));

    addDoc(commit());

    addDoc(adoc("name", "ali babi'", "id", "3a", "v_t", "Hello Dude man!"));

    addDoc(commit());

    checkNumDocs(1);

    addDoc(adoc("id", "3b", "v_t", "Hello Dude man!", "t_field",
        "fake value galore"));

    addDoc(commit());

    checkNumDocs(2);

    assertU(adoc("id", "5a", "name", "ali babi", "v_t", "MMMMM"));

    addDoc(delI("5a"));

    addDoc(adoc("id", "5a", "name", "ali babi", "v_t", "MMMMM"));

    addDoc(commit());

    checkNumDocs(3);

    addDoc(adoc("id", "same", "name", "baryy white", "v_t", "random1"));
    addDoc(adoc("id", "same", "name", "bishop black", "v_t", "random2"));

    addDoc(commit());

    checkNumDocs(4);
    factory.setEnabled(false);
  }

  @Test
  public void testMultiThreaded() throws Exception {
    UpdateRequestProcessorChain chained = h.getCore().getUpdateProcessingChain(
        "dedupe");
    SignatureUpdateProcessorFactory factory = ((SignatureUpdateProcessorFactory) chained
        .getFactories()[0]);
    factory.setEnabled(true);
    Thread[] threads = null;
    Thread[] threads2 = null;

    threads = new Thread[7];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread() {

        @Override
        public void run() {
          for (int i = 0; i < 30; i++) {
            // h.update(adoc("id", Integer.toString(1+ i), "v_t",
            // "Goodbye Dude girl!"));
            try {
              addDoc(adoc("id", Integer.toString(1 + i), "v_t",
                  "Goodbye Dude girl!"));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
      };

      threads[i].setName("testThread-" + i);
    }

    threads2 = new Thread[3];
    for (int i = 0; i < threads2.length; i++) {
      threads2[i] = new Thread() {

        @Override
        public void run() {
          for (int i = 0; i < 10; i++) {
            // h.update(adoc("id" , Integer.toString(1+ i + 10000), "v_t",
            // "Goodbye Dude girl"));
            // h.update(commit());
            try {
              addDoc(adoc("id", Integer.toString(1 + i), "v_t",
                  "Goodbye Dude girl!"));
              addDoc(commit());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
      };

      threads2[i].setName("testThread2-" + i);
    }

    for (int i = 0; i < threads.length; i++) {
      threads[i].start();
    }

    for (int i = 0; i < threads2.length; i++) {
      threads2[i].start();
    }

    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }

    for (int i = 0; i < threads2.length; i++) {
      threads2[i].join();
    }
    SolrCore core = h.getCore();

    assertU(commit());

    checkNumDocs(1);
    factory.setEnabled(false);
  }

  /**
   * a non-indexed signatureField is fine as long as overwriteDupes==false
   */
  @Test
  public void testNonIndexedSignatureField() throws Exception {
    SolrCore core = h.getCore();

    checkNumDocs(0);    

    chain = "stored_sig";
    addDoc(adoc("id", "2a", "v_t", "Hello Dude man!", "name", "ali babi'"));
    addDoc(adoc("id", "2b", "v_t", "Hello Dude man!", "name", "ali babi'"));
    addDoc(commit());

    checkNumDocs(2);
  }

  @Test
  public void testFailNonIndexedSigWithOverwriteDupes() throws Exception {
    SolrCore core = h.getCore();
    SignatureUpdateProcessorFactory f = new SignatureUpdateProcessorFactory();
    NamedList<String> initArgs = new NamedList<String>();
    initArgs.add("overwriteDupes", "true");
    initArgs.add("signatureField", "signatureField_sS");
    f.init(initArgs);
    boolean exception_ok = false;
    try {
      f.inform(core);
    } catch (Exception e) {
      exception_ok = true;
    }
    assertTrue("Should have gotten an exception from inform(SolrCore)", 
               exception_ok);
  }

  private void addDoc(String doc) throws Exception {
    Map<String, String[]> params = new HashMap<String, String[]>();
    MultiMapSolrParams mmparams = new MultiMapSolrParams(params);
    params.put(UpdateParams.UPDATE_CHAIN, new String[] { chain });
    SolrQueryRequestBase req = new SolrQueryRequestBase(h.getCore(),
        (SolrParams) mmparams) {
    };

    XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();
    handler.init(null);
    ArrayList<ContentStream> streams = new ArrayList<ContentStream>(2);
    streams.add(new ContentStreamBase.StringStream(doc));
    req.setContentStreams(streams);
    handler.handleRequestBody(req, new SolrQueryResponse());
    req.close();
  }
}
