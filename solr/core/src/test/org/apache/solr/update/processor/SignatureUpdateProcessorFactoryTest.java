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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.Constants;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 */
public class SignatureUpdateProcessorFactoryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void betterNotBeJ9() {
    assumeFalse("FIXME: SOLR-5793: This test fails under IBM J9", 
                Constants.JAVA_VENDOR.startsWith("IBM"));
  }


  /** modified by tests as needed */
  private String chain = "dedupe";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-dedup-overwrites.xml", "schema15.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
    chain = "dedupe"; // set the default that most tests expect
  }

  static void checkNumDocs(int n) {
    SolrQueryRequest req = req();
    try {
      assertEquals(n, req.getSearcher().getIndexReader().numDocs());
    } finally {
      req.close();
    }
  }
  
  @Test
  public void testDupeAllFieldsDetection() throws Exception {
    this.chain = "dedupe-allfields";
    assertNotNull(h.getCore().getUpdateProcessingChain(this.chain));

    addDoc(adoc("v_t", "Hello Dude man!"));
    addDoc(adoc("v_t", "Hello Dude man!", "name", "name1'"));
    addDoc(adoc("v_t", "Hello Dude man!", "name", "name2'"));

    addDoc(commit());
    
    checkNumDocs(3);
  }  

  @Test
  public void testDupeDetection() throws Exception {
    assertNotNull(h.getCore().getUpdateProcessingChain(this.chain));

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
  }

  @Test
  public void testMultiThreaded() throws Exception {
    assertNotNull(h.getCore().getUpdateProcessingChain(this.chain));

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
  }

  /**
   * a non-indexed signatureField is fine as long as overwriteDupes==false
   */
  @Test
  public void testNonIndexedSignatureField() throws Exception {
    this.chain = "stored_sig";
    assertNotNull(h.getCore().getUpdateProcessingChain(this.chain));

    checkNumDocs(0);    

    addDoc(adoc("id", "2a", "v_t", "Hello Dude man!", "name", "ali babi'"));
    addDoc(adoc("id", "2b", "v_t", "Hello Dude man!", "name", "ali babi'"));
    addDoc(commit());

    checkNumDocs(2);
  }

  @Test
  public void testFailNonIndexedSigWithOverwriteDupes() throws Exception {
    SolrCore core = h.getCore();
    SignatureUpdateProcessorFactory f = new SignatureUpdateProcessorFactory();
    NamedList<String> initArgs = new NamedList<>();
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
  
  @Test
  @SuppressWarnings({"rawtypes"})
  public void testNonStringFieldsValues() throws Exception {
    this.chain = "dedupe-allfields";
    assertNotNull(h.getCore().getUpdateProcessingChain(this.chain));

    Map<String,String[]> params = new HashMap<>();
    MultiMapSolrParams mmparams = new MultiMapSolrParams(params);
    params.put(UpdateParams.UPDATE_CHAIN, new String[] {chain});
    
    UpdateRequest ureq = new UpdateRequest();
    
    {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("v_t", "same");
      doc.addField("weight", 1.0f);
      doc.addField("ints_is", 34);
      doc.addField("ints_is", 42);
      ureq.add(doc);
    }
    {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("v_t", "same");
      doc.addField("weight", 2.0f);
      doc.addField("ints_is", 42);
      doc.addField("ints_is", 66);
      ureq.add(doc);
    }
    {
      // A and B should have same sig as eachother
      // even though the particulars of how the the ints_is list are built

      SolrInputDocument docA = new SolrInputDocument();
      SolrInputDocument docB = new SolrInputDocument();

      UnusualList<Integer> ints = new UnusualList<>(3);
      for (int val : new int[] {42, 66, 34}) {
        docA.addField("ints_is", val);
        ints.add(val);
      }
      docB.addField("ints_is", ints);

      for (SolrInputDocument doc : new SolrInputDocument[] { docA, docB }) {
        doc.addField("v_t", "same");
        doc.addField("weight", 3.0f);
        ureq.add(doc);
      }
    }
    {
      // now add another doc with the same values as A & B above, 
      // but diff ints_is collection (diff order)
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("v_t", "same");
      doc.addField("weight", 3.0f);
      for (int val : new int[] {66, 42, 34}) {
        doc.addField("ints_is", val);
      }
      ureq.add(doc);
    }
        

    LocalSolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), mmparams);
    try {
      req.setContentStreams(Collections.singletonList(ContentStreamBase.create(new BinaryRequestWriter(), ureq)));
      UpdateRequestHandler h = new UpdateRequestHandler();
      h.init(new NamedList());
      h.handleRequestBody(req, new SolrQueryResponse());
    } finally {
      req.close();
    }
    
    addDoc(commit());
    
    checkNumDocs(4);
  }

  /** A list with an unusual toString */
  private static final class UnusualList<T> extends ArrayList<T> {
    public UnusualList(int size) {
      super(size);
    }
    @Override
    public String toString() {
      return "UNUSUAL:" + super.toString();
    }
  }

  private void addDoc(String doc) throws Exception  {
    addDoc(doc, chain);
  }
}
