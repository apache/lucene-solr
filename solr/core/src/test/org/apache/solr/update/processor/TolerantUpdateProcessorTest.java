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

import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.DirectSolrConnection;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.BaseTestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

public class TolerantUpdateProcessorTest extends UpdateProcessorTestBase {
  
  /**
   * List of valid + invalid documents
   */
  private static List<SolrInputDocument> docs = null;
  /**
   * IDs of the invalid documents in <code>docs</code>
   */
  private static String[] badIds = null;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-update-processor-chains.xml", "schema12.xml");
  }
  
  @AfterClass
  public static void tearDownClass() {
    docs = null;
    badIds = null;
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    //expected exception messages
    ignoreException("Error adding field");
    ignoreException("Document is missing mandatory uniqueKey field");
    if (docs == null) {
      docs = new ArrayList<>(20);
      badIds = new String[10];
      for(int i = 0; i < 10;i++) {
        // a valid document
        docs.add(doc(field("id", String.valueOf(2*i)), field("weight", i)));
        // ... and an invalid one
        docs.add(doc(field("id", String.valueOf(2*i+1)), field("weight", "b")));
        badIds[i] = String.valueOf(2*i+1);
      }
    }
    
  }
  
  @Override
  public void tearDown() throws Exception {
    resetExceptionIgnores();
    assertU(delQ("*:*"));
    assertU(commit());
    assertQ(req("q","*:*")
        ,"//result[@numFound='0']");
    super.tearDown();
  }

  /**
   * future proof TolerantUpdateProcessor against new default method impls being added to UpdateProcessor 
   * to ensure that every method involved in a processor chain life cycle is overridden with 
   * exception catching/tracking.
   */
  public void testReflection() {
    for (Method method : TolerantUpdateProcessor.class.getMethods()) {
      if (method.getDeclaringClass().equals(Object.class) || method.getName().equals("close")) {
        continue;
      }
      assertEquals("base class(es) has changed, TolerantUpdateProcessor needs updated to ensure it " +
                   "overrides all solr update lifcycle methods with exception tracking: " + method.toString(),
                   TolerantUpdateProcessor.class, method.getDeclaringClass());
    }
  }
 
  
  @Test
  public void testValidAdds() throws IOException {
    SolrInputDocument validDoc = doc(field("id", "1"), field("text", "the quick brown fox"));
    add("tolerant-chain-max-errors-10", null, validDoc);
    
    validDoc = doc(field("id", "2"), field("text", "the quick brown fox"));
    add("tolerant-chain-max-errors-not-set", null, validDoc);
    
    assertU(commit());
    assertQ(req("q","*:*")
        ,"//result[@numFound='2']");
    assertQ(req("q","id:1")
        ,"//result[@numFound='1']");
    assertQ(req("q","id:2")
        ,"//result[@numFound='1']");
  }
  
  @Test
  public void testInvalidAdds() throws IOException {
    SolrInputDocument invalidDoc1 = doc(field("text", "the quick brown fox")); //no id
    // This doc should fail without being tolerant
    Exception e = expectThrows(Exception.class, () -> add("not-tolerant", null, invalidDoc1));
    assertTrue(e.getMessage().contains("Document is missing mandatory uniqueKey field"));

    assertAddsSucceedWithErrors("tolerant-chain-max-errors-10", Arrays.asList(new SolrInputDocument[]{invalidDoc1}), null, "(unknown)");
    
    //a valid doc
    SolrInputDocument validDoc1 = doc(field("id", "1"), field("text", "the quick brown fox"));

    // This batch should fail without being tolerant
    e = expectThrows(Exception.class, () -> add("not-tolerant", null,
        Arrays.asList(new SolrInputDocument[]{invalidDoc1, validDoc1})));
    assertTrue(e.getMessage().contains("Document is missing mandatory uniqueKey field"));
    
    assertU(commit());
    assertQ(req("q","id:1"),"//result[@numFound='0']");
    
    
    assertAddsSucceedWithErrors("tolerant-chain-max-errors-10", Arrays.asList(new SolrInputDocument[]{invalidDoc1, validDoc1}), null, "(unknown)");
    assertU(commit());
    
    // verify that the good document made it in. 
    assertQ(req("q","id:1"),"//result[@numFound='1']");
    
    SolrInputDocument invalidDoc2 = doc(field("id", "2"), field("weight", "aaa"));
    SolrInputDocument validDoc2 = doc(field("id", "3"), field("weight", "3"));

    // This batch should fail without being tolerant
    e = expectThrows(Exception.class, () -> add("not-tolerant", null,
        Arrays.asList(new SolrInputDocument[]{invalidDoc2, validDoc2})));
    assertTrue(e.getMessage().contains("Error adding field"));

    assertU(commit());
    assertQ(req("q","id:3"),"//result[@numFound='0']");
    
    assertAddsSucceedWithErrors("tolerant-chain-max-errors-10", Arrays.asList(new SolrInputDocument[]{invalidDoc2, validDoc2}), null, "2");
    assertU(commit());
    
    // The valid document was indexed
    assertQ(req("q","id:3"),"//result[@numFound='1']");
    
    // The invalid document was NOT indexed
    assertQ(req("q","id:2"),"//result[@numFound='0']");
    
  }
  
  @Test
  public void testMaxErrorsDefault() throws IOException {
    // by default the TolerantUpdateProcessor accepts all errors, so this batch should succeed with 10 errors.
    assertAddsSucceedWithErrors("tolerant-chain-max-errors-not-set", docs, null, badIds);
    assertU(commit());
    assertQ(req("q","*:*"),"//result[@numFound='10']");
  }
  
  public void testMaxErrorsSucceed() throws IOException {
    ModifiableSolrParams requestParams = new ModifiableSolrParams();
    requestParams.add("maxErrors", "10");
    // still OK
    assertAddsSucceedWithErrors("tolerant-chain-max-errors-not-set", docs, requestParams, badIds);
    assertU(commit());
    assertQ(req("q","*:*"),"//result[@numFound='10']");
  }
  
  @Test
  public void testMaxErrorsThrowsException() throws IOException {
    ModifiableSolrParams requestParams = new ModifiableSolrParams();
    requestParams.add("maxErrors", "5");

    SolrException e = expectThrows(SolrException.class, () ->
        assertAddsSucceedWithErrors("tolerant-chain-max-errors-not-set", docs, requestParams, badIds));
    assertTrue(e.getMessage(),
        e.getMessage().contains("ERROR: [doc=1] Error adding field 'weight'='b' msg=For input string: \"b\""));
    //the first good documents made it to the index
    assertU(commit());
    assertQ(req("q","*:*"),"//result[@numFound='6']");
  }

  @Test
  public void testMaxErrorsInfinite() throws IOException {
    ModifiableSolrParams requestParams = new ModifiableSolrParams();
    requestParams.add("maxErrors", "-1");
    assertAddsSucceedWithErrors("tolerant-chain-max-errors-not-set", docs, null, badIds);

    assertU(commit());
    assertQ(req("q","*:*"),"//result[@numFound='10']");
  }
  
  @Test
  public void testMaxErrors0() throws IOException {
    //make the TolerantUpdateProcessor intolerant
    List<SolrInputDocument> smallBatch = docs.subList(0, 2);
    ModifiableSolrParams requestParams = new ModifiableSolrParams();
    requestParams.add("maxErrors", "0");

    SolrException e = expectThrows(SolrException.class, () ->
        assertAddsSucceedWithErrors("tolerant-chain-max-errors-10", smallBatch, requestParams, "1"));
    assertTrue(e.getMessage().contains("ERROR: [doc=1] Error adding field 'weight'='b' msg=For input string: \"b\""));

    //the first good documents made it to the index
    assertU(commit());
    assertQ(req("q","*:*"),"//result[@numFound='1']");
  }
  
  @Test
  public void testInvalidDelete() throws XPathExpressionException, SAXException {
    ignoreException("undefined field invalidfield");
    String response = update("tolerant-chain-max-errors-10", adoc("id", "1", "text", "the quick brown fox"));
    assertNull(BaseTestHarness.validateXPath(response,
                                             "//int[@name='status']=0",
                                             "//arr[@name='errors']",
                                             "count(//arr[@name='errors']/lst)=0"));
    
    response = update("tolerant-chain-max-errors-10", delQ("invalidfield:1"));
    assertNull(BaseTestHarness.validateXPath
               (response,
                "//int[@name='status']=0",
                "count(//arr[@name='errors']/lst)=1",
                "//arr[@name='errors']/lst/str[@name='type']/text()='DELQ'",
                "//arr[@name='errors']/lst/str[@name='id']/text()='invalidfield:1'",
                "//arr[@name='errors']/lst/str[@name='message']/text()='undefined field invalidfield'"));
  }
  
  @Test
  public void testValidDelete() throws XPathExpressionException, SAXException {
    ignoreException("undefined field invalidfield");
    String response = update("tolerant-chain-max-errors-10", adoc("id", "1", "text", "the quick brown fox"));
    assertNull(BaseTestHarness.validateXPath(response,
                                             "//int[@name='status']=0",
                                             "//arr[@name='errors']",
                                             "count(//arr[@name='errors']/lst)=0"));

    assertU(commit());
    assertQ(req("q","*:*")
        ,"//result[@numFound='1']");
    
    response = update("tolerant-chain-max-errors-10", delQ("id:1"));
    assertNull(BaseTestHarness.validateXPath(response,
                                             "//int[@name='status']=0",
                                             "//arr[@name='errors']",
                                             "count(//arr[@name='errors']/lst)=0"));
    assertU(commit());
    assertQ(req("q","*:*")
        ,"//result[@numFound='0']");
  }
  
  @Test
  public void testResponse() throws SAXException, XPathExpressionException, IOException {
    String response = update("tolerant-chain-max-errors-10", adoc("id", "1", "text", "the quick brown fox"));
    assertNull(BaseTestHarness.validateXPath(response,
                                             "//int[@name='status']=0",
                                             "//arr[@name='errors']",
                                             "count(//arr[@name='errors']/lst)=0"));
    response = update("tolerant-chain-max-errors-10", adoc("text", "the quick brown fox"));
    assertNull(BaseTestHarness.validateXPath(response, "//int[@name='status']=0",
        "//int[@name='maxErrors']/text()='10'",
        "count(//arr[@name='errors']/lst)=1",
        "//arr[@name='errors']/lst/str[@name='id']/text()='(unknown)'",
        "//arr[@name='errors']/lst/str[@name='message']/text()='Document is missing mandatory uniqueKey field: id'"));
    
    response = update("tolerant-chain-max-errors-10", adoc("text", "the quick brown fox"));
    StringWriter builder = new StringWriter();
    builder.append("<add>");
    for (SolrInputDocument doc:docs) {
      ClientUtils.writeXML(doc, builder);
    }
    builder.append("</add>");
    response = update("tolerant-chain-max-errors-10", builder.toString());
    assertNull(BaseTestHarness.validateXPath(response, "//int[@name='status']=0",
        "//int[@name='maxErrors']/text()='10'",
        "count(//arr[@name='errors']/lst)=10",
        "not(//arr[@name='errors']/lst/str[@name='id']/text()='0')",
        "//arr[@name='errors']/lst/str[@name='id']/text()='1'",
        "not(//arr[@name='errors']/lst/str[@name='id']/text()='2')",
        "//arr[@name='errors']/lst/str[@name='id']/text()='3'",
        "not(//arr[@name='errors']/lst/str[@name='id']/text()='4')",
        "//arr[@name='errors']/lst/str[@name='id']/text()='5'",
        "not(//arr[@name='errors']/lst/str[@name='id']/text()='6')",
        "//arr[@name='errors']/lst/str[@name='id']/text()='7'",
        "not(//arr[@name='errors']/lst/str[@name='id']/text()='8')",
        "//arr[@name='errors']/lst/str[@name='id']/text()='9'",
        "not(//arr[@name='errors']/lst/str[@name='id']/text()='10')",
        "//arr[@name='errors']/lst/str[@name='id']/text()='11'",
        "not(//arr[@name='errors']/lst/str[@name='id']/text()='12')",
        "//arr[@name='errors']/lst/str[@name='id']/text()='13'",
        "not(//arr[@name='errors']/lst/str[@name='id']/text()='14')",
        "//arr[@name='errors']/lst/str[@name='id']/text()='15'",
        "not(//arr[@name='errors']/lst/str[@name='id']/text()='16')",
        "//arr[@name='errors']/lst/str[@name='id']/text()='17'",
        "not(//arr[@name='errors']/lst/str[@name='id']/text()='18')",
        "//arr[@name='errors']/lst/str[@name='id']/text()='19'"));

    // spot check response when effective maxErrors is unlimited
    response = update("tolerant-chain-max-errors-not-set", builder.toString());
    assertNull(BaseTestHarness.validateXPath(response, "//int[@name='maxErrors']/text()='-1'"));
                                             
  }


  
  public String update(String chain, String xml) {
    DirectSolrConnection connection = new DirectSolrConnection(h.getCore());
    SolrRequestHandler handler = h.getCore().getRequestHandler("/update");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("update.chain", chain);
    try {
      return connection.request(handler, params, xml);
    } catch (SolrException e) {
      throw (SolrException)e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }
  
  private void assertAddsSucceedWithErrors(String chain,
                                           final Collection<SolrInputDocument> docs,
                                           SolrParams requestParams, 
                                           String... idsShouldFail) throws IOException {

    SolrQueryResponse response = add(chain, requestParams, docs);
    
    @SuppressWarnings("unchecked")
    List<SimpleOrderedMap<String>> errors = (List<SimpleOrderedMap<String>>)
      response.getResponseHeader().get("errors");
    assertNotNull(errors);

    assertEquals("number of errors", idsShouldFail.length, errors.size());
    
    Set<String> addErrorIdsExpected = new HashSet<String>(Arrays.asList(idsShouldFail));

    for (SimpleOrderedMap<String> err : errors) {
      assertEquals("this method only expects 'add' errors", "ADD", err.get("type"));
      
      String id = err.get("id");
      assertNotNull("null err id", id);
      assertTrue("unexpected id", addErrorIdsExpected.contains(id));

    }
  }
  
  protected SolrQueryResponse add(final String chain, SolrParams requestParams, final SolrInputDocument doc) throws IOException {
    return add(chain, requestParams, Arrays.asList(new SolrInputDocument[]{doc}));
  }
  
  protected SolrQueryResponse add(final String chain, SolrParams requestParams, final Collection<SolrInputDocument> docs) throws IOException {
    
    SolrCore core = h.getCore();
    UpdateRequestProcessorChain pc = core.getUpdateProcessingChain(chain);
    assertNotNull("No Chain named: " + chain, pc);
    
    SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap<Object>());
    
    if(requestParams == null) {
      requestParams = new ModifiableSolrParams();
    }
    
    SolrQueryRequest req = new LocalSolrQueryRequest(core, requestParams);
    UpdateRequestProcessor processor = null;
    try {
      processor = pc.createProcessor(req, rsp);
      for(SolrInputDocument doc:docs) {
        AddUpdateCommand cmd = new AddUpdateCommand(req);
        cmd.solrDoc = doc;
        processor.processAdd(cmd);
      }
      processor.finish();
      
    } finally {
      IOUtils.closeQuietly(processor);
      req.close();
    }
    return rsp;
  }
  
}
