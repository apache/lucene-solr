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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;
import org.apache.solr.common.util.ByteUtils;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BinaryResponseWriter.Resolver;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.util.SimplePostTool;
import org.junit.BeforeClass;

/**
 * Test for BinaryResponseWriter
 *
 *
 * @since solr 1.4
 */
public class TestBinaryResponseWriter extends SolrTestCaseJ4 {

  
  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml", "schema12.xml");
  }

  public void testBytesRefWriting() {
    compareStringFormat("ThisIsUTF8String");
    compareStringFormat("Thailand (ประเทศไทย)");
    compareStringFormat("LIVE: सबरीमाला मंदिर के पास पहुंची दो महिलाएं, जमकर हो रहा विरोध-प्रदर्शन");
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testJavabinCodecWithCharSeq() throws IOException {
    SolrDocument document = new SolrDocument();
    document.put("id", "1");
    String text = "नए लुक में धमाल मचाने आ रहे हैं MS Dhoni, कुछ यूं दिखाया हेलीकॉप्टर शॉट";
    document.put("desc", new StoredField("desc", new ByteArrayUtf8CharSequence(text) {
    }, TextField.TYPE_STORED));

    NamedList nl = new NamedList();
    nl.add("doc1", document);
    SimplePostTool.BAOS baos = new SimplePostTool.BAOS();
    new JavaBinCodec(new BinaryResponseWriter.Resolver(null, null)).marshal(nl, baos);
    ByteBuffer byteBuffer = baos.getByteBuffer();
    nl = (NamedList) new JavaBinCodec().unmarshal(new ByteArrayInputStream(byteBuffer.array(), byteBuffer.arrayOffset(), byteBuffer.limit()));
    assertEquals(text, nl._get("doc1/desc", null));


  }

  private void compareStringFormat(String input) {
    byte[] bytes1 = new byte[1024];
    int len1 = ByteUtils.UTF16toUTF8(input, 0, input.length(), bytes1, 0);
    BytesRef bytesref = new BytesRef(input);
    System.out.println();
    assertEquals(len1, bytesref.length);
    for (int i = 0; i < len1; i++) {
      assertEquals(input + " not matching char at :" + i, bytesref.bytes[i], bytes1[i]);
    }
  }

  /**
   * Tests known types implementation by asserting correct encoding/decoding of UUIDField
   */
  public void testUUID() throws Exception {
    String s = UUID.randomUUID().toString().toLowerCase(Locale.ROOT);
    assertU(adoc("id", "101", "uuid", s));
    assertU(commit());
    LocalSolrQueryRequest req = lrf.makeRequest("q", "*:*");
    SolrQueryResponse rsp = h.queryAndResponse(req.getParams().get(CommonParams.QT), req);
    BinaryQueryResponseWriter writer = (BinaryQueryResponseWriter) h.getCore().getQueryResponseWriter("javabin");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    writer.write(baos, req, rsp);
    @SuppressWarnings({"rawtypes"})
    NamedList res;
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      res = (NamedList) jbc.unmarshal(new ByteArrayInputStream(baos.toByteArray()));
    } 
    SolrDocumentList docs = (SolrDocumentList) res.get("response");
    for (Object doc : docs) {
      SolrDocument document = (SolrDocument) doc;
      assertEquals("Returned object must be a string", "java.lang.String", document.getFieldValue("uuid").getClass().getName());
      assertEquals("Wrong UUID string returned", s, document.getFieldValue("uuid"));
    }

    req.close();
  }

  public void testOmitHeader() throws Exception {
    SolrQueryRequest req = req("q", "*:*", "omitHeader", "true");
    SolrQueryResponse rsp = h.queryAndResponse(null, req);

    NamedList<Object> res = BinaryResponseWriter.getParsedResponse(req, rsp);
    assertNull(res.get("responseHeader"));
    req.close();

    req = req("q", "*:*");
    rsp = h.queryAndResponse(null, req);
    res = BinaryResponseWriter.getParsedResponse(req, rsp);
    assertNotNull(res.get("responseHeader"));
    req.close();
  }

  public void testResolverSolrDocumentPartialFields() throws Exception {
    LocalSolrQueryRequest req = lrf.makeRequest("q", "*:*",
                                                "fl", "id,xxx,ddd_s"); 
    SolrDocument in = new SolrDocument();
    in.addField("id", 345);
    in.addField("aaa_s", "aaa");
    in.addField("bbb_s", "bbb");
    in.addField("ccc_s", "ccc");
    in.addField("ddd_s", "ddd");
    in.addField("eee_s", "eee");    

    Resolver r = new Resolver(req, new SolrReturnFields(req));
    Object o = r.resolve(in, new JavaBinCodec());

    assertNotNull("obj is null", o);
    assertTrue("obj is not doc", o instanceof SolrDocument);

    SolrDocument out = new SolrDocument();
    for (Map.Entry<String, Object> e : in) {
      if(r.isWritable(e.getKey())) out.put(e.getKey(),e.getValue());

    }
    assertTrue("id not found", out.getFieldNames().contains("id"));
    assertTrue("ddd_s not found", out.getFieldNames().contains("ddd_s"));
    assertEquals("Wrong number of fields found", 
                 2, out.getFieldNames().size());
    req.close();

  }

}
