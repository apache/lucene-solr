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
package org.apache.solr.handler;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.solr.client.solrj.request.DocumentAnalysisRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

/**
 * A test for {@link DocumentAnalysisRequestHandler}.
 *
 *
 * @since solr 1.4
 */
public class DocumentAnalysisRequestHandlerTest extends AnalysisRequestHandlerTestBase {

  private DocumentAnalysisRequestHandler handler;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Override
  @Before
  @SuppressWarnings({"rawtypes"})
  public void setUp() throws Exception {
    super.setUp();
    handler = new DocumentAnalysisRequestHandler();
    handler.init(new NamedList());
  }

  /**
   * Tests the {@link DocumentAnalysisRequestHandler#resolveAnalysisRequest(org.apache.solr.request.SolrQueryRequest)}
   */
  @Test
  public void testResolveAnalysisRequest() throws Exception {

    String docsInput =
            "<docs>" +
                    "<doc>" +
                    "<field name=\"id\">1</field>" +
                    "<field name=\"whitetok\">The Whitetok</field>" +
                    "<field name=\"text\">The Text</field>" +
                    "</doc>" +
                    "</docs>";

    final ContentStream cs = new ContentStreamBase.StringStream(docsInput);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("analysis.query", "The Query String");
    params.add("analysis.showmatch", "true");
    SolrQueryRequest req = new SolrQueryRequestBase(h.getCore(), params) {
      @Override
      public Iterable<ContentStream> getContentStreams() {
        return Collections.singleton(cs);
      }
    };

    DocumentAnalysisRequest request = handler.resolveAnalysisRequest(req);

    assertNotNull(request);
    assertTrue(request.isShowMatch());
    assertNotNull(request.getQuery());
    assertEquals("The Query String", request.getQuery());
    List<SolrInputDocument> documents = request.getDocuments();
    assertNotNull(documents);
    assertEquals(1, documents.size());
    SolrInputDocument document = documents.get(0);
    SolrInputField field = document.getField("id");
    assertNotNull(field);
    assertEquals("1", field.getFirstValue());
    field = document.getField("whitetok");
    assertNotNull(field);
    assertEquals("The Whitetok", field.getFirstValue());
    field = document.getField("text");
    assertNotNull(field);
    assertEquals("The Text", field.getFirstValue());

    req.close();
  }

  /** A binary-only ContentStream */
  static class ByteStream extends ContentStreamBase {
    private final byte[] bytes;
    
    public ByteStream(byte[] bytes, String contentType) {
      this.bytes = bytes; 
      this.contentType = contentType;
      name = null;
      size = Long.valueOf(bytes.length);
      sourceInfo = "rawBytes";
    }

    @Override
    public InputStream getStream() throws IOException {
      return new ByteArrayInputStream(bytes);
    }

    @Override
    public Reader getReader() throws IOException {
      throw new IOException("This is a byte stream, Readers are not supported.");
    }
  }

  
  // This test should also test charset detection in UpdateRequestHandler,
  // but the DocumentAnalysisRequestHandler is simplier to use/check.
  @Test
  public void testCharsetInDocument() throws Exception {
    final byte[] xmlBytes = (
      "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\r\n" +
      "<docs>\r\n" +
      " <doc>\r\n" +
      "  <field name=\"id\">Müller</field>\r\n" +
      " </doc>" +
      "</docs>"
    ).getBytes(StandardCharsets.ISO_8859_1);
    
    // we declare a content stream without charset:
    final ContentStream cs = new ByteStream(xmlBytes, "application/xml");
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    SolrQueryRequest req = new SolrQueryRequestBase(h.getCore(), params) {
      @Override
      public Iterable<ContentStream> getContentStreams() {
        return Collections.singleton(cs);
      }
    };

    DocumentAnalysisRequest request = handler.resolveAnalysisRequest(req);
    assertNotNull(request);
    final List<SolrInputDocument> documents = request.getDocuments();
    assertNotNull(documents);
    assertEquals(1, documents.size());
    SolrInputDocument doc = documents.get(0);
    assertEquals("Müller", doc.getField("id").getValue());
  }

  // This test should also test charset detection in UpdateRequestHandler,
  // but the DocumentAnalysisRequestHandler is simplier to use/check.
  @Test
  public void testCharsetOutsideDocument() throws Exception {
    final byte[] xmlBytes = (
      "<docs>\r\n" +
      " <doc>\r\n" +
      "  <field name=\"id\">Müller</field>\r\n" +
      " </doc>" +
      "</docs>"
    ).getBytes(StandardCharsets.ISO_8859_1);
    
    // we declare a content stream with charset:
    final ContentStream cs = new ByteStream(xmlBytes, "application/xml; charset=ISO-8859-1");
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    SolrQueryRequest req = new SolrQueryRequestBase(h.getCore(), params) {
      @Override
      public Iterable<ContentStream> getContentStreams() {
        return Collections.singleton(cs);
      }
    };

    DocumentAnalysisRequest request = handler.resolveAnalysisRequest(req);
    assertNotNull(request);
    final List<SolrInputDocument> documents = request.getDocuments();
    assertNotNull(documents);
    assertEquals(1, documents.size());
    SolrInputDocument doc = documents.get(0);
    assertEquals("Müller", doc.getField("id").getValue());
  }

  /**
   * Tests the {@link DocumentAnalysisRequestHandler#handleAnalysisRequest(org.apache.solr.client.solrj.request.DocumentAnalysisRequest,
   * org.apache.solr.schema.IndexSchema)}
   */
  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testHandleAnalysisRequest() throws Exception {

    SolrInputDocument document = new SolrInputDocument();
    document.addField("id", 1);
    document.addField("whitetok", "Jumping Jack");
    document.addField("text", "The Fox Jumped Over The Dogs");
    document.addField("number_l_p", 88L);

    DocumentAnalysisRequest request = new DocumentAnalysisRequest()
            .setQuery("JUMPING")
            .setShowMatch(true)
            .addDocument(document);

    NamedList<Object> result = handler.handleAnalysisRequest(request, h.getCore().getLatestSchema());
    assertNotNull("result is null and it shouldn't be", result);
    NamedList<NamedList<NamedList<Object>>> documentResult = (NamedList<NamedList<NamedList<Object>>>) result.get("1");
    assertNotNull("An analysis for document with key '1' should be returned", documentResult);

    NamedList<Object> queryResult;
    List<NamedList> tokenList;
    NamedList<Object> indexResult;
    NamedList<List<NamedList>> valueResult;
    String name;

    // the id field
    NamedList<NamedList<Object>> idResult = documentResult.get("id");
    assertNotNull("an analysis for the 'id' field should be returned", idResult);
    queryResult = idResult.get("query");
    assertEquals("Only the default analyzer should be applied", 1, queryResult.size());
    name = queryResult.getName(0);
    assertTrue("Only the default analyzer should be applied", name.matches("org.apache.solr.schema.FieldType\\$DefaultAnalyzer.*"));
    tokenList = (List<NamedList>) queryResult.getVal(0);
    assertEquals("Query has only one token", 1, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("JUMPING", null, "word", 0, 7, 1, new int[]{1}, null, false));
    indexResult = idResult.get("index");
    assertEquals("The id field has only a single value", 1, indexResult.size());
    valueResult = (NamedList<List<NamedList>>) indexResult.get("1");
    assertEquals("Only the default analyzer should be applied", 1, valueResult.size());
    name = queryResult.getName(0);
    assertTrue("Only the default analyzer should be applied", name.matches("org.apache.solr.schema.FieldType\\$DefaultAnalyzer.*"));
    tokenList = valueResult.getVal(0);
    assertEquals("The 'id' field value has only one token", 1, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("1", null, "word", 0, 1, 1, new int[]{1}, null, false));

    // the number_l_p field
    NamedList<NamedList<Object>> number_l_p_Result = documentResult.get("number_l_p");
    assertNotNull("an analysis for the 'number_l_p' field should be returned", number_l_p_Result);
    indexResult = number_l_p_Result.get("index");
    assertEquals("The number_l_p field has only a single value", 1, indexResult.size());
    valueResult = (NamedList<List<NamedList>>) indexResult.get("88");
    assertEquals("Only the default analyzer should be applied", 1, valueResult.size());
    name = queryResult.getName(0);
    assertTrue("Only the default analyzer should be applied", name.matches("org.apache.solr.schema.FieldType\\$DefaultAnalyzer.*"));
    tokenList = valueResult.getVal(0);
    assertEquals("The 'number_l_p' field value has only one token", 1, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("88", null, "word", 0, 2, 1, new int[]{1}, null, false));

    // the name field
    NamedList<NamedList<Object>> whitetokResult = documentResult.get("whitetok");
    assertNotNull("an analysis for the 'whitetok' field should be returned", whitetokResult);
    queryResult = whitetokResult.get("query");
    tokenList = (List<NamedList>) queryResult.get(MockTokenizer.class.getName());
    assertNotNull("Expecting the 'MockTokenizer' to be applied on the query for the 'whitetok' field", tokenList);
    assertEquals("Query has only one token", 1, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("JUMPING", null, "word", 0, 7, 1, new int[]{1}, null, false));
    indexResult = whitetokResult.get("index");
    assertEquals("The 'whitetok' field has only a single value", 1, indexResult.size());
    valueResult = (NamedList<List<NamedList>>) indexResult.get("Jumping Jack");
    tokenList = valueResult.getVal(0);
    assertEquals("Expecting 2 tokens to be present", 2, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("Jumping", null, "word", 0, 7, 1, new int[]{1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("Jack", null, "word", 8, 12, 2, new int[]{2}, null, false));

    // the text field
    NamedList<NamedList<Object>> textResult = documentResult.get("text");
    assertNotNull("an analysis for the 'text' field should be returned", textResult);
    queryResult = textResult.get("query");
    tokenList = (List<NamedList>) queryResult.get("org.apache.lucene.analysis.standard.StandardTokenizer");
    assertNotNull("Expecting the 'StandardTokenizer' to be applied on the query for the 'text' field", tokenList);
    assertEquals("Query has only one token", 1, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("JUMPING", null, "<ALPHANUM>", 0, 7, 1, new int[]{1}, null, false));
    tokenList = (List<NamedList>) queryResult.get("org.apache.lucene.analysis.core.LowerCaseFilter");
    assertNotNull("Expecting the 'LowerCaseFilter' to be applied on the query for the 'text' field", tokenList);
    assertEquals("Query has only one token", 1, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("jumping", null, "<ALPHANUM>", 0, 7, 1, new int[]{1,1}, null, false));
    tokenList = (List<NamedList>) queryResult.get("org.apache.lucene.analysis.core.StopFilter");
    assertNotNull("Expecting the 'StopFilter' to be applied on the query for the 'text' field", tokenList);
    assertEquals("Query has only one token", 1, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("jumping", null, "<ALPHANUM>", 0, 7, 1, new int[]{1,1,1}, null, false));
    tokenList = (List<NamedList>) queryResult.get("org.apache.lucene.analysis.en.PorterStemFilter");
    assertNotNull("Expecting the 'PorterStemFilter' to be applied on the query for the 'text' field", tokenList);
    assertEquals("Query has only one token", 1, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("jump", null, "<ALPHANUM>", 0, 7, 1, new int[]{1,1,1,1}, null, false));
    indexResult = textResult.get("index");
    assertEquals("The 'text' field has only a single value", 1, indexResult.size());
    valueResult = (NamedList<List<NamedList>>) indexResult.get("The Fox Jumped Over The Dogs");
    tokenList = valueResult.get("org.apache.lucene.analysis.standard.StandardTokenizer");
    assertNotNull("Expecting the 'StandardTokenizer' to be applied on the index for the 'text' field", tokenList);
    assertEquals("Expecting 6 tokens", 6, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("The", null, "<ALPHANUM>", 0, 3, 1, new int[]{1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("Fox", null, "<ALPHANUM>", 4, 7, 2, new int[]{2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("Jumped", null, "<ALPHANUM>", 8, 14, 3, new int[]{3}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("Over", null, "<ALPHANUM>", 15, 19, 4, new int[]{4}, null, false));
    assertToken(tokenList.get(4), new TokenInfo("The", null, "<ALPHANUM>", 20, 23, 5, new int[]{5}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("Dogs", null, "<ALPHANUM>", 24, 28, 6, new int[]{6}, null, false));
    tokenList = valueResult.get("org.apache.lucene.analysis.core.LowerCaseFilter");
    assertNotNull("Expecting the 'LowerCaseFilter' to be applied on the index for the 'text' field", tokenList);
    assertEquals("Expecting 6 tokens", 6, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("the", null, "<ALPHANUM>", 0, 3, 1, new int[]{1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("fox", null, "<ALPHANUM>", 4, 7, 2, new int[]{2,2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("jumped", null, "<ALPHANUM>", 8, 14, 3, new int[]{3,3}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("over", null, "<ALPHANUM>", 15, 19, 4, new int[]{4,4}, null, false));
    assertToken(tokenList.get(4), new TokenInfo("the", null, "<ALPHANUM>", 20, 23, 5, new int[]{5,5}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("dogs", null, "<ALPHANUM>", 24, 28, 6, new int[]{6,6}, null, false));
    tokenList = valueResult.get("org.apache.lucene.analysis.core.StopFilter");
    assertNotNull("Expecting the 'StopFilter' to be applied on the index for the 'text' field", tokenList);
    assertEquals("Expecting 4 tokens after stop word removal", 4, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("fox", null, "<ALPHANUM>", 4, 7, 2, new int[]{2,2,2}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("jumped", null, "<ALPHANUM>", 8, 14, 3, new int[]{3,3,3}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("over", null, "<ALPHANUM>", 15, 19, 4, new int[]{4,4,4}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("dogs", null, "<ALPHANUM>", 24, 28, 6, new int[]{6,6,6}, null, false));
    tokenList = valueResult.get("org.apache.lucene.analysis.en.PorterStemFilter");
    assertNotNull("Expecting the 'PorterStemFilter' to be applied on the index for the 'text' field", tokenList);
    assertEquals("Expecting 4 tokens", 4, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("fox", null, "<ALPHANUM>", 4, 7, 2, new int[]{2,2,2,2}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("jump", null, "<ALPHANUM>", 8, 14, 3, new int[]{3,3,3,3}, null, true));
    assertToken(tokenList.get(2), new TokenInfo("over", null, "<ALPHANUM>", 15, 19, 4, new int[]{4,4,4,4}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("dog", null, "<ALPHANUM>", 24, 28, 6, new int[]{6,6,6,6}, null, false));
  }
}
