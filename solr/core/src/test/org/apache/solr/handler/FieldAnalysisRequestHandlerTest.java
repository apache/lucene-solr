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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttributeImpl;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.client.solrj.request.FieldAnalysisRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.AnalysisParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.TextField;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A test for {@link FieldAnalysisRequestHandler}.
 *
 *
 * @since solr 1.4
 */
public class FieldAnalysisRequestHandlerTest extends AnalysisRequestHandlerTestBase {
  
  private FieldAnalysisRequestHandler handler;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    handler = new FieldAnalysisRequestHandler();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }
  
  @Test
  public void testPointField() throws Exception {
    FieldAnalysisRequest request = new FieldAnalysisRequest();
    request.addFieldType("pint");
    request.setFieldValue("5");
    
    @SuppressWarnings({"rawtypes"})
    NamedList<NamedList> nl = handler.handleAnalysisRequest(request, h.getCore().getLatestSchema());
    @SuppressWarnings({"rawtypes"})
    NamedList pintNL = (NamedList)nl.get("field_types").get("pint");
    @SuppressWarnings({"rawtypes"})
    NamedList indexNL = (NamedList)pintNL.get("index");
    @SuppressWarnings({"rawtypes"})
    ArrayList analyzerNL = (ArrayList)indexNL.get("org.apache.solr.schema.FieldType$DefaultAnalyzer$1");
    String text = (String)((NamedList)analyzerNL.get(0)).get("text"); 
    assertEquals("5", text);
  }

  /**
   * Tests the {@link FieldAnalysisRequestHandler#resolveAnalysisRequest(org.apache.solr.request.SolrQueryRequest)}
   */
  @Test
  public void testResolveAnalysisRequest() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(AnalysisParams.FIELD_NAME, "text,nametext");
    params.add(AnalysisParams.FIELD_TYPE, "whitetok,keywordtok");
    params.add(AnalysisParams.FIELD_VALUE, "the quick red fox jumped over the lazy brown dogs");
    params.add(CommonParams.Q, "fox brown");

    SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), params);
    FieldAnalysisRequest request = handler.resolveAnalysisRequest(req);
    List<String> fieldNames = request.getFieldNames();
    assertEquals("Expecting 2 field names", 2, fieldNames.size());
    assertEquals("text", fieldNames.get(0));
    assertEquals("nametext", fieldNames.get(1));
    List<String> fieldTypes = request.getFieldTypes();
    assertEquals("Expecting 2 field types", 2, fieldTypes.size());
    assertEquals("whitetok", fieldTypes.get(0));
    assertEquals("keywordtok", fieldTypes.get(1));
    assertEquals("the quick red fox jumped over the lazy brown dogs", request.getFieldValue());
    assertEquals("fox brown", request.getQuery());
    assertFalse(request.isShowMatch());
    req.close();

    // testing overide of query value using analysis.query param
    params.add(AnalysisParams.QUERY, "quick lazy");
    req=new LocalSolrQueryRequest(h.getCore(), params);
    request = handler.resolveAnalysisRequest(req);
    assertEquals("quick lazy", request.getQuery());
    req.close();

    // testing analysis.showmatch param
    params.add(AnalysisParams.SHOW_MATCH, "false");
    req=new LocalSolrQueryRequest(h.getCore(), params);
    request = handler.resolveAnalysisRequest(req);
    assertFalse(request.isShowMatch());
    req.close();

    params.set(AnalysisParams.SHOW_MATCH, "true");
    req=new LocalSolrQueryRequest(h.getCore(), params);
    request = handler.resolveAnalysisRequest(req);
    assertTrue(request.isShowMatch());
    req.close();

    // testing absence of query value
    params.remove(CommonParams.Q);
    params.remove(AnalysisParams.QUERY);
    req=new LocalSolrQueryRequest(h.getCore(), params);
    request = handler.resolveAnalysisRequest(req);
    assertNull(request.getQuery());
    req.close();

    // test absence of index-time value and presence of q
    params.remove(AnalysisParams.FIELD_VALUE);
    params.add(CommonParams.Q, "quick lazy");
    request = handler.resolveAnalysisRequest(req);
    assertEquals("quick lazy", request.getQuery());
    req.close();

    // test absence of index-time value and presence of query
    params.remove(CommonParams.Q);
    params.add(AnalysisParams.QUERY, "quick lazy");
    request = handler.resolveAnalysisRequest(req);
    assertEquals("quick lazy", request.getQuery());
    req.close();

    // must fail if all of q, analysis.query or analysis.value are absent
    params.remove(CommonParams.Q);
    params.remove(AnalysisParams.QUERY);
    params.remove(AnalysisParams.FIELD_VALUE);
    try (SolrQueryRequest solrQueryRequest = new LocalSolrQueryRequest(h.getCore(), params)) {
      SolrException ex = expectThrows(SolrException.class, () -> handler.resolveAnalysisRequest(solrQueryRequest));
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    }

    req.close();
  }

  /**
   * Tests the {@link FieldAnalysisRequestHandler#handleAnalysisRequest(org.apache.solr.client.solrj.request.FieldAnalysisRequest,
   * org.apache.solr.schema.IndexSchema)}
   */
  @Test
  @SuppressWarnings({"unchecked"})
  public void testHandleAnalysisRequest() throws Exception {

    FieldAnalysisRequest request = new FieldAnalysisRequest();
    request.addFieldName("whitetok");
    request.addFieldName("keywordtok");
    request.addFieldType("text");
    request.addFieldType("nametext");
    request.setFieldValue("the quick red fox jumped over the lazy brown dogs");
    request.setQuery("fox brown");
    request.setShowMatch(true);

    @SuppressWarnings({"rawtypes"})
    NamedList<NamedList> result = handler.handleAnalysisRequest(request, h.getCore().getLatestSchema());
    assertTrue("result is null and it shouldn't be", result != null);

    @SuppressWarnings({"rawtypes"})
    NamedList<NamedList> fieldTypes = result.get("field_types");
    assertNotNull("field_types should never be null", fieldTypes);
    @SuppressWarnings({"rawtypes"})
    NamedList<NamedList> textType = fieldTypes.get("text");
    assertNotNull("expecting result for field type 'text'", textType);

    @SuppressWarnings({"rawtypes"})
    NamedList<List<NamedList>> indexPart = textType.get("index");
    assertNotNull("expecting an index token analysis for field type 'text'", indexPart);

    @SuppressWarnings({"rawtypes"})
    List<NamedList> tokenList = indexPart.get("org.apache.lucene.analysis.standard.StandardTokenizer");
    assertNotNull("Expcting StandardTokenizer analysis breakdown", tokenList);
    assertEquals(tokenList.size(), 10);
    assertToken(tokenList.get(0), new TokenInfo("the", null, "<ALPHANUM>", 0, 3, 1, new int[]{1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("quick", null, "<ALPHANUM>", 4, 9, 2, new int[]{2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("red", null, "<ALPHANUM>", 10, 13, 3, new int[]{3}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("fox", null, "<ALPHANUM>", 14, 17, 4, new int[]{4}, null, true));
    assertToken(tokenList.get(4), new TokenInfo("jumped", null, "<ALPHANUM>", 18, 24, 5, new int[]{5}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("over", null, "<ALPHANUM>", 25, 29, 6, new int[]{6}, null, false));
    assertToken(tokenList.get(6), new TokenInfo("the", null, "<ALPHANUM>", 30, 33, 7, new int[]{7}, null, false));
    assertToken(tokenList.get(7), new TokenInfo("lazy", null, "<ALPHANUM>", 34, 38, 8, new int[]{8}, null, false));
    assertToken(tokenList.get(8), new TokenInfo("brown", null, "<ALPHANUM>", 39, 44, 9, new int[]{9}, null, true));
    assertToken(tokenList.get(9), new TokenInfo("dogs", null, "<ALPHANUM>", 45, 49, 10, new int[]{10}, null, false));
    tokenList = indexPart.get("org.apache.lucene.analysis.core.LowerCaseFilter");
    assertNotNull("Expcting LowerCaseFilter analysis breakdown", tokenList);
    assertEquals(tokenList.size(), 10);
    assertToken(tokenList.get(0), new TokenInfo("the", null, "<ALPHANUM>", 0, 3, 1, new int[]{1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("quick", null, "<ALPHANUM>", 4, 9, 2, new int[]{2,2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("red", null, "<ALPHANUM>", 10, 13, 3, new int[]{3,3}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("fox", null, "<ALPHANUM>", 14, 17, 4, new int[]{4,4}, null, true));
    assertToken(tokenList.get(4), new TokenInfo("jumped", null, "<ALPHANUM>", 18, 24, 5, new int[]{5,5}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("over", null, "<ALPHANUM>", 25, 29, 6, new int[]{6,6}, null, false));
    assertToken(tokenList.get(6), new TokenInfo("the", null, "<ALPHANUM>", 30, 33, 7, new int[]{7,7}, null, false));
    assertToken(tokenList.get(7), new TokenInfo("lazy", null, "<ALPHANUM>", 34, 38, 8, new int[]{8,8}, null, false));
    assertToken(tokenList.get(8), new TokenInfo("brown", null, "<ALPHANUM>", 39, 44, 9, new int[]{9,9}, null, true));
    assertToken(tokenList.get(9), new TokenInfo("dogs", null, "<ALPHANUM>", 45, 49, 10, new int[]{10,10}, null, false));
    tokenList = indexPart.get("org.apache.lucene.analysis.core.StopFilter");
    assertNotNull("Expcting StopFilter analysis breakdown", tokenList);
    assertEquals(tokenList.size(), 8);
    assertToken(tokenList.get(0), new TokenInfo("quick", null, "<ALPHANUM>", 4, 9, 2, new int[]{2,2,2}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("red", null, "<ALPHANUM>", 10, 13, 3, new int[]{3,3,3}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("fox", null, "<ALPHANUM>", 14, 17, 4, new int[]{4,4,4}, null, true));
    assertToken(tokenList.get(3), new TokenInfo("jumped", null, "<ALPHANUM>", 18, 24, 5, new int[]{5,5,5}, null, false));
    assertToken(tokenList.get(4), new TokenInfo("over", null, "<ALPHANUM>", 25, 29, 6, new int[]{6,6,6}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("lazy", null, "<ALPHANUM>", 34, 38, 8, new int[]{8,8,8}, null, false));
    assertToken(tokenList.get(6), new TokenInfo("brown", null, "<ALPHANUM>", 39, 44, 9, new int[]{9,9,9}, null, true));
    assertToken(tokenList.get(7), new TokenInfo("dogs", null, "<ALPHANUM>", 45, 49, 10, new int[]{10,10,10}, null, false));
    tokenList = indexPart.get("org.apache.lucene.analysis.en.PorterStemFilter");
    assertNotNull("Expcting PorterStemFilter analysis breakdown", tokenList);
    assertEquals(tokenList.size(), 8);
    assertToken(tokenList.get(0), new TokenInfo("quick", null, "<ALPHANUM>", 4, 9, 2, new int[]{2,2,2,2}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("red", null, "<ALPHANUM>", 10, 13, 3, new int[]{3,3,3,3}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("fox", null, "<ALPHANUM>", 14, 17, 4, new int[]{4,4,4,4}, null, true));
    assertToken(tokenList.get(3), new TokenInfo("jump", null, "<ALPHANUM>", 18, 24, 5, new int[]{5,5,5,5}, null, false));
    assertToken(tokenList.get(4), new TokenInfo("over", null, "<ALPHANUM>", 25, 29, 6, new int[]{6,6,6,6}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("lazi", null, "<ALPHANUM>", 34, 38, 8, new int[]{8,8,8,8}, null, false));
    assertToken(tokenList.get(6), new TokenInfo("brown", null, "<ALPHANUM>", 39, 44, 9, new int[]{9,9,9,9}, null, true));
    assertToken(tokenList.get(7), new TokenInfo("dog", null, "<ALPHANUM>", 45, 49, 10, new int[]{10,10,10,10}, null, false));

    @SuppressWarnings({"rawtypes"})
    NamedList<List<NamedList>> queryPart = textType.get("query");
    assertNotNull("expecting a query token analysis for field type 'text'", queryPart);

    tokenList = queryPart.get("org.apache.lucene.analysis.standard.StandardTokenizer");
    assertNotNull("Expecting StandardTokenizer analysis breakdown", tokenList);
    assertEquals("Expecting StandardTokenizer to produce 2 tokens from '" + request.getQuery() + "'", 2, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("fox", null, "<ALPHANUM>", 0, 3, 1, new int[]{1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("brown", null, "<ALPHANUM>", 4, 9, 2, new int[]{2}, null, false));
    tokenList = queryPart.get("org.apache.lucene.analysis.core.LowerCaseFilter");
    assertNotNull("Expcting LowerCaseFilter analysis breakdown", tokenList);
    assertEquals(2, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("fox", null, "<ALPHANUM>", 0, 3, 1, new int[]{1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("brown", null, "<ALPHANUM>", 4, 9, 2, new int[]{2,2}, null, false));
    tokenList = queryPart.get("org.apache.lucene.analysis.core.StopFilter");
    assertNotNull("Expcting StopFilter analysis breakdown", tokenList);
    assertEquals(2, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("fox", null, "<ALPHANUM>", 0, 3, 1, new int[]{1,1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("brown", null, "<ALPHANUM>", 4, 9, 2, new int[]{2,2,2}, null, false));
    tokenList = queryPart.get("org.apache.lucene.analysis.en.PorterStemFilter");
    assertNotNull("Expcting PorterStemFilter analysis breakdown", tokenList);
    assertEquals(2, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("fox", null, "<ALPHANUM>", 0, 3, 1, new int[]{1,1,1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("brown", null, "<ALPHANUM>", 4, 9, 2, new int[]{2,2,2,2}, null, false));

    @SuppressWarnings({"rawtypes"})
    NamedList<NamedList> nameTextType = fieldTypes.get("nametext");
    assertNotNull("expecting result for field type 'nametext'", nameTextType);

    indexPart = nameTextType.get("index");
    assertNotNull("expecting an index token analysis for field type 'nametext'", indexPart);

    tokenList = indexPart.get("org.apache.lucene.analysis.core.WhitespaceTokenizer");
    assertNotNull("Expcting WhitespaceTokenizer analysis breakdown", tokenList);
    assertEquals(10, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("the", null, "word", 0, 3, 1, new int[]{1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("quick", null, "word", 4, 9, 2, new int[]{2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("red", null, "word", 10, 13, 3, new int[]{3}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("fox", null, "word", 14, 17, 4, new int[]{4}, null, true));
    assertToken(tokenList.get(4), new TokenInfo("jumped", null, "word", 18, 24, 5, new int[]{5}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("over", null, "word", 25, 29, 6, new int[]{6}, null, false));
    assertToken(tokenList.get(6), new TokenInfo("the", null, "word", 30, 33, 7, new int[]{7}, null, false));
    assertToken(tokenList.get(7), new TokenInfo("lazy", null, "word", 34, 38, 8, new int[]{8}, null, false));
    assertToken(tokenList.get(8), new TokenInfo("brown", null, "word", 39, 44, 9, new int[]{9}, null, true));
    assertToken(tokenList.get(9), new TokenInfo("dogs", null, "word", 45, 49, 10, new int[]{10}, null, false));

    queryPart = nameTextType.get("query");
    assertNotNull("expecting a query token analysis for field type 'nametext'", queryPart);
    tokenList = queryPart.get(WhitespaceTokenizer.class.getName());
    assertToken(tokenList.get(0), new TokenInfo("fox", null, "word", 0, 3, 1, new int[]{1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("brown", null, "word", 4, 9, 2, new int[]{2}, null, false));

    @SuppressWarnings({"rawtypes"})
    NamedList<NamedList> fieldNames = result.get("field_names");
    assertNotNull("field_nameds should never be null", fieldNames);

    @SuppressWarnings({"rawtypes"})
    NamedList<NamedList> whitetok = fieldNames.get("whitetok");
    assertNotNull("expecting result for field 'whitetok'", whitetok);

    indexPart = whitetok.get("index");
    assertNotNull("expecting an index token analysis for field 'whitetok'", indexPart);
    assertEquals("expecting only MockTokenizer to be applied", 1, indexPart.size());
    tokenList = indexPart.get(MockTokenizer.class.getName());
    assertNotNull("expecting only MockTokenizer to be applied", tokenList);
    assertEquals("expecting MockTokenizer to produce 10 tokens", 10, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("the", null, "word", 0, 3, 1, new int[]{1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("quick", null, "word", 4, 9, 2, new int[]{2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("red", null, "word", 10, 13, 3, new int[]{3}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("fox", null, "word", 14, 17, 4, new int[]{4}, null, true));
    assertToken(tokenList.get(4), new TokenInfo("jumped", null, "word", 18, 24, 5, new int[]{5}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("over", null, "word", 25, 29, 6, new int[]{6}, null, false));
    assertToken(tokenList.get(6), new TokenInfo("the", null, "word", 30, 33, 7, new int[]{7}, null, false));
    assertToken(tokenList.get(7), new TokenInfo("lazy", null, "word", 34, 38, 8, new int[]{8}, null, false));
    assertToken(tokenList.get(8), new TokenInfo("brown", null, "word", 39, 44, 9, new int[]{9}, null, true));
    assertToken(tokenList.get(9), new TokenInfo("dogs", null, "word", 45, 49, 10, new int[]{10}, null, false));

    queryPart = whitetok.get("query");
    assertNotNull("expecting a query token analysis for field 'whitetok'", queryPart);
    assertEquals("expecting only MockTokenizer to be applied", 1, queryPart.size());
    tokenList = queryPart.get(MockTokenizer.class.getName());
    assertNotNull("expecting only MockTokenizer to be applied", tokenList);
    assertEquals("expecting MockTokenizer to produce 2 tokens", 2, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("fox", null, "word", 0, 3, 1, new int[]{1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("brown", null, "word", 4, 9, 2, new int[]{2}, null, false));

    @SuppressWarnings({"rawtypes"})
    NamedList<NamedList> keywordtok = fieldNames.get("keywordtok");
    assertNotNull("expecting result for field 'keywordtok'", keywordtok);

    indexPart = keywordtok.get("index");
    assertNotNull("expecting an index token analysis for field 'keywordtok'", indexPart);
    assertEquals("expecting only MockTokenizer to be applied", 1, indexPart.size());
    tokenList = indexPart.get(MockTokenizer.class.getName());
    assertNotNull("expecting only MockTokenizer to be applied", tokenList);
    assertEquals("expecting MockTokenizer to produce 1 token", 1, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("the quick red fox jumped over the lazy brown dogs", null, "word", 0, 49, 1, new int[]{1}, null, false));

    queryPart = keywordtok.get("query");
    assertNotNull("expecting a query token analysis for field 'keywordtok'", queryPart);
    assertEquals("expecting only MockTokenizer to be applied", 1, queryPart.size());
    tokenList = queryPart.get(MockTokenizer.class.getName());
    assertNotNull("expecting only MockTokenizer to be applied", tokenList);
    assertEquals("expecting MockTokenizer to produce 1 token", 1, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("fox brown", null, "word", 0, 9, 1, new int[]{1}, null, false));

  }

  @Test
  public void testCharFilterAnalysis() throws Exception {

    FieldAnalysisRequest request = new FieldAnalysisRequest();
    request.addFieldType("charfilthtmlmap");
    request.setFieldValue("<html><body>whátëvêr</body></html>");
    request.setShowMatch(false);

    @SuppressWarnings({"rawtypes"})
    NamedList<NamedList> result = handler.handleAnalysisRequest(request, h.getCore().getLatestSchema());
    assertTrue("result is null and it shouldn't be", result != null);

    @SuppressWarnings({"unchecked", "rawtypes"})
    NamedList<NamedList> fieldTypes = result.get("field_types");
    assertNotNull("field_types should never be null", fieldTypes);
    @SuppressWarnings({"unchecked", "rawtypes"})
    NamedList<NamedList> textType = fieldTypes.get("charfilthtmlmap");
    assertNotNull("expecting result for field type 'charfilthtmlmap'", textType);

    @SuppressWarnings({"rawtypes"})
    NamedList indexPart = textType.get("index");
    assertNotNull("expecting an index token analysis for field type 'charfilthtmlmap'", indexPart);

    assertEquals("\n\nwhátëvêr\n\n", indexPart.get("org.apache.lucene.analysis.charfilter.HTMLStripCharFilter"));
    assertEquals("\n\nwhatever\n\n", indexPart.get("org.apache.lucene.analysis.charfilter.MappingCharFilter"));

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<NamedList> tokenList = (List<NamedList>)indexPart.get(MockTokenizer.class.getName());
    assertNotNull("Expecting MockTokenizer analysis breakdown", tokenList);
    assertEquals(tokenList.size(), 1);
    assertToken(tokenList.get(0), new TokenInfo("whatever", null, "word", 12, 20, 1, new int[]{1}, null, false));
  }

  @Test
  public void testPositionHistoryWithWDGF() throws Exception {

    FieldAnalysisRequest request = new FieldAnalysisRequest();
    request.addFieldType("skutype1");
    request.setFieldValue("hi, 3456-12 a Test");
    request.setShowMatch(false);

    @SuppressWarnings({"rawtypes"})
    NamedList<NamedList> result = handler.handleAnalysisRequest(request, h.getCore().getLatestSchema());
    assertTrue("result is null and it shouldn't be", result != null);

    @SuppressWarnings({"unchecked", "rawtypes"})
    NamedList<NamedList> fieldTypes = result.get("field_types");
    assertNotNull("field_types should never be null", fieldTypes);
    @SuppressWarnings({"unchecked", "rawtypes"})
    NamedList<NamedList> textType = fieldTypes.get("skutype1");
    assertNotNull("expecting result for field type 'skutype1'", textType);

    @SuppressWarnings({"unchecked", "rawtypes"})
    NamedList<List<NamedList>> indexPart = textType.get("index");
    assertNotNull("expecting an index token analysis for field type 'skutype1'", indexPart);

    @SuppressWarnings({"rawtypes"})
    List<NamedList> tokenList = indexPart.get(MockTokenizer.class.getName());
    assertNotNull("Expcting MockTokenizer analysis breakdown", tokenList);
    assertEquals(4, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("hi,", null, "word", 0, 3, 1, new int[]{1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("3456-12", null, "word", 4, 11, 2, new int[]{2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("a", null, "word", 12, 13, 3, new int[]{3}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("Test", null, "word", 14, 18, 4, new int[]{4}, null, false));
    tokenList = indexPart.get("org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter");
    assertNotNull("Expcting WordDelimiterGraphFilter analysis breakdown", tokenList);
    assertEquals(6, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("hi", null, "word", 0, 2, 1, new int[]{1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("345612", null, "word", 4, 11, 2, new int[]{2,2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("3456", null, "word", 4, 8, 2, new int[]{2,2}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("12", null, "word", 9, 11, 3, new int[]{2,3}, null, false));
    assertToken(tokenList.get(4), new TokenInfo("a", null, "word", 12, 13, 4, new int[]{3,4}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("Test", null, "word", 14, 18, 5, new int[]{4,5}, null, false));
    tokenList = indexPart.get("org.apache.lucene.analysis.core.LowerCaseFilter");
    assertNotNull("Expcting LowerCaseFilter analysis breakdown", tokenList);
    assertEquals(6, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("hi", null, "word", 0, 2, 1, new int[]{1,1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("345612", null, "word", 4, 11, 2, new int[]{2,2,2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("3456", null, "word", 4, 8, 2, new int[]{2,2,2}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("12", null, "word", 9, 11, 3, new int[]{2,3,3}, null, false));
    assertToken(tokenList.get(4), new TokenInfo("a", null, "word", 12, 13, 4, new int[]{3,4,4}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("test", null, "word", 14, 18, 5, new int[]{4,5,5}, null, false));
  }

  @Test
  public void testSpatial() throws Exception {
    FieldAnalysisRequest request = new FieldAnalysisRequest();
    request.addFieldType("location_rpt");
    request.setFieldValue("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))");

    @SuppressWarnings({"rawtypes"})
    NamedList<NamedList> result = handler.handleAnalysisRequest(request, h.getCore().getLatestSchema());
    @SuppressWarnings({"unchecked", "rawtypes"})
    NamedList<List<NamedList>> tokens = (NamedList<List<NamedList>>)
        ((NamedList)result.get("field_types").get("location_rpt")).get("index");
    @SuppressWarnings({"rawtypes"})
    List<NamedList> tokenList = tokens.get("org.apache.lucene.spatial.prefix.PrefixTreeStrategy$ShapeTokenStream");


    List<String> vals = new ArrayList<>(tokenList.size());
    for(@SuppressWarnings({"rawtypes"})NamedList v : tokenList) {
      vals.add( (String)v.get("text") );
    }
    Collections.sort(vals);
    assertEquals( "[s, s7, s7w, s7w1+, s9, s9v, s9v2+, sp, spp, spp5+, sv, svk, svk6+]", vals.toString() );
  }

  @Test //See SOLR-8460
  public void testCustomAttribute() throws Exception {
    FieldAnalysisRequest request = new FieldAnalysisRequest();
    request.addFieldType("skutype1");
    request.setFieldValue("hi, 3456-12 a Test");
    request.setShowMatch(false);
    FieldType fieldType = new TextField();
    Analyzer analyzer = new TokenizerChain(
        new TokenizerFactory(Collections.emptyMap()) {
          @Override
          public Tokenizer create(AttributeFactory factory) {
            return new CustomTokenizer(factory);
          }
        },
        new TokenFilterFactory[] {
            new TokenFilterFactory(Collections.emptyMap()) {
              @Override
              public TokenStream create(TokenStream input) {
                return new CustomTokenFilter(input);
              }
            }
        }
    );
    fieldType.setIndexAnalyzer(analyzer);

    @SuppressWarnings({"rawtypes"})
    NamedList<NamedList> result = handler.analyzeValues(request, fieldType, "fieldNameUnused");
    // just test that we see "900" in the flags attribute here
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<NamedList> tokenInfoList = (List<NamedList>) result.findRecursive("index", CustomTokenFilter.class.getName());
    // '1' from CustomTokenFilter plus 900 from CustomFlagsAttributeImpl.
    assertEquals(901, tokenInfoList.get(0).get("org.apache.lucene.analysis.tokenattributes.FlagsAttribute#flags"));
  }

  @Test(expected = Exception.class)
  public void testNoDefaultField() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.Q, "fox brown");
    SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), params);
    handler.resolveAnalysisRequest(req);
  }

  /** A custom impl of a standard attribute impl; test this instance is used. */
  public class CustomFlagsAttributeImpl extends FlagsAttributeImpl {
    @Override
    public void setFlags(int flags) {
      super.setFlags(900 + flags);//silly modification
    }
  }

  private class CustomTokenizer extends Tokenizer {
    CharTermAttribute charAtt;
    FlagsAttribute customAtt;
    boolean sentOneToken;

    public CustomTokenizer(AttributeFactory factory) {
      super(factory);
      addAttributeImpl(new CustomFlagsAttributeImpl());
      charAtt = addAttribute(CharTermAttribute.class);
      customAtt = addAttribute(FlagsAttribute.class);
    }

    @Override
    public void reset() throws IOException {
      sentOneToken = false;
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (sentOneToken) {
        return false;
      }
      sentOneToken = true;
      clearAttributes();
      charAtt.append("firstToken");
      return true;
    }
  }

  private class CustomTokenFilter extends TokenFilter {
    FlagsAttribute flagAtt;

    public CustomTokenFilter(TokenStream input) {
      super(input);
      flagAtt = getAttribute(FlagsAttribute.class);
      if (flagAtt == null) {
        throw new IllegalStateException("FlagsAttribute should have been added already");
      }
      if (!(flagAtt instanceof CustomFlagsAttributeImpl)) {
        throw new IllegalStateException("FlagsAttribute should be our custom " + CustomFlagsAttributeImpl.class);
      }
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        flagAtt.setFlags(1);
        return true;
      } else {
        return false;
      }
    }
  }
}
