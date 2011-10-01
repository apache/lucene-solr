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

package org.apache.solr.handler;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.solr.common.params.AnalysisParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.client.solrj.request.FieldAnalysisRequest;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

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
  }

  /**
   * Tests the {@link FieldAnalysisRequestHandler#handleAnalysisRequest(org.apache.solr.client.solrj.request.FieldAnalysisRequest,
   * org.apache.solr.schema.IndexSchema)}
   */
  @Test
  public void testHandleAnalysisRequest() throws Exception {

    FieldAnalysisRequest request = new FieldAnalysisRequest();
    request.addFieldName("whitetok");
    request.addFieldName("keywordtok");
    request.addFieldType("text");
    request.addFieldType("nametext");
    request.setFieldValue("the quick red fox jumped over the lazy brown dogs");
    request.setQuery("fox brown");
    request.setShowMatch(true);

    NamedList<NamedList> result = handler.handleAnalysisRequest(request, h.getCore().getSchema());
    assertTrue("result is null and it shouldn't be", result != null);

    NamedList<NamedList> fieldTypes = result.get("field_types");
    assertNotNull("field_types should never be null", fieldTypes);
    NamedList<NamedList> textType = fieldTypes.get("text");
    assertNotNull("expecting result for field type 'text'", textType);

    NamedList<List<NamedList>> indexPart = textType.get("index");
    assertNotNull("expecting an index token analysis for field type 'text'", indexPart);

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
    tokenList = indexPart.get("org.apache.lucene.analysis.standard.StandardFilter");
    assertNotNull("Expcting StandardFilter analysis breakdown", tokenList);
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
    tokenList = indexPart.get("org.apache.lucene.analysis.core.LowerCaseFilter");
    assertNotNull("Expcting LowerCaseFilter analysis breakdown", tokenList);
    assertEquals(tokenList.size(), 10);
    assertToken(tokenList.get(0), new TokenInfo("the", null, "<ALPHANUM>", 0, 3, 1, new int[]{1,1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("quick", null, "<ALPHANUM>", 4, 9, 2, new int[]{2,2,2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("red", null, "<ALPHANUM>", 10, 13, 3, new int[]{3,3,3}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("fox", null, "<ALPHANUM>", 14, 17, 4, new int[]{4,4,4}, null, true));
    assertToken(tokenList.get(4), new TokenInfo("jumped", null, "<ALPHANUM>", 18, 24, 5, new int[]{5,5,5}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("over", null, "<ALPHANUM>", 25, 29, 6, new int[]{6,6,6}, null, false));
    assertToken(tokenList.get(6), new TokenInfo("the", null, "<ALPHANUM>", 30, 33, 7, new int[]{7,7,7}, null, false));
    assertToken(tokenList.get(7), new TokenInfo("lazy", null, "<ALPHANUM>", 34, 38, 8, new int[]{8,8,8}, null, false));
    assertToken(tokenList.get(8), new TokenInfo("brown", null, "<ALPHANUM>", 39, 44, 9, new int[]{9,9,9}, null, true));
    assertToken(tokenList.get(9), new TokenInfo("dogs", null, "<ALPHANUM>", 45, 49, 10, new int[]{10,10,10}, null, false));
    tokenList = indexPart.get("org.apache.lucene.analysis.core.StopFilter");
    assertNotNull("Expcting StopFilter analysis breakdown", tokenList);
    assertEquals(tokenList.size(), 8);
    assertToken(tokenList.get(0), new TokenInfo("quick", null, "<ALPHANUM>", 4, 9, 1, new int[]{2,2,2,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("red", null, "<ALPHANUM>", 10, 13, 2, new int[]{3,3,3,2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("fox", null, "<ALPHANUM>", 14, 17, 3, new int[]{4,4,4,3}, null, true));
    assertToken(tokenList.get(3), new TokenInfo("jumped", null, "<ALPHANUM>", 18, 24, 4, new int[]{5,5,5,4}, null, false));
    assertToken(tokenList.get(4), new TokenInfo("over", null, "<ALPHANUM>", 25, 29, 5, new int[]{6,6,6,5}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("lazy", null, "<ALPHANUM>", 34, 38, 6, new int[]{8,8,8,6}, null, false));
    assertToken(tokenList.get(6), new TokenInfo("brown", null, "<ALPHANUM>", 39, 44, 7, new int[]{9,9,9,7}, null, true));
    assertToken(tokenList.get(7), new TokenInfo("dogs", null, "<ALPHANUM>", 45, 49, 8, new int[]{10,10,10,8}, null, false));
    tokenList = indexPart.get("org.apache.lucene.analysis.en.PorterStemFilter");
    assertNotNull("Expcting PorterStemFilter analysis breakdown", tokenList);
    assertEquals(tokenList.size(), 8);
    assertToken(tokenList.get(0), new TokenInfo("quick", null, "<ALPHANUM>", 4, 9, 1, new int[]{2,2,2,1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("red", null, "<ALPHANUM>", 10, 13, 2, new int[]{3,3,3,2,2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("fox", null, "<ALPHANUM>", 14, 17, 3, new int[]{4,4,4,3,3}, null, true));
    assertToken(tokenList.get(3), new TokenInfo("jump", null, "<ALPHANUM>", 18, 24, 4, new int[]{5,5,5,4,4}, null, false));
    assertToken(tokenList.get(4), new TokenInfo("over", null, "<ALPHANUM>", 25, 29, 5, new int[]{6,6,6,5,5}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("lazi", null, "<ALPHANUM>", 34, 38, 6, new int[]{8,8,8,6,6}, null, false));
    assertToken(tokenList.get(6), new TokenInfo("brown", null, "<ALPHANUM>", 39, 44, 7, new int[]{9,9,9,7,7}, null, true));
    assertToken(tokenList.get(7), new TokenInfo("dog", null, "<ALPHANUM>", 45, 49, 8, new int[]{10,10,10,8,8}, null, false));

    NamedList<List<NamedList>> queryPart = textType.get("query");
    assertNotNull("expecting a query token analysis for field type 'text'", queryPart);

    tokenList = queryPart.get("org.apache.lucene.analysis.standard.StandardTokenizer");
    assertNotNull("Expecting StandardTokenizer analysis breakdown", tokenList);
    assertEquals("Expecting StandardTokenizer to produce 2 tokens from '" + request.getQuery() + "'", 2, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("fox", null, "<ALPHANUM>", 0, 3, 1, new int[]{1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("brown", null, "<ALPHANUM>", 4, 9, 2, new int[]{2}, null, false));
    tokenList = queryPart.get("org.apache.lucene.analysis.standard.StandardFilter");
    assertNotNull("Expcting StandardFilter analysis breakdown", tokenList);
    assertEquals(2, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("fox", null, "<ALPHANUM>", 0, 3, 1, new int[]{1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("brown", null, "<ALPHANUM>", 4, 9, 2, new int[]{2,2}, null, false));
    tokenList = queryPart.get("org.apache.lucene.analysis.core.LowerCaseFilter");
    assertNotNull("Expcting LowerCaseFilter analysis breakdown", tokenList);
    assertEquals(2, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("fox", null, "<ALPHANUM>", 0, 3, 1, new int[]{1,1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("brown", null, "<ALPHANUM>", 4, 9, 2, new int[]{2,2,2}, null, false));
    tokenList = queryPart.get("org.apache.lucene.analysis.core.StopFilter");
    assertNotNull("Expcting StopFilter analysis breakdown", tokenList);
    assertEquals(2, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("fox", null, "<ALPHANUM>", 0, 3, 1, new int[]{1,1,1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("brown", null, "<ALPHANUM>", 4, 9, 2, new int[]{2,2,2,2}, null, false));
    tokenList = queryPart.get("org.apache.lucene.analysis.en.PorterStemFilter");
    assertNotNull("Expcting PorterStemFilter analysis breakdown", tokenList);
    assertEquals(2, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("fox", null, "<ALPHANUM>", 0, 3, 1, new int[]{1,1,1,1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("brown", null, "<ALPHANUM>", 4, 9, 2, new int[]{2,2,2,2,2}, null, false));

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

    NamedList<NamedList> fieldNames = result.get("field_names");
    assertNotNull("field_nameds should never be null", fieldNames);

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

    NamedList<NamedList> result = handler.handleAnalysisRequest(request, h.getCore().getSchema());
    assertTrue("result is null and it shouldn't be", result != null);

    NamedList<NamedList> fieldTypes = result.get("field_types");
    assertNotNull("field_types should never be null", fieldTypes);
    NamedList<NamedList> textType = fieldTypes.get("charfilthtmlmap");
    assertNotNull("expecting result for field type 'charfilthtmlmap'", textType);

    NamedList indexPart = textType.get("index");
    assertNotNull("expecting an index token analysis for field type 'charfilthtmlmap'", indexPart);
    
    assertEquals("  whátëvêr  ", indexPart.get("org.apache.lucene.analysis.charfilter.HTMLStripCharFilter"));
    assertEquals("  whatever  ", indexPart.get("org.apache.lucene.analysis.charfilter.MappingCharFilter"));

    List<NamedList> tokenList = (List<NamedList>)indexPart.get(MockTokenizer.class.getName());
    assertNotNull("Expecting MockTokenizer analysis breakdown", tokenList);
    assertEquals(tokenList.size(), 1);
    assertToken(tokenList.get(0), new TokenInfo("whatever", null, "word", 12, 20, 1, new int[]{1}, null, false));
  }
  
  @Test
  public void testPositionHistoryWithWDF() throws Exception {

    FieldAnalysisRequest request = new FieldAnalysisRequest();
    request.addFieldType("skutype1");
    request.setFieldValue("hi, 3456-12 a Test");
    request.setShowMatch(false);

    NamedList<NamedList> result = handler.handleAnalysisRequest(request, h.getCore().getSchema());
    assertTrue("result is null and it shouldn't be", result != null);

    NamedList<NamedList> fieldTypes = result.get("field_types");
    assertNotNull("field_types should never be null", fieldTypes);
    NamedList<NamedList> textType = fieldTypes.get("skutype1");
    assertNotNull("expecting result for field type 'skutype1'", textType);

    NamedList<List<NamedList>> indexPart = textType.get("index");
    assertNotNull("expecting an index token analysis for field type 'skutype1'", indexPart);

    List<NamedList> tokenList = indexPart.get(MockTokenizer.class.getName());
    assertNotNull("Expcting MockTokenizer analysis breakdown", tokenList);
    assertEquals(4, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("hi,", null, "word", 0, 3, 1, new int[]{1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("3456-12", null, "word", 4, 11, 2, new int[]{2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("a", null, "word", 12, 13, 3, new int[]{3}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("Test", null, "word", 14, 18, 4, new int[]{4}, null, false));
    tokenList = indexPart.get("org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter");
    assertNotNull("Expcting WordDelimiterFilter analysis breakdown", tokenList);
    assertEquals(6, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("hi", null, "word", 0, 2, 1, new int[]{1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("3456", null, "word", 4, 8, 2, new int[]{2,2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("12", null, "word", 9, 11, 3, new int[]{2,3}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("345612", null, "word", 4, 11, 3, new int[]{2,3}, null, false));
    assertToken(tokenList.get(4), new TokenInfo("a", null, "word", 12, 13, 4, new int[]{3,4}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("Test", null, "word", 14, 18, 5, new int[]{4,5}, null, false));
    tokenList = indexPart.get("org.apache.lucene.analysis.core.LowerCaseFilter");
    assertNotNull("Expcting LowerCaseFilter analysis breakdown", tokenList);
    assertEquals(6, tokenList.size());
    assertToken(tokenList.get(0), new TokenInfo("hi", null, "word", 0, 2, 1, new int[]{1,1,1}, null, false));
    assertToken(tokenList.get(1), new TokenInfo("3456", null, "word", 4, 8, 2, new int[]{2,2,2}, null, false));
    assertToken(tokenList.get(2), new TokenInfo("12", null, "word", 9, 11, 3, new int[]{2,3,3}, null, false));
    assertToken(tokenList.get(3), new TokenInfo("345612", null, "word", 4, 11, 3, new int[]{2,3,3}, null, false));
    assertToken(tokenList.get(4), new TokenInfo("a", null, "word", 12, 13, 4, new int[]{3,4,4}, null, false));
    assertToken(tokenList.get(5), new TokenInfo("test", null, "word", 14, 18, 5, new int[]{4,5,5}, null, false));
  }
  
}
