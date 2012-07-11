package org.apache.lucene.queryparser.xml.builders;

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

import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.queryparser.xml.ParserException;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TestNumericRangeQueryBuilder extends LuceneTestCase {

  public void testGetFilterHandleNumericParseErrorStrict() throws Exception {
    NumericRangeQueryBuilder filterBuilder = new NumericRangeQueryBuilder();

    String xml = "<NumericRangeQuery fieldName='AGE' type='int' lowerTerm='-1' upperTerm='NaN'/>";
    Document doc = getDocumentFromString(xml);
    try {
      filterBuilder.getQuery(doc.getDocumentElement());
    } catch (ParserException e) {
      return;
    }
    fail("Expected to throw " + ParserException.class);
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  public void testGetFilterInt() throws Exception {
    NumericRangeQueryBuilder filterBuilder = new NumericRangeQueryBuilder();

    String xml = "<NumericRangeQuery fieldName='AGE' type='int' lowerTerm='-1' upperTerm='10'/>";
    Document doc = getDocumentFromString(xml);
    Query filter = filterBuilder.getQuery(doc.getDocumentElement());
    assertTrue(filter instanceof NumericRangeQuery<?>);

    NumericRangeQuery<Integer> numRangeFilter = (NumericRangeQuery<Integer>) filter;
    assertEquals(Integer.valueOf(-1), numRangeFilter.getMin());
    assertEquals(Integer.valueOf(10), numRangeFilter.getMax());
    assertEquals("AGE", numRangeFilter.getField());
    assertTrue(numRangeFilter.includesMin());
    assertTrue(numRangeFilter.includesMax());

    String xml2 = "<NumericRangeQuery fieldName='AGE' type='int' lowerTerm='-1' upperTerm='10' includeUpper='false'/>";
    Document doc2 = getDocumentFromString(xml2);
    Query filter2 = filterBuilder.getQuery(doc2.getDocumentElement());
    assertTrue(filter2 instanceof NumericRangeQuery<?>);

    NumericRangeQuery<Integer> numRangeFilter2 = (NumericRangeQuery) filter2;
    assertEquals(Integer.valueOf(-1), numRangeFilter2.getMin());
    assertEquals(Integer.valueOf(10), numRangeFilter2.getMax());
    assertEquals("AGE", numRangeFilter2.getField());
    assertTrue(numRangeFilter2.includesMin());
    assertFalse(numRangeFilter2.includesMax());
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  public void testGetFilterLong() throws Exception {
    NumericRangeQueryBuilder filterBuilder = new NumericRangeQueryBuilder();

    String xml = "<NumericRangeQuery fieldName='AGE' type='LoNg' lowerTerm='-2321' upperTerm='60000000'/>";
    Document doc = getDocumentFromString(xml);
    Query filter = filterBuilder.getQuery(doc.getDocumentElement());
    assertTrue(filter instanceof NumericRangeQuery<?>);
    NumericRangeQuery<Long> numRangeFilter = (NumericRangeQuery) filter;
    assertEquals(Long.valueOf(-2321L), numRangeFilter.getMin());
    assertEquals(Long.valueOf(60000000L), numRangeFilter.getMax());
    assertEquals("AGE", numRangeFilter.getField());
    assertTrue(numRangeFilter.includesMin());
    assertTrue(numRangeFilter.includesMax());

    String xml2 = "<NumericRangeQuery fieldName='AGE' type='LoNg' lowerTerm='-2321' upperTerm='60000000' includeUpper='false'/>";
    Document doc2 = getDocumentFromString(xml2);
    Query filter2 = filterBuilder.getQuery(doc2.getDocumentElement());
    assertTrue(filter2 instanceof NumericRangeQuery<?>);

    NumericRangeQuery<Long> numRangeFilter2 = (NumericRangeQuery) filter2;
    assertEquals(Long.valueOf(-2321L), numRangeFilter2.getMin());
    assertEquals(Long.valueOf(60000000L), numRangeFilter2.getMax());
    assertEquals("AGE", numRangeFilter2.getField());
    assertTrue(numRangeFilter2.includesMin());
    assertFalse(numRangeFilter2.includesMax());
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  public void testGetFilterDouble() throws Exception {
    NumericRangeQueryBuilder filterBuilder = new NumericRangeQueryBuilder();

    String xml = "<NumericRangeQuery fieldName='AGE' type='doubLe' lowerTerm='-23.21' upperTerm='60000.00023'/>";
    Document doc = getDocumentFromString(xml);

    Query filter = filterBuilder.getQuery(doc.getDocumentElement());
    assertTrue(filter instanceof NumericRangeQuery<?>);

    NumericRangeQuery<Double> numRangeFilter = (NumericRangeQuery) filter;
    assertEquals(Double.valueOf(-23.21d), numRangeFilter.getMin());
    assertEquals(Double.valueOf(60000.00023d), numRangeFilter.getMax());
    assertEquals("AGE", numRangeFilter.getField());
    assertTrue(numRangeFilter.includesMin());
    assertTrue(numRangeFilter.includesMax());

    String xml2 = "<NumericRangeQuery fieldName='AGE' type='doubLe' lowerTerm='-23.21' upperTerm='60000.00023' includeUpper='false'/>";
    Document doc2 = getDocumentFromString(xml2);
    Query filter2 = filterBuilder.getQuery(doc2.getDocumentElement());
    assertTrue(filter2 instanceof NumericRangeQuery<?>);

    NumericRangeQuery<Double> numRangeFilter2 = (NumericRangeQuery) filter2;
    assertEquals(Double.valueOf(-23.21d), numRangeFilter2.getMin());
    assertEquals(Double.valueOf(60000.00023d), numRangeFilter2.getMax());
    assertEquals("AGE", numRangeFilter2.getField());
    assertTrue(numRangeFilter2.includesMin());
    assertFalse(numRangeFilter2.includesMax());
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  public void testGetFilterFloat() throws Exception {
    NumericRangeQueryBuilder filterBuilder = new NumericRangeQueryBuilder();

    String xml = "<NumericRangeQuery fieldName='AGE' type='FLOAT' lowerTerm='-2.321432' upperTerm='32432.23'/>";
    Document doc = getDocumentFromString(xml);

    Query filter = filterBuilder.getQuery(doc.getDocumentElement());
    assertTrue(filter instanceof NumericRangeQuery<?>);

    NumericRangeQuery<Float> numRangeFilter = (NumericRangeQuery) filter;
    assertEquals(Float.valueOf(-2.321432f), numRangeFilter.getMin());
    assertEquals(Float.valueOf(32432.23f), numRangeFilter.getMax());
    assertEquals("AGE", numRangeFilter.getField());
    assertTrue(numRangeFilter.includesMin());
    assertTrue(numRangeFilter.includesMax());

    String xml2 = "<NumericRangeQuery fieldName='AGE' type='FLOAT' lowerTerm='-2.321432' upperTerm='32432.23' includeUpper='false' precisionStep='2' />";
    Document doc2 = getDocumentFromString(xml2);

    Query filter2 = filterBuilder.getQuery(doc2.getDocumentElement());
    assertTrue(filter2 instanceof NumericRangeQuery<?>);

    NumericRangeQuery<Float> numRangeFilter2 = (NumericRangeQuery) filter2;
    assertEquals(Float.valueOf(-2.321432f), numRangeFilter2.getMin());
    assertEquals(Float.valueOf(32432.23f), numRangeFilter2.getMax());
    assertEquals("AGE", numRangeFilter2.getField());
    assertTrue(numRangeFilter2.includesMin());
    assertFalse(numRangeFilter2.includesMax());
  }

  private static Document getDocumentFromString(String str)
      throws SAXException, IOException, ParserConfigurationException {
    InputStream is = new ByteArrayInputStream(str.getBytes("UTF-8"));
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(is);
    is.close();
    return doc;
  }

}
