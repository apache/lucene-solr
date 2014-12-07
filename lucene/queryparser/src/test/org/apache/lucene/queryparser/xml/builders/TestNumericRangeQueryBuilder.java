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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.BeforeClass;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class TestNumericRangeQueryBuilder extends LuceneTestCase {

  private static FieldTypes fieldTypes;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = newIndexWriter(dir);
    org.apache.lucene.document.Document doc = w.newDocument();
    doc.addInt("AGE_INT", 14);
    doc.addLong("AGE_LONG", 14L);
    doc.addFloat("AGE_FLOAT", 14F);
    doc.addDouble("AGE_DOUBLE", 14D);
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();
    w.close();
    r.close();
    dir.close();
  }

  public void testGetFilterHandleNumericParseErrorStrict() throws Exception {
    NumericRangeQueryBuilder filterBuilder = new NumericRangeQueryBuilder();

    String xml = "<NumericRangeQuery fieldName='AGE' type='int' lowerTerm='-1' upperTerm='NaN'/>";
    Document doc = getDocumentFromString(xml);
    try {
      filterBuilder.getQuery(fieldTypes, doc.getDocumentElement());
    } catch (ParserException e) {
      return;
    }
    fail("Expected to throw " + ParserException.class);
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  public void testGetFilterInt() throws Exception {
    NumericRangeQueryBuilder filterBuilder = new NumericRangeQueryBuilder();

    String xml = "<NumericRangeQuery fieldName='AGE_INT' type='int' lowerTerm='-1' upperTerm='10'/>";
    Document doc = getDocumentFromString(xml);
    Query filter = filterBuilder.getQuery(fieldTypes, doc.getDocumentElement());
    assertTrue(filter instanceof ConstantScoreQuery);
    assertTrue(filter.toString(), filter.toString().contains("[-1 TO 10]"));

    TermRangeFilter numRangeFilter = (TermRangeFilter) ((ConstantScoreQuery) filter).getFilter();
    assertEquals("AGE_INT", numRangeFilter.getField());
    assertTrue(numRangeFilter.includesLower());
    assertTrue(numRangeFilter.includesUpper());

    String xml2 = "<NumericRangeQuery fieldName='AGE_INT' type='int' lowerTerm='-1' upperTerm='10' includeUpper='false'/>";
    Document doc2 = getDocumentFromString(xml2);
    Query filter2 = filterBuilder.getQuery(fieldTypes, doc2.getDocumentElement());
    assertTrue(filter2 instanceof ConstantScoreQuery);
    assertTrue(filter2.toString(), filter2.toString().contains("[-1 TO 10}"));

    TermRangeFilter numRangeFilter2 = (TermRangeFilter) ((ConstantScoreQuery) filter2).getFilter();
    assertEquals("AGE_INT", numRangeFilter2.getField());
    assertTrue(numRangeFilter2.includesLower());
    assertFalse(numRangeFilter2.includesUpper());
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  public void testGetFilterLong() throws Exception {
    NumericRangeQueryBuilder filterBuilder = new NumericRangeQueryBuilder();

    String xml = "<NumericRangeQuery fieldName='AGE_LONG' type='LoNg' lowerTerm='-2321' upperTerm='60000000'/>";
    Document doc = getDocumentFromString(xml);
    Query filter = filterBuilder.getQuery(fieldTypes, doc.getDocumentElement());
    assertTrue(filter instanceof ConstantScoreQuery);
    assertTrue(filter.toString(), filter.toString().contains("[-2321 TO 60000000]"));

    TermRangeFilter numRangeFilter = (TermRangeFilter) ((ConstantScoreQuery) filter).getFilter();
    assertEquals("AGE_LONG", numRangeFilter.getField());
    assertTrue(numRangeFilter.includesLower());
    assertTrue(numRangeFilter.includesUpper());


    String xml2 = "<NumericRangeQuery fieldName='AGE_LONG' type='LoNg' lowerTerm='-2321' upperTerm='60000000' includeUpper='false'/>";
    Document doc2 = getDocumentFromString(xml2);
    Query filter2 = filterBuilder.getQuery(fieldTypes, doc2.getDocumentElement());
    assertTrue(filter2 instanceof ConstantScoreQuery);
    assertTrue(filter2.toString(), filter2.toString().contains("[-2321 TO 60000000}"));

    TermRangeFilter numRangeFilter2 = (TermRangeFilter) ((ConstantScoreQuery) filter2).getFilter();
    assertEquals("AGE_LONG", numRangeFilter2.getField());
    assertTrue(numRangeFilter2.includesLower());
    assertFalse(numRangeFilter2.includesUpper());
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  public void testGetFilterDouble() throws Exception {
    NumericRangeQueryBuilder filterBuilder = new NumericRangeQueryBuilder();

    String xml = "<NumericRangeQuery fieldName='AGE_DOUBLE' type='doubLe' lowerTerm='-23.21' upperTerm='60000.00023'/>";
    Document doc = getDocumentFromString(xml);

    Query filter = filterBuilder.getQuery(fieldTypes, doc.getDocumentElement());
    assertTrue(filter instanceof ConstantScoreQuery);
    assertTrue(filter.toString(), filter.toString().contains("[-23.21 TO 60000.00023]"));

    TermRangeFilter numRangeFilter = (TermRangeFilter) ((ConstantScoreQuery) filter).getFilter();
    assertEquals("AGE_DOUBLE", numRangeFilter.getField());
    assertTrue(numRangeFilter.includesLower());
    assertTrue(numRangeFilter.includesUpper());


    String xml2 = "<NumericRangeQuery fieldName='AGE_DOUBLE' type='doubLe' lowerTerm='-23.21' upperTerm='60000.00023' includeUpper='false'/>";
    Document doc2 = getDocumentFromString(xml2);
    Query filter2 = filterBuilder.getQuery(fieldTypes, doc2.getDocumentElement());
    assertTrue(filter2 instanceof ConstantScoreQuery);
    assertTrue(filter2.toString(), filter2.toString().contains("[-23.21 TO 60000.00023}"));

    TermRangeFilter numRangeFilter2 = (TermRangeFilter) ((ConstantScoreQuery) filter2).getFilter();
    assertEquals("AGE_DOUBLE", numRangeFilter2.getField());
    assertTrue(numRangeFilter2.includesLower());
    assertFalse(numRangeFilter2.includesUpper());
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  public void testGetFilterFloat() throws Exception {
    NumericRangeQueryBuilder filterBuilder = new NumericRangeQueryBuilder();

    String xml = "<NumericRangeQuery fieldName='AGE_FLOAT' type='FLOAT' lowerTerm='-2.321432' upperTerm='32432.23'/>";
    Document doc = getDocumentFromString(xml);

    Query filter = filterBuilder.getQuery(fieldTypes, doc.getDocumentElement());
    assertTrue(filter instanceof ConstantScoreQuery);
    assertTrue(filter.toString(), filter.toString().contains("[-2.321432 TO 32432.23]"));

    TermRangeFilter numRangeFilter = (TermRangeFilter) ((ConstantScoreQuery) filter).getFilter();
    assertEquals("AGE_FLOAT", numRangeFilter.getField());
    assertTrue(numRangeFilter.includesLower());
    assertTrue(numRangeFilter.includesUpper());

    String xml2 = "<NumericRangeQuery fieldName='AGE_FLOAT' type='FLOAT' lowerTerm='-2.321432' upperTerm='32432.23' includeUpper='false' precisionStep='2' />";
    Document doc2 = getDocumentFromString(xml2);

    Query filter2 = filterBuilder.getQuery(fieldTypes, doc2.getDocumentElement());
    assertTrue(filter2 instanceof ConstantScoreQuery);
    assertTrue(filter2.toString(), filter2.toString().contains("[-2.321432 TO 32432.23}"));

    TermRangeFilter numRangeFilter2 = (TermRangeFilter) ((ConstantScoreQuery) filter2).getFilter();
    assertEquals("AGE_FLOAT", numRangeFilter2.getField());
    assertTrue(numRangeFilter2.includesLower());
    assertFalse(numRangeFilter2.includesUpper());
  }

  private static Document getDocumentFromString(String str)
      throws SAXException, IOException, ParserConfigurationException {
    InputStream is = new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(is);
    is.close();
    return doc;
  }

}
