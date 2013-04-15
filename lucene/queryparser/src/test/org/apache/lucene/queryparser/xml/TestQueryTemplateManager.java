package org.apache.lucene.queryparser.xml;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;


/**
 * This class illustrates how form input (such as from a web page or Swing gui) can be
 * turned into Lucene queries using a choice of XSL templates for different styles of queries.
 */
public class TestQueryTemplateManager extends LuceneTestCase {

  private CoreParser builder;
  private Analyzer analyzer;
  private IndexSearcher searcher;
  private IndexReader reader;
  private Directory dir;

  //A collection of documents' field values for use in our tests
  String docFieldValues[] =
      {
          "artist=Jeff Buckley \talbum=Grace \treleaseDate=1999 \tgenre=rock",
          "artist=Fugazi \talbum=Repeater \treleaseDate=1990 \tgenre=alternative",
          "artist=Fugazi \talbum=Red Medicine \treleaseDate=1995 \tgenre=alternative",
          "artist=Peeping Tom \talbum=Peeping Tom \treleaseDate=2006 \tgenre=rock",
          "artist=Red Snapper \talbum=Prince Blimey \treleaseDate=1996 \tgenre=electronic"
      };

  //A collection of example queries, consisting of name/value pairs representing form content plus
  // a choice of query style template to use in the test, with expected number of hits
  String queryForms[] =
      {
          "artist=Fugazi \texpectedMatches=2 \ttemplate=albumBooleanQuery",
          "artist=Fugazi \treleaseDate=1990 \texpectedMatches=1 \ttemplate=albumBooleanQuery",
          "artist=Buckley \tgenre=rock \texpectedMatches=1 \ttemplate=albumFilteredQuery",
          "artist=Buckley \tgenre=electronic \texpectedMatches=0 \ttemplate=albumFilteredQuery",
          "queryString=artist:buckly~ NOT genre:electronic \texpectedMatches=1 \ttemplate=albumLuceneClassicQuery"
      };


  public void testFormTransforms() throws SAXException, IOException, ParserConfigurationException, TransformerException, ParserException {
    // Sun 1.5 suffers from http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6240963
    if (Constants.JAVA_VENDOR.startsWith("Sun") && Constants.JAVA_VERSION.startsWith("1.5")) {
      String defLang = Locale.getDefault().getLanguage();
      assumeFalse("Sun JRE 1.5 suffers from http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6240963 under Turkish locale", defLang.equals("tr") || defLang.equals("az"));
    }
    //Cache all the query templates we will be referring to.
    QueryTemplateManager qtm = new QueryTemplateManager();
    qtm.addQueryTemplate("albumBooleanQuery", getClass().getResourceAsStream("albumBooleanQuery.xsl"));
    qtm.addQueryTemplate("albumFilteredQuery", getClass().getResourceAsStream("albumFilteredQuery.xsl"));
    qtm.addQueryTemplate("albumLuceneClassicQuery", getClass().getResourceAsStream("albumLuceneClassicQuery.xsl"));
    //Run all of our test queries
    for (String queryForm : queryForms) {
      Properties queryFormProperties = getPropsFromString(queryForm);

      //Get the required query XSL template for this test
//      Templates template=getTemplate(queryFormProperties.getProperty("template"));

      //Transform the queryFormProperties into a Lucene XML query
      Document doc = qtm.getQueryAsDOM(queryFormProperties, queryFormProperties.getProperty("template"));

      //Parse the XML query using the XML parser
      Query q = builder.getQuery(doc.getDocumentElement());

      //Run the query
      int h = searcher.search(q, null, 1000).totalHits;

      //Check we have the expected number of results
      int expectedHits = Integer.parseInt(queryFormProperties.getProperty("expectedMatches"));
      assertEquals("Number of results should match for query " + queryForm, expectedHits, h);

    }
  }

  //Helper method to construct Lucene query forms used in our test
  Properties getPropsFromString(String nameValuePairs) {
    Properties result = new Properties();
    StringTokenizer st = new StringTokenizer(nameValuePairs, "\t=");
    while (st.hasMoreTokens()) {
      String name = st.nextToken().trim();
      if (st.hasMoreTokens()) {
        String value = st.nextToken().trim();
        result.setProperty(name, value);
      }
    }
    return result;
  }

  //Helper method to construct Lucene documents used in our tests
  org.apache.lucene.document.Document getDocumentFromString(String nameValuePairs) {
    org.apache.lucene.document.Document result = new org.apache.lucene.document.Document();
    StringTokenizer st = new StringTokenizer(nameValuePairs, "\t=");
    while (st.hasMoreTokens()) {
      String name = st.nextToken().trim();
      if (st.hasMoreTokens()) {
        String value = st.nextToken().trim();
        result.add(newTextField(name, value, Field.Store.YES));
      }
    }
    return result;
  }

  /*
    * @see TestCase#setUp()
    */
  @Override
  public void setUp() throws Exception {
    super.setUp();

    analyzer = new MockAnalyzer(random());
    //Create an index
    dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer));
    for (String docFieldValue : docFieldValues) {
      w.addDocument(getDocumentFromString(docFieldValue));
    }
    w.forceMerge(1);
    w.close();
    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);

    //initialize the parser
    builder = new CorePlusExtensionsParser("artist", analyzer);

  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }
}
