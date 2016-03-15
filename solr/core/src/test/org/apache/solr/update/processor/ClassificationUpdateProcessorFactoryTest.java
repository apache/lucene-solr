package org.apache.solr.update.processor;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link ClassificationUpdateProcessor} and {@link ClassificationUpdateProcessorFactory}
 */
public class ClassificationUpdateProcessorFactoryTest extends SolrTestCaseJ4 {
  // field names are used in accordance with the solrconfig and schema supplied
  private static final String ID = "id";
  private static final String TITLE = "title";
  private static final String CONTENT = "content";
  private static final String AUTHOR = "author";
  private static final String CLASS = "cat";

  private static final String CHAIN = "classification";


  private ClassificationUpdateProcessorFactory cFactoryToTest = new ClassificationUpdateProcessorFactory();
  private NamedList args = new NamedList<String>();

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false");
    initCore("solrconfig-classification.xml", "schema-classification.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @Before
  public void initArgs() {
    args.add("inputFields", "inputField1,inputField2");
    args.add("classField", "classField1");
    args.add("algorithm", "bayes");
    args.add("knn.k", "9");
    args.add("knn.minDf", "8");
    args.add("knn.minTf", "10");
  }

  @Test
  public void testFullInit() {
    cFactoryToTest.init(args);

    String[] inputFieldNames = cFactoryToTest.getInputFieldNames();
    assertEquals("inputField1", inputFieldNames[0]);
    assertEquals("inputField2", inputFieldNames[1]);
    assertEquals("classField1", cFactoryToTest.getClassFieldName());
    assertEquals("bayes", cFactoryToTest.getAlgorithm());
    assertEquals(8, cFactoryToTest.getMinDf());
    assertEquals(10, cFactoryToTest.getMinTf());
    assertEquals(9, cFactoryToTest.getK());

  }

  @Test
  public void testInitEmptyInputField() {
    args.removeAll("inputFields");
    try {
      cFactoryToTest.init(args);
    } catch (SolrException e) {
      assertEquals("Classification UpdateProcessor 'inputFields' can not be null", e.getMessage());
    }
  }

  @Test
  public void testInitEmptyClassField() {
    args.removeAll("classField");
    try {
      cFactoryToTest.init(args);
    } catch (SolrException e) {
      assertEquals("Classification UpdateProcessor 'classField' can not be null", e.getMessage());
    }
  }

  @Test
  public void testDefaults() {
    args.removeAll("algorithm");
    args.removeAll("knn.k");
    args.removeAll("knn.minDf");
    args.removeAll("knn.minTf");
    cFactoryToTest.init(args);
    assertEquals("knn", cFactoryToTest.getAlgorithm());
    assertEquals(1, cFactoryToTest.getMinDf());
    assertEquals(1, cFactoryToTest.getMinTf());
    assertEquals(10, cFactoryToTest.getK());
  }

  @Test
  public void testBasicClassification() throws Exception {
    prepareTrainedIndex();
    // To be classified,we index documents without a class and verify the expected one is returned
    addDoc(adoc(ID, "10",
        TITLE, "word4 word4 word4",
        CONTENT, "word5 word5 ",
        AUTHOR, "Name1 Surname1"));
    addDoc(adoc(ID, "11",
        TITLE, "word1 word1",
        CONTENT, "word2 word2",
        AUTHOR, "Name Surname"));
    addDoc(commit());

    Document doc10 = getDoc("10");
    assertEquals("class2", doc10.get(CLASS));
    Document doc11 = getDoc("11");
    assertEquals("class1", doc11.get(CLASS));
  }

  /**
   * Index some example documents with a class manually assigned.
   * This will be our trained model.
   *
   * @throws Exception If there is a low-level I/O error
   */
  private void prepareTrainedIndex() throws Exception {
    //class1
    addDoc(adoc(ID, "1",
        TITLE, "word1 word1 word1",
        CONTENT, "word2 word2 word2",
        AUTHOR, "Name Surname",
        CLASS, "class1"));
    addDoc(adoc(ID, "2",
        TITLE, "word1 word1",
        CONTENT, "word2 word2",
        AUTHOR, "Name Surname",
        CLASS, "class1"));
    addDoc(adoc(ID, "3",
        TITLE, "word1 word1 word1",
        CONTENT, "word2",
        AUTHOR, "Name Surname",
        CLASS, "class1"));
    addDoc(adoc(ID, "4",
        TITLE, "word1 word1 word1",
        CONTENT, "word2 word2 word2",
        AUTHOR, "Name Surname",
        CLASS, "class1"));
    //class2
    addDoc(adoc(ID, "5",
        TITLE, "word4 word4 word4",
        CONTENT, "word5 word5",
        AUTHOR, "Name1 Surname1",
        CLASS, "class2"));
    addDoc(adoc(ID, "6",
        TITLE, "word4 word4",
        CONTENT, "word5",
        AUTHOR, "Name1 Surname1",
        CLASS, "class2"));
    addDoc(adoc(ID, "7",
        TITLE, "word4 word4 word4",
        CONTENT, "word5 word5 word5",
        AUTHOR, "Name1 Surname1",
        CLASS, "class2"));
    addDoc(adoc(ID, "8",
        TITLE, "word4",
        CONTENT, "word5 word5 word5 word5",
        AUTHOR, "Name1 Surname1",
        CLASS, "class2"));
    addDoc(commit());
  }

  private Document getDoc(String id) throws IOException {
    try (SolrQueryRequest req = req()) {
      SolrIndexSearcher searcher = req.getSearcher();
      TermQuery query = new TermQuery(new Term(ID, id));
      TopDocs doc1 = searcher.search(query, 1);
      ScoreDoc scoreDoc = doc1.scoreDocs[0];
      return searcher.doc(scoreDoc.doc);
    }
  }

  static void addDoc(String doc) throws Exception {
    Map<String, String[]> params = new HashMap<>();
    MultiMapSolrParams mmparams = new MultiMapSolrParams(params);
    params.put(UpdateParams.UPDATE_CHAIN, new String[]{CHAIN});
    SolrQueryRequestBase req = new SolrQueryRequestBase(h.getCore(),
        (SolrParams) mmparams) {
    };

    UpdateRequestHandler handler = new UpdateRequestHandler();
    handler.init(null);
    ArrayList<ContentStream> streams = new ArrayList<>(2);
    streams.add(new ContentStreamBase.StringStream(doc));
    req.setContentStreams(streams);
    handler.handleRequestBody(req, new SolrQueryResponse());
    req.close();
  }
}
