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

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

/**
 * Tests for {@link ClassificationUpdateProcessor} and {@link ClassificationUpdateProcessorFactory}
 */
public class ClassificationUpdateProcessorIntegrationTest extends SolrTestCaseJ4 {
  /* field names are used in accordance with the solrconfig and schema supplied */
  private static final String ID = "id";
  private static final String TITLE = "title";
  private static final String CONTENT = "content";
  private static final String AUTHOR = "author";
  private static final String CLASS = "cat";

  private static final String CHAIN = "classification";
  private static final String BROKEN_CHAIN_FILTER_QUERY = "classification-unsupported-filterQuery";

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

  @Test
  public void classify_fullConfiguration_shouldAutoClassify() throws Exception {
    indexTrainingSet();
    // To be classified,we index documents without a class and verify the expected one is returned
    addDoc(adoc(ID, "22",
        TITLE, "word4 word4 word4",
        CONTENT, "word5 word5 ",
        AUTHOR, "Name1 Surname1"), CHAIN);
    addDoc(adoc(ID, "21",
        TITLE, "word1 word1",
        CONTENT, "word2 word2",
        AUTHOR, "Name Surname"), CHAIN);
    addDoc(commit());

    Document doc22 = getDoc("22");
    assertThat(doc22.get(CLASS),is("class2"));
    Document doc21 = getDoc("21");
    assertThat(doc21.get(CLASS),is("class1"));
  }

  @Test
  public void classify_unsupportedFilterQueryConfiguration_shouldThrowExceptionWithDetailedMessage() throws Exception {
    indexTrainingSet();
    try {
      addDoc(adoc(ID, "21",
          TITLE, "word4 word4 word4",
          CONTENT, "word5 word5 ",
          AUTHOR, "Name1 Surname1"), BROKEN_CHAIN_FILTER_QUERY);
      addDoc(adoc(ID, "22",
          TITLE, "word1 word1",
          CONTENT, "word2 word2",
          AUTHOR, "Name Surname"), BROKEN_CHAIN_FILTER_QUERY);
      addDoc(commit());
    } catch (SolrException e) {
      assertEquals("Classification UpdateProcessor Training Filter Query: 'not valid ( lucene query' is not supported", e.getMessage());
    }
  }

  /**
   * Index some example documents with a class manually assigned.
   * This will be our trained model.
   *
   * @throws Exception If there is a low-level I/O error
   */
  private void indexTrainingSet() throws Exception {
    //class1
    addDoc(adoc(ID, "1",
        TITLE, "word1 word1 word1",
        CONTENT, "word2 word2 word2",
        AUTHOR, "Name Surname",
        CLASS, "class1"), CHAIN);
    addDoc(adoc(ID, "2",
        TITLE, "word1 word1",
        CONTENT, "word2 word2",
        AUTHOR, "Name Surname",
        CLASS, "class1"), CHAIN);
    addDoc(adoc(ID, "3",
        TITLE, "word1 word1 word1",
        CONTENT, "word2",
        AUTHOR, "Name Surname",
        CLASS, "class1"), CHAIN);
    addDoc(adoc(ID, "4",
        TITLE, "word1 word1 word1",
        CONTENT, "word2 word2 word2",
        AUTHOR, "Name Surname",
        CLASS, "class1"), CHAIN);
    //class2
    addDoc(adoc(ID, "5",
        TITLE, "word4 word4 word4",
        CONTENT, "word5 word5",
        AUTHOR, "Name Surname",
        CLASS, "class2"), CHAIN);
    addDoc(adoc(ID, "6",
        TITLE, "word4 word4",
        CONTENT, "word5",
        AUTHOR, "Name Surname",
        CLASS, "class2"), CHAIN);
    addDoc(adoc(ID, "7",
        TITLE, "word4 word4 word4",
        CONTENT, "word5 word5 word5",
        AUTHOR, "Name Surname",
        CLASS, "class2"), CHAIN);
    addDoc(adoc(ID, "8",
        TITLE, "word4",
        CONTENT, "word5 word5 word5 word5",
        AUTHOR, "Name Surname",
        CLASS, "class2"), CHAIN);
    //class3
    addDoc(adoc(ID, "9",
        TITLE, "word4 word4 word4",
        CONTENT, "word5 word5",
        AUTHOR, "Name1 Surname1",
        CLASS, "class3"), CHAIN);
    addDoc(adoc(ID, "10",
        TITLE, "word4 word4",
        CONTENT, "word5",
        AUTHOR, "Name1 Surname1",
        CLASS, "class3"), CHAIN);
    addDoc(adoc(ID, "11",
        TITLE, "word4 word4 word4",
        CONTENT, "word5 word5 word5",
        AUTHOR, "Name1 Surname1",
        CLASS, "class3"), CHAIN);
    addDoc(adoc(ID, "12",
        TITLE, "word4",
        CONTENT, "word5 word5 word5 word5",
        AUTHOR, "Name1 Surname1",
        CLASS, "class3"), CHAIN);
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

  private void addDoc(String doc) throws Exception {
    addDoc(doc, CHAIN);
  }
}
