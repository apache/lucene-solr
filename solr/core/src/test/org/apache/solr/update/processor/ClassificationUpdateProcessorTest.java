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
import java.util.ArrayList;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.update.AddUpdateCommand;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link ClassificationUpdateProcessor}
 */
public class ClassificationUpdateProcessorTest extends SolrTestCaseJ4 {
  /* field names are used in accordance with the solrconfig and schema supplied */
  private static final String ID = "id";
  private static final String TITLE = "title";
  private static final String CONTENT = "content";
  private static final String AUTHOR = "author";
  private static final String TRAINING_CLASS = "cat";
  private static final String PREDICTED_CLASS = "predicted";
  public static final String KNN = "knn";

  protected Directory directory;
  protected IndexReader reader;
  protected IndexSearcher searcher;
  protected Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
  private ClassificationUpdateProcessor updateProcessorToTest;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false");
    initCore("solrconfig-classification.xml", "schema-classification.xml");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    analyzer.close();
    super.tearDown();
  }




  @Test
  public void classificationMonoClass_predictedClassFieldSet_shouldAssignClassInPredictedClassField() throws Exception {
    UpdateRequestProcessor mockProcessor=mock(UpdateRequestProcessor.class);
    prepareTrainedIndexMonoClass();

    AddUpdateCommand update=new AddUpdateCommand(req());
    SolrInputDocument unseenDocument1 = sdoc(ID, "10",
        TITLE, "word4 word4 word4",
        CONTENT, "word2 word2 ",
        AUTHOR, "unseenAuthor");
    update.solrDoc=unseenDocument1;

    ClassificationUpdateProcessorParams params = initParams(ClassificationUpdateProcessorFactory.Algorithm.KNN);
    params.setPredictedClassField(PREDICTED_CLASS);

    updateProcessorToTest=new ClassificationUpdateProcessor(params,mockProcessor,reader,req().getSchema());
    updateProcessorToTest.processAdd(update);

    assertThat(unseenDocument1.getFieldValue(PREDICTED_CLASS),is("class1"));
  }

  @Test
  public void knnMonoClass_sampleParams_shouldAssignCorrectClass() throws Exception {
    UpdateRequestProcessor mockProcessor=mock(UpdateRequestProcessor.class);
    prepareTrainedIndexMonoClass();

    AddUpdateCommand update=new AddUpdateCommand(req());
    SolrInputDocument unseenDocument1 = sdoc(ID, "10",
        TITLE, "word4 word4 word4",
        CONTENT, "word2 word2 ",
        AUTHOR, "unseenAuthor");
    update.solrDoc=unseenDocument1;

    ClassificationUpdateProcessorParams params = initParams(ClassificationUpdateProcessorFactory.Algorithm.KNN);

    updateProcessorToTest=new ClassificationUpdateProcessor(params,mockProcessor,reader,req().getSchema());
    updateProcessorToTest.processAdd(update);

    assertThat(unseenDocument1.getFieldValue(TRAINING_CLASS),is("class1"));
  }

  @Test
  public void knnMonoClass_boostFields_shouldAssignCorrectClass() throws Exception {
    UpdateRequestProcessor mockProcessor=mock(UpdateRequestProcessor.class);
    prepareTrainedIndexMonoClass();

    AddUpdateCommand update=new AddUpdateCommand(req());
    SolrInputDocument unseenDocument1 = sdoc(ID, "10",
        TITLE, "word4 word4 word4",
        CONTENT, "word2 word2 ",
        AUTHOR, "unseenAuthor");
    update.solrDoc=unseenDocument1;

    ClassificationUpdateProcessorParams params = initParams(ClassificationUpdateProcessorFactory.Algorithm.KNN);
    params.setInputFieldNames(new String[]{TITLE + "^1.5", CONTENT + "^0.5", AUTHOR + "^2.5"});

    updateProcessorToTest=new ClassificationUpdateProcessor(params,mockProcessor,reader,req().getSchema());

    updateProcessorToTest.processAdd(update);

    assertThat(unseenDocument1.getFieldValue(TRAINING_CLASS),is("class2"));
  }

  @Test
  public void bayesMonoClass_sampleParams_shouldAssignCorrectClass() throws Exception {
    UpdateRequestProcessor mockProcessor=mock(UpdateRequestProcessor.class);
    prepareTrainedIndexMonoClass();

    AddUpdateCommand update=new AddUpdateCommand(req());
    SolrInputDocument unseenDocument1 = sdoc(ID, "10",
        TITLE, "word4 word4 word4",
        CONTENT, "word2 word2 ",
        AUTHOR, "unseenAuthor");
    update.solrDoc=unseenDocument1;

    ClassificationUpdateProcessorParams params= initParams(ClassificationUpdateProcessorFactory.Algorithm.BAYES);

    updateProcessorToTest=new ClassificationUpdateProcessor(params,mockProcessor,reader,req().getSchema());
    updateProcessorToTest.processAdd(update);

    assertThat(unseenDocument1.getFieldValue(TRAINING_CLASS),is("class1"));
  }

  @Test
  public void knnMonoClass_contextQueryFiltered_shouldAssignCorrectClass() throws Exception {
    UpdateRequestProcessor mockProcessor=mock(UpdateRequestProcessor.class);
    prepareTrainedIndexMonoClass();

    AddUpdateCommand update=new AddUpdateCommand(req());
    SolrInputDocument unseenDocument1 = sdoc(ID, "10",
        TITLE, "word4 word4 word4",
        CONTENT, "word2 word2 ",
        AUTHOR, "a");
    update.solrDoc=unseenDocument1;

    ClassificationUpdateProcessorParams params= initParams(ClassificationUpdateProcessorFactory.Algorithm.KNN);
    Query class3DocsChunk=new TermQuery(new Term(TITLE,"word6"));
    params.setTrainingFilterQuery(class3DocsChunk);

    updateProcessorToTest=new ClassificationUpdateProcessor(params,mockProcessor,reader,req().getSchema());
    updateProcessorToTest.processAdd(update);

    assertThat(unseenDocument1.getFieldValue(TRAINING_CLASS),is("class3"));
  }

  @Test
  public void bayesMonoClass_boostFields_shouldAssignCorrectClass() throws Exception {
    UpdateRequestProcessor mockProcessor=mock(UpdateRequestProcessor.class);
    prepareTrainedIndexMonoClass();

    AddUpdateCommand update=new AddUpdateCommand(req());
    SolrInputDocument unseenDocument1 = sdoc(ID, "10",
        TITLE, "word4 word4 word4",
        CONTENT, "word2 word2 ",
        AUTHOR, "unseenAuthor");
    update.solrDoc=unseenDocument1;

    ClassificationUpdateProcessorParams params= initParams(ClassificationUpdateProcessorFactory.Algorithm.BAYES);
    params.setInputFieldNames(new String[]{TITLE+"^1.5",CONTENT+"^0.5",AUTHOR+"^2.5"});

    updateProcessorToTest=new ClassificationUpdateProcessor(params,mockProcessor,reader,req().getSchema());

    updateProcessorToTest.processAdd(update);

    assertThat(unseenDocument1.getFieldValue(TRAINING_CLASS),is("class2"));
  }

  @Test
  public void knnClassification_maxOutputClassesGreaterThanAvailable_shouldAssignCorrectClass() throws Exception {
    UpdateRequestProcessor mockProcessor=mock(UpdateRequestProcessor.class);
    prepareTrainedIndexMultiClass();

    AddUpdateCommand update=new AddUpdateCommand(req());
    SolrInputDocument unseenDocument1 = sdoc(ID, "10",
        TITLE, "word1 word1 word1",
        CONTENT, "word2 word2 ",
        AUTHOR, "unseenAuthor");
    update.solrDoc=unseenDocument1;

    ClassificationUpdateProcessorParams params= initParams(ClassificationUpdateProcessorFactory.Algorithm.KNN);
    params.setMaxPredictedClasses(100);

    updateProcessorToTest=new ClassificationUpdateProcessor(params,mockProcessor,reader,req().getSchema());
    updateProcessorToTest.processAdd(update);

    ArrayList<Object> assignedClasses = (ArrayList)unseenDocument1.getFieldValues(TRAINING_CLASS);
    assertThat(assignedClasses.get(0),is("class2"));
    assertThat(assignedClasses.get(1),is("class1"));
  }

  @Test
  public void knnMultiClass_maxOutputClasses2_shouldAssignMax2Classes() throws Exception {
    UpdateRequestProcessor mockProcessor=mock(UpdateRequestProcessor.class);
    prepareTrainedIndexMultiClass();

    AddUpdateCommand update=new AddUpdateCommand(req());
    SolrInputDocument unseenDocument1 = sdoc(ID, "10",
        TITLE, "word1 word1 word1",
        CONTENT, "word2 word2 ",
        AUTHOR, "unseenAuthor");
    update.solrDoc=unseenDocument1;

    ClassificationUpdateProcessorParams params= initParams(ClassificationUpdateProcessorFactory.Algorithm.KNN);
    params.setMaxPredictedClasses(2);

    updateProcessorToTest=new ClassificationUpdateProcessor(params,mockProcessor,reader,req().getSchema());
    updateProcessorToTest.processAdd(update);

    ArrayList<Object> assignedClasses = (ArrayList)unseenDocument1.getFieldValues(TRAINING_CLASS);
    assertThat(assignedClasses.size(),is(2));
    assertThat(assignedClasses.get(0),is("class2"));
    assertThat(assignedClasses.get(1),is("class1"));
  }

  @Test
  public void bayesMultiClass_maxOutputClasses2_shouldAssignMax2Classes() throws Exception {
    UpdateRequestProcessor mockProcessor=mock(UpdateRequestProcessor.class);
    prepareTrainedIndexMultiClass();

    AddUpdateCommand update=new AddUpdateCommand(req());
    SolrInputDocument unseenDocument1 = sdoc(ID, "10",
        TITLE, "word1 word1 word1",
        CONTENT, "word2 word2 ",
        AUTHOR, "unseenAuthor");
    update.solrDoc=unseenDocument1;

    ClassificationUpdateProcessorParams params= initParams(ClassificationUpdateProcessorFactory.Algorithm.BAYES);
    params.setMaxPredictedClasses(2);

    updateProcessorToTest=new ClassificationUpdateProcessor(params,mockProcessor,reader,req().getSchema());
    updateProcessorToTest.processAdd(update);

    ArrayList<Object> assignedClasses = (ArrayList)unseenDocument1.getFieldValues(TRAINING_CLASS);
    assertThat(assignedClasses.size(),is(2));
    assertThat(assignedClasses.get(0),is("class2"));
    assertThat(assignedClasses.get(1),is("class1"));
  }

  @Test
  public void knnMultiClass_boostFieldsMaxOutputClasses2_shouldAssignMax2Classes() throws Exception {
    UpdateRequestProcessor mockProcessor=mock(UpdateRequestProcessor.class);
    prepareTrainedIndexMultiClass();

    AddUpdateCommand update=new AddUpdateCommand(req());
    SolrInputDocument unseenDocument1 = sdoc(ID, "10",
        TITLE, "word4 word4 word4",
        CONTENT, "word2 word2 ",
        AUTHOR, "unseenAuthor");
    update.solrDoc=unseenDocument1;

    ClassificationUpdateProcessorParams params= initParams(ClassificationUpdateProcessorFactory.Algorithm.KNN);
    params.setInputFieldNames(new String[]{TITLE+"^1.5",CONTENT+"^0.5",AUTHOR+"^2.5"});
    params.setMaxPredictedClasses(2);

    updateProcessorToTest=new ClassificationUpdateProcessor(params,mockProcessor,reader,req().getSchema());

    updateProcessorToTest.processAdd(update);

    ArrayList<Object> assignedClasses = (ArrayList)unseenDocument1.getFieldValues(TRAINING_CLASS);
    assertThat(assignedClasses.size(),is(2));
    assertThat(assignedClasses.get(0),is("class4"));
    assertThat(assignedClasses.get(1),is("class6"));
  }

  @Test
  public void bayesMultiClass_boostFieldsMaxOutputClasses2_shouldAssignMax2Classes() throws Exception {
    UpdateRequestProcessor mockProcessor=mock(UpdateRequestProcessor.class);
    prepareTrainedIndexMultiClass();

    AddUpdateCommand update=new AddUpdateCommand(req());
    SolrInputDocument unseenDocument1 = sdoc(ID, "10",
        TITLE, "word4 word4 word4",
        CONTENT, "word2 word2 ",
        AUTHOR, "unseenAuthor");
    update.solrDoc=unseenDocument1;

    ClassificationUpdateProcessorParams params= initParams(ClassificationUpdateProcessorFactory.Algorithm.BAYES);
    params.setInputFieldNames(new String[]{TITLE+"^1.5",CONTENT+"^0.5",AUTHOR+"^2.5"});
    params.setMaxPredictedClasses(2);

    updateProcessorToTest=new ClassificationUpdateProcessor(params,mockProcessor,reader,req().getSchema());

    updateProcessorToTest.processAdd(update);

    ArrayList<Object> assignedClasses = (ArrayList)unseenDocument1.getFieldValues(TRAINING_CLASS);
    assertThat(assignedClasses.size(),is(2));
    assertThat(assignedClasses.get(0),is("class4"));
    assertThat(assignedClasses.get(1),is("class6"));
  }

  private ClassificationUpdateProcessorParams initParams(ClassificationUpdateProcessorFactory.Algorithm classificationAlgorithm) {
    ClassificationUpdateProcessorParams params= new ClassificationUpdateProcessorParams();
    params.setInputFieldNames(new String[]{TITLE,CONTENT,AUTHOR});
    params.setTrainingClassField(TRAINING_CLASS);
    params.setPredictedClassField(TRAINING_CLASS);
    params.setMinTf(1);
    params.setMinDf(1);
    params.setK(5);
    params.setAlgorithm(classificationAlgorithm);
    params.setMaxPredictedClasses(1);
    return params;
  }

  /**
   * Index some example documents with a class manually assigned.
   * This will be our trained model.
   *
   * @throws Exception If there is a low-level I/O error
   */
  private void prepareTrainedIndexMonoClass() throws Exception {
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);

    //class1
    addDoc(writer, buildLuceneDocument(ID, "1",
        TITLE, "word1 word1 word1",
        CONTENT, "word2 word2 word2",
        AUTHOR, "a",
        TRAINING_CLASS, "class1"));
    addDoc(writer, buildLuceneDocument(ID, "2",
        TITLE, "word1 word1",
        CONTENT, "word2 word2",
        AUTHOR, "a",
        TRAINING_CLASS, "class1"));
    addDoc(writer, buildLuceneDocument(ID, "3",
        TITLE, "word1 word1 word1",
        CONTENT, "word2",
        AUTHOR, "a",
        TRAINING_CLASS, "class1"));
    addDoc(writer, buildLuceneDocument(ID, "4",
        TITLE, "word1 word1 word1",
        CONTENT, "word2 word2 word2",
        AUTHOR, "a",
        TRAINING_CLASS, "class1"));
    //class2
    addDoc(writer, buildLuceneDocument(ID, "5",
        TITLE, "word4 word4 word4",
        CONTENT, "word5 word5",
        AUTHOR, "c",
        TRAINING_CLASS, "class2"));
    addDoc(writer, buildLuceneDocument(ID, "6",
        TITLE, "word4 word4",
        CONTENT, "word5",
        AUTHOR, "c",
        TRAINING_CLASS, "class2"));
    addDoc(writer, buildLuceneDocument(ID, "7",
        TITLE, "word4 word4 word4",
        CONTENT, "word5 word5 word5",
        AUTHOR, "c",
        TRAINING_CLASS, "class2"));
    addDoc(writer, buildLuceneDocument(ID, "8",
        TITLE, "word4",
        CONTENT, "word5 word5 word5 word5",
        AUTHOR, "c",
        TRAINING_CLASS, "class2"));
    //class3
    addDoc(writer, buildLuceneDocument(ID, "9",
        TITLE, "word6",
        CONTENT, "word7",
        AUTHOR, "a",
        TRAINING_CLASS, "class3"));
    addDoc(writer, buildLuceneDocument(ID, "10",
        TITLE, "word6",
        CONTENT, "word7",
        AUTHOR, "a",
        TRAINING_CLASS, "class3"));
    addDoc(writer, buildLuceneDocument(ID, "11",
        TITLE, "word6",
        CONTENT, "word7",
        AUTHOR, "a",
        TRAINING_CLASS, "class3"));
    addDoc(writer, buildLuceneDocument(ID, "12",
        TITLE, "word6",
        CONTENT, "word7",
        AUTHOR, "a",
        TRAINING_CLASS, "class3"));

    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }

  private void prepareTrainedIndexMultiClass() throws Exception {
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);

    //class1
    addDoc(writer, buildLuceneDocument(ID, "1",
        TITLE, "word1 word1 word1",
        CONTENT, "word2 word2 word2",
        AUTHOR, "Name Surname",
        TRAINING_CLASS, "class1",
        TRAINING_CLASS, "class2"
        ));
    addDoc(writer, buildLuceneDocument(ID, "2",
        TITLE, "word1 word1",
        CONTENT, "word2 word2",
        AUTHOR, "Name Surname",
        TRAINING_CLASS, "class3",
        TRAINING_CLASS, "class2"
    ));
    addDoc(writer, buildLuceneDocument(ID, "3",
        TITLE, "word1 word1 word1",
        CONTENT, "word2",
        AUTHOR, "Name Surname",
        TRAINING_CLASS, "class1",
        TRAINING_CLASS, "class2"
    ));
    addDoc(writer, buildLuceneDocument(ID, "4",
        TITLE, "word1 word1 word1",
        CONTENT, "word2 word2 word2",
        AUTHOR, "Name Surname",
        TRAINING_CLASS, "class1",
        TRAINING_CLASS, "class2"
    ));
    //class2
    addDoc(writer, buildLuceneDocument(ID, "5",
        TITLE, "word4 word4 word4",
        CONTENT, "word5 word5",
        AUTHOR, "Name1 Surname1",
        TRAINING_CLASS, "class6",
        TRAINING_CLASS, "class4"
    ));
    addDoc(writer, buildLuceneDocument(ID, "6",
        TITLE, "word4 word4",
        CONTENT, "word5",
        AUTHOR, "Name1 Surname1",
        TRAINING_CLASS, "class5",
        TRAINING_CLASS, "class4"
    ));
    addDoc(writer, buildLuceneDocument(ID, "7",
        TITLE, "word4 word4 word4",
        CONTENT, "word5 word5 word5",
        AUTHOR, "Name1 Surname1",
        TRAINING_CLASS, "class6",
        TRAINING_CLASS, "class4"
    ));
    addDoc(writer, buildLuceneDocument(ID, "8",
        TITLE, "word4",
        CONTENT, "word5 word5 word5 word5",
        AUTHOR, "Name1 Surname1",
        TRAINING_CLASS, "class6",
        TRAINING_CLASS, "class4"
    ));

    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }

  public static Document buildLuceneDocument(Object... fieldsAndValues) {
    Document luceneDoc = new Document();
    for (int i=0; i<fieldsAndValues.length; i+=2) {
      luceneDoc.add(newTextField((String)fieldsAndValues[i], (String)fieldsAndValues[i+1], Field.Store.YES));
    }
    return luceneDoc;
  }

  private int addDoc(RandomIndexWriter writer, Document doc) throws IOException {
    writer.addDocument(doc);
    return writer.numDocs() - 1;
  }
}