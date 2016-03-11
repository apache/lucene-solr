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
package org.apache.lucene.classification;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.After;
import org.junit.Before;

/**
 * Base class for testing {@link Classifier}s
 */
public abstract class ClassificationTestBase<T> extends LuceneTestCase {
  protected static final String POLITICS_INPUT = "Here are some interesting questions and answers about Mitt Romney.. " +
          "If you don't know the answer to the question about Mitt Romney, then simply click on the answer below the question section.";
  protected static final BytesRef POLITICS_RESULT = new BytesRef("politics");

  protected static final String TECHNOLOGY_INPUT = "Much is made of what the likes of Facebook, Google and Apple know about users." +
          " Truth is, Amazon may know more.";

  protected static final String STRONG_TECHNOLOGY_INPUT = "Much is made of what the likes of Facebook, Google and Apple know about users." +
      " Truth is, Amazon may know more. This technology observation is extracted from the internet.";

  protected static final String SUPER_STRONG_TECHNOLOGY_INPUT = "More than 400 million people trust Google with their e-mail, and 50 million store files" +
      " in the cloud using the Dropbox service. People manage their bank accounts, pay bills, trade stocks and " +
      "generally transfer or store huge volumes of personal data online. traveling seeks raises some questions Republican presidential. ";

  protected static final BytesRef TECHNOLOGY_RESULT = new BytesRef("technology");

  protected RandomIndexWriter indexWriter;
  protected Directory dir;
  protected FieldType ft;

  protected String textFieldName;
  protected String categoryFieldName;
  protected String booleanFieldName;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    indexWriter = new RandomIndexWriter(random(), dir);
    textFieldName = "text";
    categoryFieldName = "cat";
    booleanFieldName = "bool";
    ft = new FieldType(TextField.TYPE_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorOffsets(true);
    ft.setStoreTermVectorPositions(true);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    indexWriter.close();
    dir.close();
  }

  protected ClassificationResult<T> checkCorrectClassification(Classifier<T> classifier, String inputDoc, T expectedResult) throws Exception {
    ClassificationResult<T> classificationResult = classifier.assignClass(inputDoc);
    assertNotNull(classificationResult.getAssignedClass());
    assertEquals("got an assigned class of " + classificationResult.getAssignedClass(), expectedResult, classificationResult.getAssignedClass());
    double score = classificationResult.getScore();
    assertTrue("score should be between 0 and 1, got:" + score, score <= 1 && score >= 0);
    return classificationResult;
  }

  protected void checkOnlineClassification(Classifier<T> classifier, String inputDoc, T expectedResult, Analyzer analyzer, String textFieldName, String classFieldName) throws Exception {
    checkOnlineClassification(classifier, inputDoc, expectedResult, analyzer, textFieldName, classFieldName, null);
  }

  protected void checkOnlineClassification(Classifier<T> classifier, String inputDoc, T expectedResult, Analyzer analyzer, String textFieldName, String classFieldName, Query query) throws Exception {
    getSampleIndex(analyzer);

    ClassificationResult<T> classificationResult = classifier.assignClass(inputDoc);
    assertNotNull(classificationResult.getAssignedClass());
    assertEquals("got an assigned class of " + classificationResult.getAssignedClass(), expectedResult, classificationResult.getAssignedClass());
    double score = classificationResult.getScore();
    assertTrue("score should be between 0 and 1, got: " + score, score <= 1 && score >= 0);
    updateSampleIndex();
    ClassificationResult<T> secondClassificationResult = classifier.assignClass(inputDoc);
    assertEquals(classificationResult.getAssignedClass(), secondClassificationResult.getAssignedClass());
    assertEquals(Double.valueOf(score), Double.valueOf(secondClassificationResult.getScore()));

  }

  protected LeafReader getSampleIndex(Analyzer analyzer) throws IOException {
    indexWriter.close();
    indexWriter = new RandomIndexWriter(random(), dir, newIndexWriterConfig(analyzer).setOpenMode(IndexWriterConfig.OpenMode.CREATE));
    indexWriter.commit();

    String text;

    Document doc = new Document();
    text = "The traveling press secretary for Mitt Romney lost his cool and cursed at reporters " +
            "who attempted to ask questions of the Republican presidential candidate in a public plaza near the Tomb of " +
            "the Unknown Soldier in Warsaw Tuesday.";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(categoryFieldName, "politics", ft));
    doc.add(new Field(booleanFieldName, "true", ft));

    indexWriter.addDocument(doc);

    doc = new Document();
    text = "Mitt Romney seeks to assure Israel and Iran, as well as Jewish voters in the United" +
            " States, that he will be tougher against Iran's nuclear ambitions than President Barack Obama.";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(categoryFieldName, "politics", ft));
    doc.add(new Field(booleanFieldName, "true", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    text = "And there's a threshold question that he has to answer for the American people and " +
            "that's whether he is prepared to be commander-in-chief,\" she continued. \"As we look to the past events, we " +
            "know that this raises some questions about his preparedness and we'll see how the rest of his trip goes.\"";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(categoryFieldName, "politics", ft));
    doc.add(new Field(booleanFieldName, "true", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    text = "Still, when it comes to gun policy, many congressional Democrats have \"decided to " +
            "keep quiet and not go there,\" said Alan Lizotte, dean and professor at the State University of New York at " +
            "Albany's School of Criminal Justice.";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(categoryFieldName, "politics", ft));
    doc.add(new Field(booleanFieldName, "true", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    text = "Standing amongst the thousands of people at the state Capitol, Jorstad, director of " +
            "technology at the University of Wisconsin-La Crosse, documented the historic moment and shared it with the " +
            "world through the Internet.";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(categoryFieldName, "technology", ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    text = "So, about all those experts and analysts who've spent the past year or so saying " +
            "Facebook was going to make a phone. A new expert has stepped forward to say it's not going to happen.";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(categoryFieldName, "technology", ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    text = "More than 400 million people trust Google with their e-mail, and 50 million store files" +
            " in the cloud using the Dropbox service. People manage their bank accounts, pay bills, trade stocks and " +
            "generally transfer or store huge volumes of personal data online.";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(categoryFieldName, "technology", ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    text = "unlabeled doc";
    doc.add(new Field(textFieldName, text, ft));
    indexWriter.addDocument(doc);

    indexWriter.commit();
    indexWriter.forceMerge(1);
    return getOnlyLeafReader(indexWriter.getReader());
  }

  protected LeafReader getRandomIndex(Analyzer analyzer, int size) throws IOException {
    indexWriter.close();
    indexWriter = new RandomIndexWriter(random(), dir, newIndexWriterConfig(analyzer).setOpenMode(IndexWriterConfig.OpenMode.CREATE));
    indexWriter.deleteAll();
    indexWriter.commit();

    FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorOffsets(true);
    ft.setStoreTermVectorPositions(true);
    Random random = random();
    for (int i = 0; i < size; i++) {
      boolean b = random.nextBoolean();
      Document doc = new Document();
      doc.add(new Field(textFieldName, createRandomString(random), ft));
      doc.add(new Field(categoryFieldName, String.valueOf(random.nextInt(1000)), ft));
      doc.add(new Field(booleanFieldName, String.valueOf(b), ft));
      indexWriter.addDocument(doc);
    }
    indexWriter.commit();
    indexWriter.forceMerge(1);
    return getOnlyLeafReader(indexWriter.getReader());
  }

  private String createRandomString(Random random) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < 20; i++) {
      builder.append(TestUtil.randomSimpleString(random, 5));
      builder.append(" ");
    }
    return builder.toString();
  }

  private void updateSampleIndex() throws Exception {

    String text;

    Document doc = new Document();
    text = "Warren Bennis says John F. Kennedy grasped a key lesson about the presidency that few have followed.";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(categoryFieldName, "politics", ft));
    doc.add(new Field(booleanFieldName, "true", ft));

    indexWriter.addDocument(doc);

    doc = new Document();
    text = "Julian Zelizer says Bill Clinton is still trying to shape his party, years after the White House, while George W. Bush opts for a much more passive role.";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(categoryFieldName, "politics", ft));
    doc.add(new Field(booleanFieldName, "true", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    text = "Crossfire: Sen. Tim Scott passes on Sen. Lindsey Graham endorsement";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(categoryFieldName, "politics", ft));
    doc.add(new Field(booleanFieldName, "true", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    text = "Illinois becomes 16th state to allow same-sex marriage.";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(categoryFieldName, "politics", ft));
    doc.add(new Field(booleanFieldName, "true", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    text = "Apple is developing iPhones with curved-glass screens and enhanced sensors that detect different levels of pressure, according to a new report.";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(categoryFieldName, "technology", ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    text = "The Xbox One is Microsoft's first new gaming console in eight years. It's a quality piece of hardware but it's also noteworthy because Microsoft is using it to make a statement.";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(categoryFieldName, "technology", ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    text = "Google says it will replace a Google Maps image after a California father complained it shows the body of his teen-age son, who was shot to death in 2009.";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(categoryFieldName, "technology", ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    text = "second unlabeled doc";
    doc.add(new Field(textFieldName, text, ft));
    indexWriter.addDocument(doc);

    indexWriter.commit();
  }
}
