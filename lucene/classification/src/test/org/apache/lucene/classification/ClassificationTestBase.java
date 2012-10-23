package org.apache.lucene.classification;

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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;

/**
 * Base class for testing {@link Classifier}s
 */
public class ClassificationTestBase extends LuceneTestCase {

  private RandomIndexWriter indexWriter;
  private String textFieldName;
  private String classFieldName;
  private Directory dir;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    indexWriter = new RandomIndexWriter(random(), dir);
    textFieldName = "text";
    classFieldName = "cat";
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    indexWriter.close();
    dir.close();
  }

  protected void checkCorrectClassification(Classifier classifier, Analyzer analyzer) throws Exception {
    SlowCompositeReaderWrapper compositeReaderWrapper = null;
    try {
      populateIndex(analyzer);
      compositeReaderWrapper = new SlowCompositeReaderWrapper(indexWriter.getReader());
      classifier.train(compositeReaderWrapper, textFieldName, classFieldName, analyzer);
      String newText = "Much is made of what the likes of Facebook, Google and Apple know about users. Truth is, Amazon may know more.";
      ClassificationResult classificationResult = classifier.assignClass(newText);
      assertEquals("technology", classificationResult.getAssignedClass());
      assertTrue(classificationResult.getScore() > 0);
    } finally {
      if (compositeReaderWrapper != null)
        compositeReaderWrapper.close();
    }
  }

  private void populateIndex(Analyzer analyzer) throws Exception {

    Document doc = new Document();
    doc.add(new TextField(textFieldName, "The traveling press secretary for Mitt Romney lost his cool and cursed at reporters " +
        "who attempted to ask questions of the Republican presidential candidate in a public plaza near the Tomb of " +
        "the Unknown Soldier in Warsaw Tuesday.", Field.Store.YES));
    doc.add(new TextField(classFieldName, "politics", Field.Store.YES));

    indexWriter.addDocument(doc, analyzer);

    doc = new Document();
    doc.add(new TextField(textFieldName, "Mitt Romney seeks to assure Israel and Iran, as well as Jewish voters in the United" +
        " States, that he will be tougher against Iran's nuclear ambitions than President Barack Obama.", Field.Store.YES));
    doc.add(new TextField(classFieldName, "politics", Field.Store.YES));
    indexWriter.addDocument(doc, analyzer);

    doc = new Document();
    doc.add(new TextField(textFieldName, "And there's a threshold question that he has to answer for the American people and " +
        "that's whether he is prepared to be commander-in-chief,\" she continued. \"As we look to the past events, we " +
        "know that this raises some questions about his preparedness and we'll see how the rest of his trip goes.\"", Field.Store.YES));
    doc.add(new TextField(classFieldName, "politics", Field.Store.YES));
    indexWriter.addDocument(doc, analyzer);

    doc = new Document();
    doc.add(new TextField(textFieldName, "Still, when it comes to gun policy, many congressional Democrats have \"decided to " +
        "keep quiet and not go there,\" said Alan Lizotte, dean and professor at the State University of New York at " +
        "Albany's School of Criminal Justice.", Field.Store.YES));
    doc.add(new TextField(classFieldName, "politics", Field.Store.YES));
    indexWriter.addDocument(doc, analyzer);

    doc = new Document();
    doc.add(new TextField(textFieldName, "Standing amongst the thousands of people at the state Capitol, Jorstad, director of " +
        "technology at the University of Wisconsin-La Crosse, documented the historic moment and shared it with the " +
        "world through the Internet.", Field.Store.YES));
    doc.add(new TextField(classFieldName, "technology", Field.Store.YES));
    indexWriter.addDocument(doc, analyzer);

    doc = new Document();
    doc.add(new TextField(textFieldName, "So, about all those experts and analysts who've spent the past year or so saying " +
        "Facebook was going to make a phone. A new expert has stepped forward to say it's not going to happen.", Field.Store.YES));
    doc.add(new TextField(classFieldName, "technology", Field.Store.YES));
    indexWriter.addDocument(doc, analyzer);

    doc = new Document();
    doc.add(new TextField(textFieldName, "More than 400 million people trust Google with their e-mail, and 50 million store files" +
        " in the cloud using the Dropbox service. People manage their bank accounts, pay bills, trade stocks and " +
        "generally transfer or store huge volumes of personal data online.", Field.Store.YES));
    doc.add(new TextField(classFieldName, "technology", Field.Store.YES));
    indexWriter.addDocument(doc, analyzer);

    indexWriter.commit();
  }
}
