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
package org.apache.lucene.classification.document;


import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.classification.ClassificationResult;
import org.apache.lucene.classification.ClassificationTestBase;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;

/**
 * Base class for testing {@link org.apache.lucene.classification.Classifier}s
 */
public abstract class DocumentClassificationTestBase<T> extends ClassificationTestBase {

  protected static final BytesRef VIDEOGAME_RESULT = new BytesRef("videogames");
  protected static final BytesRef VIDEOGAME_ANALYZED_RESULT = new BytesRef("videogam");
  protected static final BytesRef BATMAN_RESULT = new BytesRef("batman");

  protected String titleFieldName = "title";
  protected String authorFieldName = "author";

  protected Analyzer analyzer;
  protected Map<String, Analyzer> field2analyzer;
  protected IndexReader indexReader;

  @Before
  public void init() throws IOException {
    analyzer = new EnglishAnalyzer();
    field2analyzer = new LinkedHashMap<>();
    field2analyzer.put(textFieldName, analyzer);
    field2analyzer.put(titleFieldName, analyzer);
    field2analyzer.put(authorFieldName, analyzer);
    indexReader = populateDocumentClassificationIndex(analyzer);
  }

  protected double checkCorrectDocumentClassification(DocumentClassifier<T> classifier, Document inputDoc, T expectedResult) throws Exception {
    ClassificationResult<T> classificationResult = classifier.assignClass(inputDoc);
    assertNotNull(classificationResult.getAssignedClass());
    assertEquals("got an assigned class of " + classificationResult.getAssignedClass(), expectedResult, classificationResult.getAssignedClass());
    double score = classificationResult.getScore();
    assertTrue("score should be between 0 and 1, got:" + score, score <= 1 && score >= 0);
    return score;
  }

  protected IndexReader populateDocumentClassificationIndex(Analyzer analyzer) throws IOException {
    indexWriter.close();
    indexWriter = new RandomIndexWriter(random(), dir, newIndexWriterConfig(analyzer).setOpenMode(IndexWriterConfig.OpenMode.CREATE));
    indexWriter.commit();
    String text;
    String title;
    String author;

    Document doc = new Document();
    title = "Video games are an economic business";
    text = "Video games have become an art form and an industry. The video game industry is of increasing" +
        " commercial importance, with growth driven particularly by the emerging Asian markets and mobile games." +
        " As of 2015, video games generated sales of USD 74 billion annually worldwide, and were the third-largest" +
        " segment in the U.S. entertainment market, behind broadcast and cable TV.";
    author = "Ign";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(titleFieldName, title, ft));
    doc.add(new Field(authorFieldName, author, ft));
    doc.add(new Field(categoryFieldName, "videogames", ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    title = "Video games: the definition of fun on PC and consoles";
    text = "A video game is an electronic game that involves human interaction with a user interface to generate" +
        " visual feedback on a video device. The word video in video game traditionally referred to a raster display device," +
        "[1] but it now implies any type of display device that can produce two- or three-dimensional images." +
        " The electronic systems used to play video games are known as platforms; examples of these are personal" +
        " computers and video game consoles. These platforms range from large mainframe computers to small handheld devices." +
        " Specialized video games such as arcade games, while previously common, have gradually declined in use.";
    author = "Ign";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(titleFieldName, title, ft));
    doc.add(new Field(authorFieldName, author, ft));
    doc.add(new Field(categoryFieldName, "videogames", ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    title = "Video games: the history across PC, consoles and fun";
    text = "Early games used interactive electronic devices with various display formats. The earliest example is" +
        " from 1947—a device was filed for a patent on 25 January 1947, by Thomas T. Goldsmith Jr. and Estle Ray Mann," +
        " and issued on 14 December 1948, as U.S. Patent 2455992.[2]" +
        "Inspired by radar display tech, it consisted of an analog device that allowed a user to control a vector-drawn" +
        " dot on the screen to simulate a missile being fired at targets, which were drawings fixed to the screen.[3]";
    author = "Ign";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(titleFieldName, title, ft));
    doc.add(new Field(authorFieldName, author, ft));
    doc.add(new Field(categoryFieldName, "videogames", ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    title = "Video games: the history";
    text = "Early games used interactive electronic devices with various display formats. The earliest example is" +
        " from 1947—a device was filed for a patent on 25 January 1947, by Thomas T. Goldsmith Jr. and Estle Ray Mann," +
        " and issued on 14 December 1948, as U.S. Patent 2455992.[2]" +
        "Inspired by radar display tech, it consisted of an analog device that allowed a user to control a vector-drawn" +
        " dot on the screen to simulate a missile being fired at targets, which were drawings fixed to the screen.[3]";
    author = "Ign";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(titleFieldName, title, ft));
    doc.add(new Field(authorFieldName, author, ft));
    doc.add(new Field(categoryFieldName, "videogames", ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    title = "Batman: Arkham Knight PC Benchmarks, For What They're Worth";
    text = "Although I didn’t spend much time playing Batman: Arkham Origins, I remember the game rather well after" +
        " testing it on no less than 30 graphics cards and 20 CPUs. Arkham Origins appeared to take full advantage of" +
        " Unreal Engine 3, it ran smoothly on affordable GPUs, though it’s worth remembering that Origins was developed " +
        "for last-gen consoles.This week marked the arrival of Batman: Arkham Knight, the fourth entry in WB’s Batman:" +
        " Arkham series and a direct sequel to 2013’s Arkham Origins 2011’s Arkham City." +
        "Arkham Knight is also powered by Unreal Engine 3, but you can expect noticeably improved graphics, in part because" +
        " the PlayStation 4 and Xbox One have replaced the PS3 and 360 as the lowest common denominator.";
    author = "Rocksteady Studios";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(titleFieldName, title, ft));
    doc.add(new Field(authorFieldName, author, ft));
    doc.add(new Field(categoryFieldName, "batman", ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    title = "Face-Off: Batman: Arkham Knight, the Dark Knight returns!";
    text = "Despite the drama surrounding the PC release leading to its subsequent withdrawal, there's a sense of success" +
        " in the console space as PlayStation 4 owners, and indeed those on Xbox One, get a superb rendition of Batman:" +
        " Arkham Knight. It's fair to say Rocksteady sized up each console's strengths well ahead of producing its first" +
        " current-gen title, and it's paid off in one of the best Batman games we've seen in years.";
    author = "Rocksteady Studios";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(titleFieldName, title, ft));
    doc.add(new Field(authorFieldName, author, ft));
    doc.add(new Field(categoryFieldName, "batman", ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    title = "Batman: Arkham Knight Having More Trouble, But This Time not in Gotham";
    text = "As news began to break about the numerous issues affecting the PC version of Batman: Arkham Knight, players" +
        " of the console version breathed a sigh of relief and got back to playing the game. Now players of the PlayStation" +
        " 4 version are having problems of their own, albeit much less severe ones." +
        "This time Batman will have a difficult time in Gotham.";
    author = "Rocksteady Studios";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(titleFieldName, title, ft));
    doc.add(new Field(authorFieldName, author, ft));
    doc.add(new Field(categoryFieldName, "batman", ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    indexWriter.addDocument(doc);

    doc = new Document();
    title = "Batman: Arkham Knight the new legend of Gotham";
    text = "As news began to break about the numerous issues affecting the PC version of the game, players" +
        " of the console version breathed a sigh of relief and got back to play. Now players of the PlayStation" +
        " 4 version are having problems of their own, albeit much less severe ones.";
    author = "Rocksteady Studios";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(titleFieldName, title, ft));
    doc.add(new Field(authorFieldName, author, ft));
    doc.add(new Field(categoryFieldName, "batman", ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    indexWriter.addDocument(doc);


    doc = new Document();
    text = "unlabeled doc";
    doc.add(new Field(textFieldName, text, ft));
    indexWriter.addDocument(doc);

    indexWriter.commit();
    return indexWriter.getReader();
  }

  protected Document getVideoGameDocument() {
    Document doc = new Document();
    String title = "The new generation of PC and Console Video games";
    String text = "Recently a lot of games have been released for the latest generations of consoles and personal computers." +
        "One of them is Batman: Arkham Knight released recently on PS4, X-box and personal computer." +
        "Another important video game that will be released in November is Assassin's Creed, a classic series that sees its new installement on Halloween." +
        "Recently a lot of problems affected the Assassin's creed series but this time it should ran smoothly on affordable GPUs." +
        "Players are waiting for the versions of their favourite video games and so do we.";
    String author = "Ign";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(titleFieldName, title, ft));
    doc.add(new Field(authorFieldName, author, ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    return doc;
  }

  protected Document getBatmanDocument() {
    Document doc = new Document();
    String title = "Batman: Arkham Knight new adventures for the super hero across Gotham, the Dark Knight has returned!";
    String title2 = "I am a second title !";
    String text = "This game is the electronic version of the famous super hero adventures.It involves the interaction with the open world" +
        " of the city of Gotham. Finally the player will be able to have fun on its personal device." +
        " The three-dimensional images of the game are stunning, because it uses the Unreal Engine 3." +
        " The systems available are PS4, X-Box and personal computer." +
        " Will the simulate missile that is going to be  fired, success ?\" +\n" +
        " Will this video game make the history" +
        " Help you favourite super hero to defeat all his enemies. The Dark Knight has returned !";
    String author = "Rocksteady Studios";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(titleFieldName, title, ft));
    doc.add(new Field(titleFieldName, title2, ft));
    doc.add(new Field(authorFieldName, author, ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    return doc;
  }

  protected Document getBatmanAmbiguosDocument() {
    Document doc = new Document();
    String title = "Batman: Arkham Knight new adventures for the super hero across Gotham, the Dark Knight has returned! Batman will win !";
    String text = "Early games used interactive electronic devices with various display formats. The earliest example is" +
        " from 1947—a device was filed for a patent on 25 January 1947, by Thomas T. Goldsmith Jr. and Estle Ray Mann," +
        " and issued on 14 December 1948, as U.S. Patent 2455992.[2]" +
        "Inspired by radar display tech, it consisted of an analog device that allowed a user to control a vector-drawn" +
        " dot on the screen to simulate a missile being fired at targets, which were drawings fixed to the screen.[3]";
    String author = "Ign";
    doc.add(new Field(textFieldName, text, ft));
    doc.add(new Field(titleFieldName, title, ft));
    doc.add(new Field(authorFieldName, author, ft));
    doc.add(new Field(booleanFieldName, "false", ft));
    return doc;
  }
}
