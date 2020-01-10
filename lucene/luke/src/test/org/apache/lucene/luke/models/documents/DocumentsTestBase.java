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

package org.apache.lucene.luke.models.documents;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.junit.After;
import org.junit.Before;

public abstract class DocumentsTestBase extends LuceneTestCase {
  protected IndexReader reader;
  protected Directory dir;
  protected Path indexDir;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createIndex();
    dir = newFSDirectory(indexDir);
    reader = DirectoryReader.open(dir);
  }

  protected void createIndex() throws IOException {
    indexDir = createTempDir();

    Directory dir = newFSDirectory(indexDir);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, new StandardAnalyzer());

    FieldType titleType = new FieldType();
    titleType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    titleType.setStored(true);
    titleType.setTokenized(true);
    titleType.setOmitNorms(true);

    FieldType authorType = new FieldType();
    authorType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    authorType.setStored(true);
    authorType.setTokenized(true);
    authorType.setOmitNorms(false);

    FieldType textType = new FieldType();
    textType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    textType.setStored(false);
    textType.setTokenized(true);
    textType.setStoreTermVectors(true);
    textType.setOmitNorms(false);

    FieldType downloadsType = new FieldType();
    downloadsType.setDimensions(1, Integer.BYTES);
    downloadsType.setStored(true);

    Document doc1 = new Document();
    doc1.add(new Field("title", "Pride and Prejudice", titleType));
    doc1.add(new Field("author", "Jane Austen", authorType));
    doc1.add(new Field("text",
        "It is a truth universally acknowledged, that a single man in possession of a good fortune, must be in want of a wife.",
        textType));
    doc1.add(new SortedSetDocValuesField("subject", new BytesRef("Fiction")));
    doc1.add(new SortedSetDocValuesField("subject", new BytesRef("Love stories")));
    doc1.add(new Field("downloads", packInt(28533), downloadsType));
    writer.addDocument(doc1);

    Document doc2 = new Document();
    doc2.add(new Field("title", "Alice's Adventures in Wonderland", titleType));
    doc2.add(new Field("author", "Lewis Carroll", authorType));
    doc2.add(new Field("text", "Alice was beginning to get very tired of sitting by her sister on the bank, and of having nothing to do: once or twice she had peeped into the book her sister was reading, but it had no pictures or conversations in it, ‘and what is the use of a book,’ thought Alice ‘without pictures or conversations?’",
        textType));
    doc2.add(new SortedSetDocValuesField("subject", new BytesRef("Fantasy literature")));
    doc2.add(new Field("downloads", packInt(18712), downloadsType));
    writer.addDocument(doc2);

    Document doc3 = new Document();
    doc3.add(new Field("title", "Frankenstein; Or, The Modern Prometheus", titleType));
    doc3.add(new Field("author", "Mary Wollstonecraft Shelley", authorType));
    doc3.add(new Field("text", "You will rejoice to hear that no disaster has accompanied the commencement of an enterprise which you have regarded with such evil forebodings. I arrived here yesterday, and my first task is to assure my dear sister of my welfare and increasing confidence in the success of my undertaking.",
        textType));
    doc3.add(new SortedSetDocValuesField("subject", new BytesRef("Science fiction")));
    doc3.add(new SortedSetDocValuesField("subject", new BytesRef("Horror tales")));
    doc3.add(new SortedSetDocValuesField("subject", new BytesRef("Monsters")));
    doc3.add(new Field("downloads", packInt(14737), downloadsType));
    writer.addDocument(doc3);

    Document doc4 = new Document();
    doc4.add(new Field("title", "A Doll's House : a play", titleType));
    doc4.add(new Field("author", "Henrik Ibsen", authorType));
    doc4.add(new Field("text", "",
        textType));
    doc4.add(new SortedSetDocValuesField("subject", new BytesRef("Drama")));
    doc4.add(new Field("downloads", packInt(14629), downloadsType));
    writer.addDocument(doc4);

    Document doc5 = new Document();
    doc5.add(new Field("title", "The Adventures of Sherlock Holmes", titleType));
    doc5.add(new Field("author", "Arthur Conan Doyle", authorType));
    doc5.add(new Field("text", "To Sherlock Holmes she is always the woman. I have seldom heard him mention her under any other name. In his eyes she eclipses and predominates the whole of her sex.",
        textType));
    doc5.add(new SortedSetDocValuesField("subject", new BytesRef("Fiction")));
    doc5.add(new SortedSetDocValuesField("subject", new BytesRef("Detective and mystery stories")));
    doc5.add(new Field("downloads", packInt(12828), downloadsType));
    writer.addDocument(doc5);

    writer.commit();

    writer.close();
    dir.close();
  }

  private BytesRef packInt(int value) {
    byte[] dest = new byte[Integer.BYTES];
    NumericUtils.intToSortableBytes(value, dest, 0);
    return new BytesRef(dest);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    reader.close();
    dir.close();
  }

}
