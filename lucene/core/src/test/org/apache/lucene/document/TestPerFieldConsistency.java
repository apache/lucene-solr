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

package org.apache.lucene.document;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

import java.io.IOException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestPerFieldConsistency extends LuceneTestCase {

  public void testDocWithMissingSchemaOptionsThrowsError() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig()); ) {
      final String FIELD_NAME = "myfield";
      final Field textField = new Field(FIELD_NAME, "myvalue", TextField.TYPE_NOT_STORED);
      final Field docValuesField = new BinaryDocValuesField(FIELD_NAME, new BytesRef("myvalue"));
      final Field pointField = new LongPoint(FIELD_NAME, 1);
      final Field vectorField =
          new VectorField(
              FIELD_NAME, new float[] {.1f, .2f, .3f}, VectorValues.SearchStrategy.EUCLIDEAN_HNSW);
      final Field[] fields = new Field[] {textField, docValuesField, pointField, vectorField};
      final String[] errorMsgs = new String[] {"index options", "doc values", "points", "vector"};

      final Document doc0 = new Document();
      for (Field field : fields) {
        doc0.add(field);
      }
      writer.addDocument(doc0);

      // the same segment: indexing a doc with a missing field throws error
      int missingFieldIdx = randomIntBetween(0, fields.length - 1);
      final Document doc1 = new Document();
      for (int i = 0; i < fields.length; i++) {
        if (i != missingFieldIdx) {
          doc1.add(fields[i]);
        }
      }
      IllegalArgumentException exception =
          expectThrows(IllegalArgumentException.class, () -> writer.addDocument(doc1));
      String expectedErrMsg =
          "Inconsistency of field data structures across documents for field [myfield] of doc [1].";
      assertEquals(expectedErrMsg, exception.getMessage());

      writer.flush();
      try (IndexReader reader = DirectoryReader.open(writer)) {
        assertEquals(1, reader.numDocs());
        assertEquals(1, reader.numDeletedDocs());
      }

      // diff segment, same index: indexing a doc with a missing field throws error
      exception = expectThrows(IllegalArgumentException.class, () -> writer.addDocument(doc1));
      assertTrue(
          exception
              .getMessage()
              .contains("cannot change field \"myfield\" from " + errorMsgs[missingFieldIdx]));

      writer.addDocument(doc0); // add document with correct data structures

      writer.flush();
      try (IndexReader reader = DirectoryReader.open(writer)) {
        assertEquals(2, reader.numDocs());
        assertEquals(2, reader.numDeletedDocs());
      }
    }
  }

  public void testDocWithExtraIndexingOptionsThrowsError() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig()); ) {
      final String FIELD_NAME = "myfield";
      final Field textField = new Field(FIELD_NAME, "myvalue", TextField.TYPE_NOT_STORED);
      final Field docValuesField = new BinaryDocValuesField(FIELD_NAME, new BytesRef("myvalue"));
      final Field pointField = new LongPoint(FIELD_NAME, 1);
      final Field vectorField =
          new VectorField(
              FIELD_NAME, new float[] {.1f, .2f, .3f}, VectorValues.SearchStrategy.EUCLIDEAN_HNSW);
      final Field[] fields = new Field[] {textField, docValuesField, pointField, vectorField};
      final String[] errorMsgs = new String[] {"index options", "doc values", "points", "vector"};

      final Document doc0 = new Document();
      int existingFieldIdx = randomIntBetween(0, fields.length - 1);
      doc0.add(fields[existingFieldIdx]);
      writer.addDocument(doc0);

      // the same segment: indexing a field with extra field indexing options returns error
      int extraFieldIndex = randomIntBetween(0, fields.length - 1);
      while (extraFieldIndex == existingFieldIdx) {
        extraFieldIndex = randomIntBetween(0, fields.length - 1);
      }
      final Document doc1 = new Document();
      doc1.add(fields[existingFieldIdx]);
      doc1.add(fields[extraFieldIndex]);

      IllegalArgumentException exception =
          expectThrows(IllegalArgumentException.class, () -> writer.addDocument(doc1));
      String expectedErrMsg =
          "Inconsistency of field data structures across documents for field [myfield] of doc [1].";
      assertEquals(expectedErrMsg, exception.getMessage());

      writer.flush();
      try (IndexReader reader = DirectoryReader.open(writer)) {
        LeafReader lr1 = reader.leaves().get(0).reader();
        assertEquals(1, lr1.numDocs());
        assertEquals(1, lr1.numDeletedDocs());
      }

      // diff segment, same index: indexing a field with extra field indexing options returns error
      exception = expectThrows(IllegalArgumentException.class, () -> writer.addDocument(doc1));
      assertTrue(
          exception
              .getMessage()
              .contains("cannot change field \"myfield\" from " + errorMsgs[extraFieldIndex]));

      writer.addDocument(doc0); // add document with correct data structures

      writer.flush();
      try (IndexReader reader = DirectoryReader.open(writer)) {
        LeafReader lr2 = reader.leaves().get(1).reader();
        assertEquals(1, lr2.numDocs());
        assertEquals(1, lr2.numDeletedDocs());
      }
    }
  }
}
