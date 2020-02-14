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
package org.apache.lucene.index;


import java.io.IOException;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.VectorValues;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.search.ExactVectorDistanceQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.VectorDistanceQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

// TODO: improve tests
public class TestVectorsFormat extends LuceneTestCase {

  private static final String VECTOR_FIELD = "vector";

  public void testBasicIndexing() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setCodec(Codec.forName("Lucene90")));

    int numDoc = 10;
    int dim = 32;
    float[][] values = new float[numDoc][];
    for (int i = 0; i < numDoc; i++) {
      values[i] = randomVector(dim);
      add(writer, i, values[i]);
    }

    writer.commit();
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext leafReaderContext : reader.leaves()) {
      VectorValues vectorValues = leafReaderContext.reader().getVectorValues(VECTOR_FIELD);

      BinaryDocValues docValues = vectorValues.getValues();
      assertNotNull(docValues);
    }

    reader.close();
    dir.close();
  }

  public void testMerging() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setCodec(Codec.forName("Lucene90")));

    int numDoc = 10;
    int dim = 32;
    int docId = 0;
    for (int i = 0; i < numDoc; i++) {
      float[] vector = randomVector(dim);
      add(writer, docId++, vector);
    }
    writer.commit();

    for (int i = 0; i < numDoc; i++) {
      float[] vector = randomVector(dim);
      add(writer, docId++, vector);
    }
    writer.commit();

    writer.forceMerge(1);
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext leafReaderContext : reader.leaves()) {
      VectorValues vectorValues = leafReaderContext.reader().getVectorValues(VECTOR_FIELD);

      BinaryDocValues docValues = vectorValues.getValues();
      assertNotNull(docValues);
    }

    reader.close();
    dir.close();
  }

  public void testExactVectorDistanceQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setCodec(Codec.forName("Lucene90")));

    int numDoc = 10;
    int dim = 32;

    float[][] values = new float[numDoc][];
    for (int i = 0; i < numDoc; i++) {
      values[i] = randomVector(dim);
      add(writer, i, values[i]);
    }

    writer.commit();
    writer.close();

    float[] queryVector = randomVector(dim);
    Query query = new ExactVectorDistanceQuery(VECTOR_FIELD, queryVector);

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);

    TopDocs topDocs = searcher.search(query, 5);
    assertEquals(5, topDocs.scoreDocs.length);

    reader.close();
    dir.close();
  }

  public void testVectorDistanceQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setCodec(Codec.forName("Lucene90")));

    int numDoc = 10;
    int dim = 32;

    float[][] values = new float[numDoc][];
    for (int i = 0; i < numDoc; i++) {
      values[i] = randomVector(dim);
      add(writer, i, values[i]);
    }

    writer.commit();
    writer.close();

    float[] queryVector = randomVector(dim);
    Query query = new VectorDistanceQuery(VECTOR_FIELD, queryVector, 2);

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);

    TopDocs topDocs = searcher.search(query, 5);
    assertEquals(5, topDocs.scoreDocs.length);

    reader.close();
    dir.close();
  }

  private float[] randomVector(int dim) {
    float[] vector = new float[dim];
    for (int i = 0; i < vector.length; i++) {
      vector[i] = random().nextFloat();
    }
    return vector;
  }

  private void add(IndexWriter iw, int id, float[] vector) throws IOException {
    Document doc = new Document();
    if (vector != null) {
      doc.add(new VectorField(VECTOR_FIELD, vector));
    }
    doc.add(new StringField("id", Integer.toString(id), Field.Store.YES));
    iw.addDocument(doc);
  }
}