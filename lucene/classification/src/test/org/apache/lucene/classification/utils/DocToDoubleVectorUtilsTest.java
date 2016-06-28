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
package org.apache.lucene.classification.utils;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Testcase for {@link org.apache.lucene.classification.utils.DocToDoubleVectorUtils}
 */
public class DocToDoubleVectorUtilsTest extends LuceneTestCase {

  private IndexReader index;
  private Directory dir;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter indexWriter = new RandomIndexWriter(random(), dir);

    FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorOffsets(true);
    ft.setStoreTermVectorPositions(true);

    Document doc;
    for (int i = 0; i < 10; i++) {
      doc = new Document();
      doc.add(new Field("id", Integer.toString(i), ft));
      doc.add(new Field("text", random().nextInt(10) + " " + random().nextInt(10) + " " + random().nextInt(10), ft));
      indexWriter.addDocument(doc);
    }

    indexWriter.commit();

    index = indexWriter.getReader();

    indexWriter.close();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    index.close();
    dir.close();
    super.tearDown();
  }

  @Test
  public void testDenseFreqDoubleArrayConversion() throws Exception {
    IndexSearcher indexSearcher = new IndexSearcher(index);
    for (ScoreDoc scoreDoc : indexSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE).scoreDocs) {
      Terms docTerms = index.getTermVector(scoreDoc.doc, "text");
      Double[] vector = DocToDoubleVectorUtils.toDenseLocalFreqDoubleArray(docTerms);
      assertNotNull(vector);
      assertTrue(vector.length > 0);
    }
  }

  @Test
  public void testSparseFreqDoubleArrayConversion() throws Exception {
    Terms fieldTerms = MultiFields.getTerms(index, "text");
    if (fieldTerms != null && fieldTerms.size() != -1) {
      IndexSearcher indexSearcher = new IndexSearcher(index);
      for (ScoreDoc scoreDoc : indexSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE).scoreDocs) {
        Terms docTerms = index.getTermVector(scoreDoc.doc, "text");
        Double[] vector = DocToDoubleVectorUtils.toSparseLocalFreqDoubleArray(docTerms, fieldTerms);
        assertNotNull(vector);
        assertTrue(vector.length > 0);
      }
    }
  }
}
