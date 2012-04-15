package org.apache.lucene.search;

/**
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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * 
 */
public class TestFieldValueFilter extends LuceneTestCase {

  public void testFieldValueFilterNoValue() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    int docs = atLeast(10);
    int[] docStates = buildIndex(writer, docs);
    int numDocsNoValue = 0;
    for (int i = 0; i < docStates.length; i++) {
      if (docStates[i] == 0) {
        numDocsNoValue++;
      }
    }

    IndexReader reader = IndexReader.open(directory);
    IndexSearcher searcher = new IndexSearcher(reader);
    TopDocs search = searcher.search(new TermQuery(new Term("all", "test")),
        new FieldValueFilter("some", true), docs);
    assertEquals(search.totalHits, numDocsNoValue);
    
    ScoreDoc[] scoreDocs = search.scoreDocs;
    for (ScoreDoc scoreDoc : scoreDocs) {
      assertNull(reader.document(scoreDoc.doc).get("some"));
    }
    
    reader.close();
    directory.close();
  }
  
  public void testFieldValueFilter() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    int docs = atLeast(10);
    int[] docStates = buildIndex(writer, docs);
    int numDocsWithValue = 0;
    for (int i = 0; i < docStates.length; i++) {
      if (docStates[i] == 1) {
        numDocsWithValue++;
      }
    }
    IndexReader reader = IndexReader.open(directory);
    IndexSearcher searcher = new IndexSearcher(reader);
    TopDocs search = searcher.search(new TermQuery(new Term("all", "test")),
        new FieldValueFilter("some"), docs);
    assertEquals(search.totalHits, numDocsWithValue);
    
    ScoreDoc[] scoreDocs = search.scoreDocs;
    for (ScoreDoc scoreDoc : scoreDocs) {
      assertEquals("value", reader.document(scoreDoc.doc).get("some"));
    }
    
    reader.close();
    directory.close();
  }

  private int[] buildIndex(RandomIndexWriter writer, int docs)
      throws IOException, CorruptIndexException {
    int[] docStates = new int[docs];
    for (int i = 0; i < docs; i++) {
      Document doc = new Document();
      if (random().nextBoolean()) {
        docStates[i] = 1;
        doc.add(newField("some", "value", TextField.TYPE_STORED));
      }
      doc.add(newField("all", "test", TextField.TYPE_UNSTORED));
      doc.add(newField("id", "" + i, TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
    writer.commit();
    int numDeletes = random().nextInt(docs);
    for (int i = 0; i < numDeletes; i++) {
      int docID = random().nextInt(docs);
      writer.deleteDocuments(new Term("id", "" + docID));
      docStates[docID] = 2;
    }
    writer.close();
    return docStates;
  }

}
