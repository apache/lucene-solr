package org.apache.lucene.queries.function;


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queries.function.valuesource.ConstValueSource;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

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

/**
 * Basic tests for {@link BoostedQuery}
 */
// TODO: more tests
public class TestBoostedQuery extends LuceneTestCase {
  static Directory dir;
  static IndexReader ir;
  static IndexSearcher is;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newDirectory();
    IndexWriterConfig iwConfig = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwConfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConfig);
    Document document = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    document.add(idField);
    iw.addDocument(document);
    ir = iw.getReader();
    is = newSearcher(ir);
    iw.close();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    is = null;
    ir.close();
    ir = null;
    dir.close();
    dir = null;
  }
  
  public void testBasic() throws Exception {
    Query q = new MatchAllDocsQuery();
    TopDocs docs = is.search(q, 10);
    assertEquals(1, docs.totalHits);
    float score = docs.scoreDocs[0].score;
    
    Query boostedQ = new BoostedQuery(q, new ConstValueSource(2.0f));
    assertHits(boostedQ, new float[] { score*2 });
  }
  
  void assertHits(Query q, float scores[]) throws Exception {
    ScoreDoc expected[] = new ScoreDoc[scores.length];
    int expectedDocs[] = new int[scores.length];
    for (int i = 0; i < expected.length; i++) {
      expectedDocs[i] = i;
      expected[i] = new ScoreDoc(i, scores[i]);
    }
    TopDocs docs = is.search(q, 10, 
        new Sort(new SortField("id", SortField.Type.STRING)));
    CheckHits.checkHits(random(), q, "", is, expectedDocs);
    CheckHits.checkHitsQuery(q, expected, docs.scoreDocs, expectedDocs);
    CheckHits.checkExplanations(q, "", is);
  }
}
