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

package org.apache.lucene.monitor;

import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestForceNoBulkScoringQuery extends LuceneTestCase {

  public void testEquality() {

    TermQuery tq1 = new TermQuery(new Term("f", "t"));
    TermQuery tq2 = new TermQuery(new Term("f", "t2"));
    TermQuery tq3 = new TermQuery(new Term("f", "t2"));

    assertEquals(new ForceNoBulkScoringQuery(tq1), new ForceNoBulkScoringQuery(tq1));
    assertNotEquals(new ForceNoBulkScoringQuery(tq1), new ForceNoBulkScoringQuery(tq2));
    assertEquals(new ForceNoBulkScoringQuery(tq2), new ForceNoBulkScoringQuery(tq3));

    assertEquals(new ForceNoBulkScoringQuery(tq2).hashCode(), new ForceNoBulkScoringQuery(tq3).hashCode());
  }

  public void testRewrite() throws IOException {

    try (Directory dir = new ByteBuffersDirectory();
         IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {

      Document doc = new Document();
      doc.add(new TextField("field", "term1 term2 term3 term4", Field.Store.NO));
      iw.addDocument(doc);
      iw.commit();

      IndexReader reader = DirectoryReader.open(dir);

      PrefixQuery pq = new PrefixQuery(new Term("field", "term"));
      ForceNoBulkScoringQuery q = new ForceNoBulkScoringQuery(pq);

      assertEquals(q.getWrappedQuery(), pq);

      Query rewritten = q.rewrite(reader);
      assertTrue(rewritten instanceof ForceNoBulkScoringQuery);

      Query inner = ((ForceNoBulkScoringQuery) rewritten).getWrappedQuery();
      assertNotEquals(inner, pq);


    }


  }

}
