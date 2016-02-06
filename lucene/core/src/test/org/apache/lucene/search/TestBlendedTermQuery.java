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
package org.apache.lucene.search;


import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestBlendedTermQuery extends LuceneTestCase {

  public void testEquals() {
    Term t1 = new Term("foo", "bar");

    BlendedTermQuery bt1 = new BlendedTermQuery.Builder()
        .add(t1)
        .build();
    BlendedTermQuery bt2 = new BlendedTermQuery.Builder()
        .add(t1)
        .build();
    QueryUtils.checkEqual(bt1, bt2);

    bt1 = new BlendedTermQuery.Builder()
        .setRewriteMethod(BlendedTermQuery.BOOLEAN_REWRITE)
        .add(t1)
        .build();
    bt2 = new BlendedTermQuery.Builder()
        .setRewriteMethod(BlendedTermQuery.DISJUNCTION_MAX_REWRITE)
        .add(t1)
        .build();
    QueryUtils.checkUnequal(bt1, bt2);

    Term t2 = new Term("foo", "baz");

    bt1 = new BlendedTermQuery.Builder()
        .add(t1)
        .add(t2)
        .build();
    bt2 = new BlendedTermQuery.Builder()
        .add(t2)
        .add(t1)
        .build();
    QueryUtils.checkEqual(bt1, bt2);

    float boost1 = random().nextFloat();
    float boost2 = random().nextFloat();
    bt1 = new BlendedTermQuery.Builder()
        .add(t1, boost1)
        .add(t2, boost2)
        .build();
    bt2 = new BlendedTermQuery.Builder()
        .add(t2, boost2)
        .add(t1, boost1)
        .build();
    QueryUtils.checkEqual(bt1, bt2);
  }

  public void testToString() {
    assertEquals("Blended()", new BlendedTermQuery.Builder().build().toString());
    Term t1 = new Term("foo", "bar");
    assertEquals("Blended(foo:bar)", new BlendedTermQuery.Builder().add(t1).build().toString());
    Term t2 = new Term("foo", "baz");
    assertEquals("Blended(foo:bar foo:baz)", new BlendedTermQuery.Builder().add(t1).add(t2).build().toString());
    assertEquals("Blended((foo:bar)^4.0 (foo:baz)^3.0)", new BlendedTermQuery.Builder().add(t1, 4).add(t2, 3).build().toString());
  }

  public void testBlendedScores() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new StringField("f", "a", Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new StringField("f", "b", Store.NO));
    for (int i = 0; i < 10; ++i) {
      w.addDocument(doc);
    }

    IndexReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);
    BlendedTermQuery query = new BlendedTermQuery.Builder()
        .setRewriteMethod(new BlendedTermQuery.DisjunctionMaxRewrite(0f))
        .add(new Term("f", "a"))
        .add(new Term("f", "b"))
        .build();

    TopDocs topDocs = searcher.search(query, 20);
    assertEquals(11, topDocs.totalHits);
    // All docs must have the same score
    for (int i = 0; i < topDocs.scoreDocs.length; ++i) {
      assertEquals(topDocs.scoreDocs[0].score, topDocs.scoreDocs[i].score, 0.0f);
    }

    reader.close();
    w.close();
    dir.close();
  }

}
