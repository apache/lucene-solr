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

package org.apache.lucene.search.join;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestParentChildrenBlockJoinQuery extends LuceneTestCase {

  public void testParentChildrenBlockJoinQuery() throws Exception {
    int numParentDocs = 8 + random().nextInt(8);
    int maxChildDocsPerParent = 8 + random().nextInt(8);

    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < numParentDocs; i++) {
      int numChildDocs = random().nextInt(maxChildDocsPerParent);
      List<Document> docs = new ArrayList<>(numChildDocs + 1);
      for (int j = 0; j < numChildDocs; j++) {
        Document childDoc = new Document();
        childDoc.add(new StringField("type", "child", Field.Store.NO));
        childDoc.add(new NumericDocValuesField("score", j + 1));
        docs.add(childDoc);
      }

      Document parenDoc = new Document();
      parenDoc.add(new StringField("type", "parent", Field.Store.NO));
      parenDoc.add(new NumericDocValuesField("num_child_docs", numChildDocs));
      docs.add(parenDoc);
      writer.addDocuments(docs);
    }

    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader);
    BitSetProducer parentFilter = new QueryBitSetProducer(new TermQuery(new Term("type", "parent")));
    Query childQuery = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("type", "child")), BooleanClause.Occur.FILTER)
        .add(TestJoinUtil.numericDocValuesScoreQuery("score"), BooleanClause.Occur.SHOULD)
        .build();

    TopDocs parentDocs = searcher.search(new TermQuery(new Term("type", "parent")), numParentDocs);
    assertEquals(parentDocs.scoreDocs.length, numParentDocs);
    for (ScoreDoc parentScoreDoc : parentDocs.scoreDocs) {
      LeafReaderContext leafReader = reader.leaves().get(ReaderUtil.subIndex(parentScoreDoc.doc, reader.leaves()));
      NumericDocValues numericDocValuesField = leafReader.reader().getNumericDocValues("num_child_docs");
      long expectedChildDocs = numericDocValuesField.get(parentScoreDoc.doc - leafReader.docBase);

      ParentChildrenBlockJoinQuery parentChildrenBlockJoinQuery =
          new ParentChildrenBlockJoinQuery(parentFilter, childQuery, parentScoreDoc.doc);
      TopDocs topDocs = searcher.search(parentChildrenBlockJoinQuery, maxChildDocsPerParent);
      assertEquals(expectedChildDocs, topDocs.totalHits);
      if (expectedChildDocs > 0) {
        assertEquals(expectedChildDocs, topDocs.getMaxScore(), 0);
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
          ScoreDoc childScoreDoc = topDocs.scoreDocs[i];
          assertEquals(expectedChildDocs - i, childScoreDoc.score, 0);
        }
      }

    }

    reader.close();
    dir.close();
  }

}
