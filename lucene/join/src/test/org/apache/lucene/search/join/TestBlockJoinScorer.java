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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.LuceneTestCase;

public class TestBlockJoinScorer extends LuceneTestCase {
  public void testScoreNone() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig().setMergePolicy(
          // retain doc id order
          newLogMergePolicy(random().nextBoolean())
        )
    );
    List<Document> docs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      docs.clear();
      for (int j = 0; j < i; j++) {
        Document child = new Document();
        child.add(newStringField("value", Integer.toString(j), Field.Store.YES));
        docs.add(child);
      }
      Document parent = new Document();
      parent.add(newStringField("docType", "parent", Field.Store.NO));
      parent.add(newStringField("value", Integer.toString(i), Field.Store.NO));
      docs.add(parent);
      w.addDocuments(docs);
    }
    w.forceMerge(1);

    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    // Create a filter that defines "parent" documents in the index - in this case resumes
    BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "parent")));
    CheckJoinIndex.check(reader, parentsFilter);

    Query childQuery = new MatchAllDocsQuery();
    ToParentBlockJoinQuery query = new ToParentBlockJoinQuery(childQuery, parentsFilter,
        org.apache.lucene.search.join.ScoreMode.None);

    Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1);
    LeafReaderContext context = searcher.getIndexReader().leaves().get(0);

    Scorer scorer = weight.scorer(context);
    BitSet bits = parentsFilter.getBitSet(reader.leaves().get(0));
    int parent = 0;
    for (int i = 0; i < 9; i++) {
      parent = bits.nextSetBit(parent + 1);
      assertEquals(parent, scorer.iterator().nextDoc());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = weight.scorer(context);
    scorer.setMinCompetitiveScore(0f);
    parent = 0;
    for (int i = 0; i < 9; i++) {
      parent = bits.nextSetBit(parent + 1);
      assertEquals(parent, scorer.iterator().nextDoc());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = weight.scorer(context);
    scorer.setMinCompetitiveScore(Math.nextUp(0f));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = weight.scorer(context);
    assertEquals(2, scorer.iterator().nextDoc());
    scorer.setMinCompetitiveScore(Math.nextUp(0f));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    reader.close();
    dir.close();
  }
}