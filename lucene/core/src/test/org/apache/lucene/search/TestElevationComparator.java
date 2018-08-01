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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldValueHitQueue.Entry;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestElevationComparator extends LuceneTestCase {

  private final Map<BytesRef,Integer> priority = new HashMap<>();

  //@Test
  public void testSorting() throws Throwable {
    Directory directory = newDirectory();
    IndexWriter writer = new IndexWriter(
        directory,
        newIndexWriterConfig(new MockAnalyzer(random())).
            setMaxBufferedDocs(2).
            setMergePolicy(newLogMergePolicy(1000)).
            setSimilarity(new ClassicSimilarity())
    );
    writer.addDocument(adoc(new String[] {"id", "a", "title", "ipod", "str_s", "a"}));
    writer.addDocument(adoc(new String[] {"id", "b", "title", "ipod ipod", "str_s", "b"}));
    writer.addDocument(adoc(new String[] {"id", "c", "title", "ipod ipod ipod", "str_s","c"}));
    writer.addDocument(adoc(new String[] {"id", "x", "title", "boosted", "str_s", "x"}));
    writer.addDocument(adoc(new String[] {"id", "y", "title", "boosted boosted", "str_s","y"}));
    writer.addDocument(adoc(new String[] {"id", "z", "title", "boosted boosted boosted","str_s", "z"}));

    IndexReader r = DirectoryReader.open(writer);
    writer.close();

    IndexSearcher searcher = newSearcher(r);
    searcher.setSimilarity(new BM25Similarity());

    runTest(searcher, true);
    runTest(searcher, false);

    r.close();
    directory.close();
  }

  private void runTest(IndexSearcher searcher, boolean reversed) throws Throwable {

    BooleanQuery.Builder newq = new BooleanQuery.Builder();
    TermQuery query = new TermQuery(new Term("title", "ipod"));

    newq.add(query, BooleanClause.Occur.SHOULD);
    newq.add(getElevatedQuery(new String[] {"id", "a", "id", "x"}), BooleanClause.Occur.SHOULD);

    Sort sort = new Sort(
        new SortField("id", new ElevationComparatorSource(priority), false),
        new SortField(null, SortField.Type.SCORE, reversed)
      );

    TopDocsCollector<Entry> topCollector = TopFieldCollector.create(sort, 50, Integer.MAX_VALUE);
    searcher.search(newq.build(), topCollector);

    TopDocs topDocs = topCollector.topDocs(0, 10);
    int nDocsReturned = topDocs.scoreDocs.length;

    assertEquals(4, nDocsReturned);

    // 0 & 3 were elevated
    assertEquals(0, topDocs.scoreDocs[0].doc);
    assertEquals(3, topDocs.scoreDocs[1].doc);

    if (reversed) {
      assertEquals(1, topDocs.scoreDocs[2].doc);
      assertEquals(2, topDocs.scoreDocs[3].doc);
    } else {
      assertEquals(2, topDocs.scoreDocs[2].doc);
      assertEquals(1, topDocs.scoreDocs[3].doc);
    }

    /*
    for (int i = 0; i < nDocsReturned; i++) {
     ScoreDoc scoreDoc = topDocs.scoreDocs[i];
     ids[i] = scoreDoc.doc;
     scores[i] = scoreDoc.score;
     documents[i] = searcher.doc(ids[i]);
     System.out.println("ids[i] = " + ids[i]);
     System.out.println("documents[i] = " + documents[i]);
     System.out.println("scores[i] = " + scores[i]);
   }
    */
 }

 private Query getElevatedQuery(String[] vals) {
   BooleanQuery.Builder b = new BooleanQuery.Builder();
   int max = (vals.length / 2) + 5;
   for (int i = 0; i < vals.length - 1; i += 2) {
     b.add(new TermQuery(new Term(vals[i], vals[i + 1])), BooleanClause.Occur.SHOULD);
     priority.put(new BytesRef(vals[i + 1]), Integer.valueOf(max--));
     // System.out.println(" pri doc=" + vals[i+1] + " pri=" + (1+max));
   }
   BooleanQuery q = b.build();
   return new BoostQuery(q, 0f);
 }

 private Document adoc(String[] vals) {
   Document doc = new Document();
   for (int i = 0; i < vals.length - 2; i += 2) {
     doc.add(newTextField(vals[i], vals[i + 1], Field.Store.YES));
     if (vals[i].equals("id")) {
       doc.add(new SortedDocValuesField(vals[i], new BytesRef(vals[i+1])));
     }
   }
   return doc;
 }
}

class ElevationComparatorSource extends FieldComparatorSource {
  private final Map<BytesRef,Integer> priority;

  public ElevationComparatorSource(final Map<BytesRef,Integer> boosts) {
   this.priority = boosts;
  }

  @Override
  public FieldComparator<Integer> newComparator(final String fieldname, final int numHits, int sortPos, boolean reversed) {
   return new FieldComparator<Integer>() {

     private final int[] values = new int[numHits];
     int bottomVal;

     @Override
     public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
      return new LeafFieldComparator() {

        @Override
        public void setBottom(int slot) {
          bottomVal = values[slot];
        }

        @Override
        public int compareTop(int doc) {
          throw new UnsupportedOperationException();
        }

        private int docVal(int doc) throws IOException {
          SortedDocValues idIndex = DocValues.getSorted(context.reader(), fieldname);
          if (idIndex.advance(doc) == doc) {
            final BytesRef term = idIndex.binaryValue();
            Integer prio = priority.get(term);
            return prio == null ? 0 : prio.intValue();
          } else {
            return 0;
          }
        }

        @Override
        public int compareBottom(int doc) throws IOException {
          return docVal(doc) - bottomVal;
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
          values[slot] = docVal(doc);
        }

        @Override
        public void setScorer(Scorer scorer) {}
      };
    }

     @Override
     public int compare(int slot1, int slot2) {
       return values[slot2] - values[slot1];  // values will be small enough that there is no overflow concern
     }

     @Override
     public void setTopValue(Integer value) {
       throw new UnsupportedOperationException();
     }

     @Override
     public Integer value(int slot) {
       return Integer.valueOf(values[slot]);
     }


   };
 }
}
