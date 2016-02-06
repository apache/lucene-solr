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


import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestTopDocsMerge extends LuceneTestCase {

  private static class ShardSearcher extends IndexSearcher {
    private final List<LeafReaderContext> ctx;

    public ShardSearcher(LeafReaderContext ctx, IndexReaderContext parent) {
      super(parent);
      this.ctx = Collections.singletonList(ctx);
    }

    public void search(Weight weight, Collector collector) throws IOException {
      search(ctx, weight, collector);
    }

    public TopDocs search(Weight weight, int topN) throws IOException {
      TopScoreDocCollector collector = TopScoreDocCollector.create(topN);
      search(ctx, weight, collector);
      return collector.topDocs();    }

    @Override
    public String toString() {
      return "ShardSearcher(" + ctx.get(0) + ")";
    }
  }

  public void testSort_1() throws Exception {
    testSort(false);
  }

  public void testSort_2() throws Exception {
    testSort(true);
  }

  void testSort(boolean useFrom) throws Exception {

    IndexReader reader = null;
    Directory dir = null;

    final int numDocs = TEST_NIGHTLY ? atLeast(1000) : atLeast(100);

    final String[] tokens = new String[] {"a", "b", "c", "d", "e"};

    if (VERBOSE) {
      System.out.println("TEST: make index");
    }

    {
      dir = newDirectory();
      final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
      // w.setDoRandomForceMerge(false);

      // w.w.getConfig().setMaxBufferedDocs(atLeast(100));

      final String[] content = new String[atLeast(20)];

      for(int contentIDX=0;contentIDX<content.length;contentIDX++) {
        final StringBuilder sb = new StringBuilder();
        final int numTokens = TestUtil.nextInt(random(), 1, 10);
        for(int tokenIDX=0;tokenIDX<numTokens;tokenIDX++) {
          sb.append(tokens[random().nextInt(tokens.length)]).append(' ');
        }
        content[contentIDX] = sb.toString();
      }

      for(int docIDX=0;docIDX<numDocs;docIDX++) {
        final Document doc = new Document();
        doc.add(new SortedDocValuesField("string", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
        doc.add(newTextField("text", content[random().nextInt(content.length)], Field.Store.NO));
        doc.add(new FloatDocValuesField("float", random().nextFloat()));
        final int intValue;
        if (random().nextInt(100) == 17) {
          intValue = Integer.MIN_VALUE;
        } else if (random().nextInt(100) == 17) {
          intValue = Integer.MAX_VALUE;
        } else {
          intValue = random().nextInt();
        }
        doc.add(new NumericDocValuesField("int", intValue));
        if (VERBOSE) {
          System.out.println("  doc=" + doc);
        }
        w.addDocument(doc);
      }

      reader = w.getReader();
      w.close();
    }

    // NOTE: sometimes reader has just one segment, which is
    // important to test
    final IndexSearcher searcher = newSearcher(reader);
    final IndexReaderContext ctx = searcher.getTopReaderContext();

    final ShardSearcher[] subSearchers;
    final int[] docStarts;

    if (ctx instanceof LeafReaderContext) {
      subSearchers = new ShardSearcher[1];
      docStarts = new int[1];
      subSearchers[0] = new ShardSearcher((LeafReaderContext) ctx, ctx);
      docStarts[0] = 0;
    } else {
      final CompositeReaderContext compCTX = (CompositeReaderContext) ctx;
      final int size = compCTX.leaves().size();
      subSearchers = new ShardSearcher[size];
      docStarts = new int[size];
      int docBase = 0;
      for(int searcherIDX=0;searcherIDX<subSearchers.length;searcherIDX++) {
        final LeafReaderContext leave = compCTX.leaves().get(searcherIDX);
        subSearchers[searcherIDX] = new ShardSearcher(leave, compCTX);
        docStarts[searcherIDX] = docBase;
        docBase += leave.reader().maxDoc();
      }
    }

    final List<SortField> sortFields = new ArrayList<>();
    sortFields.add(new SortField("string", SortField.Type.STRING, true));
    sortFields.add(new SortField("string", SortField.Type.STRING, false));
    sortFields.add(new SortField("int", SortField.Type.INT, true));
    sortFields.add(new SortField("int", SortField.Type.INT, false));
    sortFields.add(new SortField("float", SortField.Type.FLOAT, true));
    sortFields.add(new SortField("float", SortField.Type.FLOAT, false));
    sortFields.add(new SortField(null, SortField.Type.SCORE, true));
    sortFields.add(new SortField(null, SortField.Type.SCORE, false));
    sortFields.add(new SortField(null, SortField.Type.DOC, true));
    sortFields.add(new SortField(null, SortField.Type.DOC, false));

    int numIters = atLeast(300); 
    for(int iter=0;iter<numIters;iter++) {

      // TODO: custom FieldComp...
      final Query query = new TermQuery(new Term("text", tokens[random().nextInt(tokens.length)]));

      final Sort sort;
      if (random().nextInt(10) == 4) {
        // Sort by score
        sort = null;
      } else {
        final SortField[] randomSortFields = new SortField[TestUtil.nextInt(random(), 1, 3)];
        for(int sortIDX=0;sortIDX<randomSortFields.length;sortIDX++) {
          randomSortFields[sortIDX] = sortFields.get(random().nextInt(sortFields.size()));
        }
        sort = new Sort(randomSortFields);
      }

      final int numHits = TestUtil.nextInt(random(), 1, numDocs + 5);
      //final int numHits = 5;

      if (VERBOSE) {
        System.out.println("TEST: search query=" + query + " sort=" + sort + " numHits=" + numHits);
      }

      int from = -1;
      int size = -1;
      // First search on whole index:
      final TopDocs topHits;
      if (sort == null) {
        if (useFrom) {
          TopScoreDocCollector c = TopScoreDocCollector.create(numHits);
          searcher.search(query, c);
          from = TestUtil.nextInt(random(), 0, numHits - 1);
          size = numHits - from;
          TopDocs tempTopHits = c.topDocs();
          if (from < tempTopHits.scoreDocs.length) {
            // Can't use TopDocs#topDocs(start, howMany), since it has different behaviour when start >= hitCount
            // than TopDocs#merge currently has
            ScoreDoc[] newScoreDocs = new ScoreDoc[Math.min(size, tempTopHits.scoreDocs.length - from)];
            System.arraycopy(tempTopHits.scoreDocs, from, newScoreDocs, 0, newScoreDocs.length);
            tempTopHits.scoreDocs = newScoreDocs;
            topHits = tempTopHits;
          } else {
            topHits = new TopDocs(tempTopHits.totalHits, new ScoreDoc[0], tempTopHits.getMaxScore());
          }
        } else {
          topHits = searcher.search(query, numHits);
        }
      } else {
        final TopFieldCollector c = TopFieldCollector.create(sort, numHits, true, true, true);
        searcher.search(query, c);
        if (useFrom) {
          from = TestUtil.nextInt(random(), 0, numHits - 1);
          size = numHits - from;
          TopDocs tempTopHits = c.topDocs();
          if (from < tempTopHits.scoreDocs.length) {
            // Can't use TopDocs#topDocs(start, howMany), since it has different behaviour when start >= hitCount
            // than TopDocs#merge currently has
            ScoreDoc[] newScoreDocs = new ScoreDoc[Math.min(size, tempTopHits.scoreDocs.length - from)];
            System.arraycopy(tempTopHits.scoreDocs, from, newScoreDocs, 0, newScoreDocs.length);
            tempTopHits.scoreDocs = newScoreDocs;
            topHits = tempTopHits;
          } else {
            topHits = new TopDocs(tempTopHits.totalHits, new ScoreDoc[0], tempTopHits.getMaxScore());
          }
        } else {
          topHits = c.topDocs(0, numHits);
        }
      }

      if (VERBOSE) {
        if (useFrom) {
          System.out.println("from=" + from + " size=" + size);
        }
        System.out.println("  top search: " + topHits.totalHits + " totalHits; hits=" + (topHits.scoreDocs == null ? "null" : topHits.scoreDocs.length + " maxScore=" + topHits.getMaxScore()));
        if (topHits.scoreDocs != null) {
          for(int hitIDX=0;hitIDX<topHits.scoreDocs.length;hitIDX++) {
            final ScoreDoc sd = topHits.scoreDocs[hitIDX];
            System.out.println("    doc=" + sd.doc + " score=" + sd.score);
          }
        }
      }

      // ... then all shards:
      final Weight w = searcher.createNormalizedWeight(query, true);

      final TopDocs[] shardHits;
      if (sort == null) {
        shardHits = new TopDocs[subSearchers.length];
      } else {
        shardHits = new TopFieldDocs[subSearchers.length];
      }
      for(int shardIDX=0;shardIDX<subSearchers.length;shardIDX++) {
        final TopDocs subHits;
        final ShardSearcher subSearcher = subSearchers[shardIDX];
        if (sort == null) {
          subHits = subSearcher.search(w, numHits);
        } else {
          final TopFieldCollector c = TopFieldCollector.create(sort, numHits, true, true, true);
          subSearcher.search(w, c);
          subHits = c.topDocs(0, numHits);
        }

        shardHits[shardIDX] = subHits;
        if (VERBOSE) {
          System.out.println("  shard=" + shardIDX + " " + subHits.totalHits + " totalHits hits=" + (subHits.scoreDocs == null ? "null" : subHits.scoreDocs.length));
          if (subHits.scoreDocs != null) {
            for(ScoreDoc sd : subHits.scoreDocs) {
              System.out.println("    doc=" + sd.doc + " score=" + sd.score);
            }
          }
        }
      }

      // Merge:
      final TopDocs mergedHits;
      if (useFrom) {
        if (sort == null) {
          mergedHits = TopDocs.merge(from, size, shardHits);
        } else {
          mergedHits = TopDocs.merge(sort, from, size, (TopFieldDocs[]) shardHits);
        }
      } else {
        if (sort == null) {
          mergedHits = TopDocs.merge(numHits, shardHits);
        } else {
          mergedHits = TopDocs.merge(sort, numHits, (TopFieldDocs[]) shardHits);
        }
      }

      if (mergedHits.scoreDocs != null) {
        // Make sure the returned shards are correct:
        for(int hitIDX=0;hitIDX<mergedHits.scoreDocs.length;hitIDX++) {
          final ScoreDoc sd = mergedHits.scoreDocs[hitIDX];
          assertEquals("doc=" + sd.doc + " wrong shard",
                       ReaderUtil.subIndex(sd.doc, docStarts),
                       sd.shardIndex);
        }
      }

      TestUtil.assertEquals(topHits, mergedHits);
    }
    reader.close();
    dir.close();
  }
}
