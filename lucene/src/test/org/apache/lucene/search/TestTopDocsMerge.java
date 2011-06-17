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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util._TestUtil;

public class TestTopDocsMerge extends LuceneTestCase {

  private static class ShardSearcher extends IndexSearcher {
    private final IndexReader.AtomicReaderContext[] ctx;

    public ShardSearcher(IndexReader.AtomicReaderContext ctx, IndexReader.ReaderContext parent) {
      super(parent);
      this.ctx = new IndexReader.AtomicReaderContext[] {ctx};
    }

    public void search(Weight weight, Collector collector) throws IOException {
      search(ctx, weight, null, collector);
    }

    public TopDocs search(Weight weight, int topN) throws IOException {
      return search(ctx, weight, null, topN);
    }

    @Override
    public String toString() {
      return "ShardSearcher(" + ctx[0] + ")";
    }
  }

  public void testSort() throws Exception {

    IndexReader reader = null;
    Directory dir = null;

    final int numDocs = atLeast(1000);
    //final int numDocs = atLeast(50);

    final String[] tokens = new String[] {"a", "b", "c", "d", "e"};

    if (VERBOSE) {
      System.out.println("TEST: make index");
    }

    {
      dir = newDirectory();
      final RandomIndexWriter w = new RandomIndexWriter(random, dir);
      // w.setDoRandomOptimize(false);

      // w.w.getConfig().setMaxBufferedDocs(atLeast(100));

      final String[] content = new String[atLeast(20)];

      for(int contentIDX=0;contentIDX<content.length;contentIDX++) {
        final StringBuilder sb = new StringBuilder();
        final int numTokens = _TestUtil.nextInt(random, 1, 10);
        for(int tokenIDX=0;tokenIDX<numTokens;tokenIDX++) {
          sb.append(tokens[random.nextInt(tokens.length)]).append(' ');
        }
        content[contentIDX] = sb.toString();
      }

      for(int docIDX=0;docIDX<numDocs;docIDX++) {
        final Document doc = new Document();
        doc.add(newField("string", _TestUtil.randomRealisticUnicodeString(random), Field.Index.NOT_ANALYZED));
        doc.add(newField("text", content[random.nextInt(content.length)], Field.Index.ANALYZED));
        doc.add(new NumericField("float").setFloatValue(random.nextFloat()));
        final int intValue;
        if (random.nextInt(100) == 17) {
          intValue = Integer.MIN_VALUE;
        } else if (random.nextInt(100) == 17) {
          intValue = Integer.MAX_VALUE;
        } else {
          intValue = random.nextInt();
        }
        doc.add(new NumericField("int").setIntValue(intValue));
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
    IndexReader[] subReaders = searcher.getIndexReader().getSequentialSubReaders();
    if (subReaders == null) {
      subReaders = new IndexReader[] {searcher.getIndexReader()};
    }
    final ShardSearcher[] subSearchers = new ShardSearcher[subReaders.length];
    final IndexReader.ReaderContext ctx = searcher.getTopReaderContext();

    if (ctx instanceof IndexReader.AtomicReaderContext) {
      assert subSearchers.length == 1;
      subSearchers[0] = new ShardSearcher((IndexReader.AtomicReaderContext) ctx, ctx);
    } else {
      final IndexReader.CompositeReaderContext compCTX = (IndexReader.CompositeReaderContext) ctx;
      for(int searcherIDX=0;searcherIDX<subSearchers.length;searcherIDX++) { 
        subSearchers[searcherIDX] = new ShardSearcher(compCTX.leaves[searcherIDX], compCTX);
      }
    }

    final List<SortField> sortFields = new ArrayList<SortField>();
    sortFields.add(new SortField("string", SortField.STRING, true));
    sortFields.add(new SortField("string", SortField.STRING, false));
    sortFields.add(new SortField("int", SortField.INT, true));
    sortFields.add(new SortField("int", SortField.INT, false));
    sortFields.add(new SortField("float", SortField.FLOAT, true));
    sortFields.add(new SortField("float", SortField.FLOAT, false));
    sortFields.add(new SortField(null, SortField.SCORE, true));
    sortFields.add(new SortField(null, SortField.SCORE, false));
    sortFields.add(new SortField(null, SortField.DOC, true));
    sortFields.add(new SortField(null, SortField.DOC, false));

    final int[] docStarts = new int[subSearchers.length];
    int docBase = 0;
    for(int subIDX=0;subIDX<docStarts.length;subIDX++) {
      docStarts[subIDX] = docBase;
      docBase += subReaders[subIDX].maxDoc();
      //System.out.println("docStarts[" + subIDX + "]=" + docStarts[subIDX]);
    }

    for(int iter=0;iter<1000*RANDOM_MULTIPLIER;iter++) {

      // TODO: custom FieldComp...
      final Query query = new TermQuery(new Term("text", tokens[random.nextInt(tokens.length)]));

      final Sort sort;
      if (random.nextInt(10) == 4) {
        // Sort by score
        sort = null;
      } else {
        final SortField[] randomSortFields = new SortField[_TestUtil.nextInt(random, 1, 3)];
        for(int sortIDX=0;sortIDX<randomSortFields.length;sortIDX++) {
          randomSortFields[sortIDX] = sortFields.get(random.nextInt(sortFields.size()));
        }
        sort = new Sort(randomSortFields);
      }

      final int numHits = _TestUtil.nextInt(random, 1, numDocs+5);
      //final int numHits = 5;
      
      if (VERBOSE) {
        System.out.println("TEST: search query=" + query + " sort=" + sort + " numHits=" + numHits);
      }

      // First search on whole index:
      final TopDocs topHits;
      if (sort == null) {
        topHits = searcher.search(query, numHits);
      } else {
        final TopFieldCollector c = TopFieldCollector.create(sort, numHits, true, true, true, random.nextBoolean());
        searcher.search(query, c);
        topHits = c.topDocs(0, numHits);
      }

      if (VERBOSE) {
        System.out.println("  top search: " + topHits.totalHits + " totalHits; hits=" + (topHits.scoreDocs == null ? "null" : topHits.scoreDocs.length));
        if (topHits.scoreDocs != null) {
          for(int hitIDX=0;hitIDX<topHits.scoreDocs.length;hitIDX++) {
            final ScoreDoc sd = topHits.scoreDocs[hitIDX];
            System.out.println("    doc=" + sd.doc + " score=" + sd.score);
          }
        }
      }

      // ... then all shards:
      final Weight w = searcher.createNormalizedWeight(query);

      final TopDocs[] shardHits = new TopDocs[subSearchers.length];
      for(int shardIDX=0;shardIDX<subSearchers.length;shardIDX++) {
        final TopDocs subHits;
        final ShardSearcher subSearcher = subSearchers[shardIDX];
        if (sort == null) {
          subHits = subSearcher.search(w, numHits);
        } else {
          final TopFieldCollector c = TopFieldCollector.create(sort, numHits, true, true, true, random.nextBoolean());
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
      final TopDocs mergedHits = TopDocs.merge(sort, numHits, shardHits);

      if (mergedHits.scoreDocs != null) {
        // Make sure the returned shards are correct:
        for(int hitIDX=0;hitIDX<mergedHits.scoreDocs.length;hitIDX++) {
          final ScoreDoc sd = mergedHits.scoreDocs[hitIDX];
          assertEquals("doc=" + sd.doc + " wrong shard",
                       ReaderUtil.subIndex(sd.doc, docStarts),
                       sd.shardIndex);
        }
      }

      _TestUtil.assertEquals(topHits, mergedHits);
    }
    searcher.close();
    reader.close();
    dir.close();
  }
}
