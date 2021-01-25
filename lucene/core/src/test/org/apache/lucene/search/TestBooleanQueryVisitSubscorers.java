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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

// TODO: refactor to a base class, that collects freqs from the scorer tree
// and test all queries with it
public class TestBooleanQueryVisitSubscorers extends LuceneTestCase {

  Analyzer analyzer;
  IndexReader reader;
  IndexSearcher searcher;
  IndexSearcher scorerSearcher;
  Directory dir;
  
  static final String F1 = "title";
  static final String F2 = "body";
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new MockAnalyzer(random());
    dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(analyzer);
    config.setMergePolicy(newLogMergePolicy()); // we will use docids to validate
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    writer.addDocument(doc("lucene", "lucene is a very popular search engine library"));
    writer.addDocument(doc("solr", "solr is a very popular search server and is using lucene"));
    writer.addDocument(doc("nutch", "nutch is an internet search engine with web crawler and is using lucene and hadoop"));
    reader = writer.getReader();
    writer.close();
    // we do not use newSearcher because the assertingXXX layers break
    // the toString representations we are relying on
    // TODO: clean that up
    searcher = new IndexSearcher(reader);
    searcher.setSimilarity(new ClassicSimilarity());
    scorerSearcher = new ScorerIndexSearcher(reader);
    scorerSearcher.setSimilarity(new CountingSimilarity());
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }

  public void testDisjunctions() throws IOException {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term(F1, "lucene")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term(F2, "lucene")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term(F2, "search")), BooleanClause.Occur.SHOULD);
    Map<Integer,Integer> tfs = getDocCounts(scorerSearcher, bq.build());
    assertEquals(3, tfs.size()); // 3 documents
    assertEquals(3, tfs.get(0).intValue()); // f1:lucene + f2:lucene + f2:search
    assertEquals(2, tfs.get(1).intValue()); // f2:search + f2:lucene
    assertEquals(2, tfs.get(2).intValue()); // f2:search + f2:lucene
  }
  
  public void testNestedDisjunctions() throws IOException {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term(F1, "lucene")), BooleanClause.Occur.SHOULD);
    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(new TermQuery(new Term(F2, "lucene")), BooleanClause.Occur.SHOULD);
    bq2.add(new TermQuery(new Term(F2, "search")), BooleanClause.Occur.SHOULD);
    bq.add(bq2.build(), BooleanClause.Occur.SHOULD);
    Map<Integer,Integer> tfs = getDocCounts(scorerSearcher, bq.build());
    assertEquals(3, tfs.size()); // 3 documents
    assertEquals(3, tfs.get(0).intValue()); // f1:lucene + f2:lucene + f2:search
    assertEquals(2, tfs.get(1).intValue()); // f2:search + f2:lucene
    assertEquals(2, tfs.get(2).intValue()); // f2:search + f2:lucene
  }
  
  public void testConjunctions() throws IOException {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term(F2, "lucene")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term(F2, "is")), BooleanClause.Occur.MUST);
    Map<Integer,Integer> tfs = getDocCounts(scorerSearcher, bq.build());
    assertEquals(3, tfs.size()); // 3 documents
    assertEquals(2, tfs.get(0).intValue()); // f2:lucene + f2:is
    assertEquals(3, tfs.get(1).intValue()); // f2:is + f2:is + f2:lucene
    assertEquals(3, tfs.get(2).intValue()); // f2:is + f2:is + f2:lucene
  }
  
  static Document doc(String v1, String v2) {
    Document doc = new Document();
    doc.add(new TextField(F1, v1, Store.YES));
    doc.add(new TextField(F2, v2, Store.YES));
    return doc;
  }
  
  static Map<Integer,Integer> getDocCounts(IndexSearcher searcher, Query query) throws IOException {
    MyCollector collector = new MyCollector();
    searcher.search(query, collector);
    return collector.docCounts;
  }
  
  static class MyCollector extends FilterCollector {

    public final Map<Integer,Integer> docCounts = new HashMap<>();
    private final Set<Scorer> tqsSet = new HashSet<>();
    
    MyCollector() {
      super(TopScoreDocCollector.create(10, Integer.MAX_VALUE));
    }

    public LeafCollector getLeafCollector(LeafReaderContext context)
        throws IOException {
      final int docBase = context.docBase;
      return new FilterLeafCollector(super.getLeafCollector(context)) {
        
        @Override
        public void setScorer(Scorable scorer) throws IOException {
          super.setScorer(scorer);
          tqsSet.clear();
          fillLeaves(scorer, tqsSet);
        }
        
        @Override
        public void collect(int doc) throws IOException {
          int freq = 0;
          for(Scorer scorer : tqsSet) {
            if (doc == scorer.docID()) {
              freq += scorer.score();
            }
          }
          docCounts.put(doc + docBase, freq);
          super.collect(doc);
        }
        
      };
    }
    
    private void fillLeaves(Scorable scorer, Set<Scorer> set) throws IOException {
      if (scorer instanceof TermScorer) {
        set.add((Scorer)scorer);
      } else {
        for (Scorable.ChildScorable child : scorer.getChildren()) {
          fillLeaves(child.child, set);
        }
      }
    }
    
    public TopDocs topDocs(){
      return ((TopDocsCollector<?>) in).topDocs();
    }
    
    public int freq(int doc) throws IOException {
      return docCounts.get(doc);
    }

  }

  public void testDisjunctionMatches() throws IOException {
    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    bq1.add(new TermQuery(new Term(F1, "lucene")), Occur.SHOULD);
    bq1.add(new PhraseQuery(F2, "search", "engine"), Occur.SHOULD);

    Weight w1 = scorerSearcher.createWeight(scorerSearcher.rewrite(bq1.build()), ScoreMode.COMPLETE, 1);
    Scorer s1 = w1.scorer(reader.leaves().get(0));
    assertEquals(0, s1.iterator().nextDoc());
    assertEquals(2, s1.getChildren().size());

    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(new TermQuery(new Term(F1, "lucene")), Occur.SHOULD);
    bq2.add(new PhraseQuery(F2, "search", "library"), Occur.SHOULD);

    Weight w2 = scorerSearcher.createWeight(scorerSearcher.rewrite(bq2.build()), ScoreMode.COMPLETE, 1);
    Scorer s2 = w2.scorer(reader.leaves().get(0));
    assertEquals(0, s2.iterator().nextDoc());
    assertEquals(1, s2.getChildren().size());
  }

  public void testMinShouldMatchMatches() throws IOException {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term(F1, "lucene")), Occur.SHOULD);
    bq.add(new TermQuery(new Term(F2, "lucene")), Occur.SHOULD);
    bq.add(new PhraseQuery(F2, "search", "library"), Occur.SHOULD);
    bq.setMinimumNumberShouldMatch(2);

    Weight w = scorerSearcher.createWeight(scorerSearcher.rewrite(bq.build()), ScoreMode.COMPLETE, 1);
    Scorer s = w.scorer(reader.leaves().get(0));
    assertEquals(0, s.iterator().nextDoc());
    assertEquals(2, s.getChildren().size());
  }

  public void testGetChildrenMinShouldMatchSumScorer() throws IOException {
    final BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(F2, "nutch")), Occur.SHOULD);
    query.add(new TermQuery(new Term(F2, "web")), Occur.SHOULD);
    query.add(new TermQuery(new Term(F2, "crawler")), Occur.SHOULD);
    query.setMinimumNumberShouldMatch(2);
    query.add(new MatchAllDocsQuery(), Occur.MUST);
    ScorerSummarizingCollector collector = new ScorerSummarizingCollector();
    searcher.search(query.build(), collector);
    assertEquals(1, collector.getNumHits());
    assertFalse(collector.getSummaries().isEmpty());
    for (String summary : collector.getSummaries()) {
      assertEquals(
          "ConjunctionScorer\n" +
          "    MUST ConstantScoreScorer\n" +
          "    MUST WANDScorer\n" +
          "            SHOULD TermScorer body:crawler\n" +
          "            SHOULD TermScorer body:web\n" +
          "            SHOULD TermScorer body:nutch",
              summary);
    }
  }

  public void testGetChildrenBoosterScorer() throws IOException {
    final BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(F2, "nutch")), Occur.SHOULD);
    query.add(new TermQuery(new Term(F2, "miss")), Occur.SHOULD);
    ScorerSummarizingCollector collector = new ScorerSummarizingCollector();
    scorerSearcher.search(query.build(), collector);
    assertEquals(1, collector.getNumHits());
    assertFalse(collector.getSummaries().isEmpty());
    for (String summary : collector.getSummaries()) {
      assertEquals(
          "TermScorer body:nutch", summary);
    }
  }

  private static class ScorerSummarizingCollector implements Collector {
    private final List<String> summaries = new ArrayList<>();
    private int numHits[] = new int[1];

    public int getNumHits() {
      return numHits[0];
    }

    public List<String> getSummaries() {
      return summaries;
    }
    
    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return new LeafCollector() {

        @Override
        public void setScorer(Scorable scorer) throws IOException {
          final StringBuilder builder = new StringBuilder();
          summarizeScorer(builder, scorer, 0);
          summaries.add(builder.toString());
        }

        @Override
        public void collect(int doc) throws IOException {
          numHits[0]++;
        }
      };
    }

    private static void summarizeScorer(final StringBuilder builder, final Scorable scorer, final int indent) throws IOException {
      builder.append(scorer.getClass().getSimpleName());
      if (scorer instanceof TermScorer) {
        TermQuery termQuery = (TermQuery) ((Scorer)scorer).getWeight().getQuery();
        builder.append(" ").append(termQuery.getTerm().field()).append(":").append(termQuery.getTerm().text());
      }
      for (final Scorable.ChildScorable childScorer : scorer.getChildren()) {
        indent(builder, indent + 1).append(childScorer.relationship).append(" ");
        summarizeScorer(builder, childScorer.child, indent + 2);
      }
    }

    private static StringBuilder indent(final StringBuilder builder, final int indent) {
      if (builder.length() != 0) {
        builder.append("\n");
      }
      for (int i = 0; i < indent; i++) {
        builder.append("    ");
      }
      return builder;
    }
  }

  // Similarity that just returns the frequency as the score
  private static class CountingSimilarity extends Similarity {

    @Override
    public long computeNorm(FieldInvertState state) {
      return 1;
    }

    @Override
    public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      return new SimScorer() {
        @Override
        public float score(float freq, long norm) {
          return freq;
        }
      };
    }
  }

}
