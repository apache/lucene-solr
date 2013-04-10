package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Scorer.ChildScorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

// TODO: refactor to a base class, that collects freqs from the scorer tree
// and test all queries with it
public class TestBooleanQueryVisitSubscorers extends LuceneTestCase {
  Analyzer analyzer;
  IndexReader reader;
  IndexSearcher searcher;
  Directory dir;
  
  static final String F1 = "title";
  static final String F2 = "body";
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new MockAnalyzer(random());
    dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    config.setMergePolicy(newLogMergePolicy()); // we will use docids to validate
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    writer.addDocument(doc("lucene", "lucene is a very popular search engine library"));
    writer.addDocument(doc("solr", "solr is a very popular search server and is using lucene"));
    writer.addDocument(doc("nutch", "nutch is an internet search engine with web crawler and is using lucene and hadoop"));
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }

  public void testDisjunctions() throws IOException {
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term(F1, "lucene")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term(F2, "lucene")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term(F2, "search")), BooleanClause.Occur.SHOULD);
    Map<Integer,Integer> tfs = getDocCounts(searcher, bq);
    assertEquals(3, tfs.size()); // 3 documents
    assertEquals(3, tfs.get(0).intValue()); // f1:lucene + f2:lucene + f2:search
    assertEquals(2, tfs.get(1).intValue()); // f2:search + f2:lucene
    assertEquals(2, tfs.get(2).intValue()); // f2:search + f2:lucene
  }
  
  public void testNestedDisjunctions() throws IOException {
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term(F1, "lucene")), BooleanClause.Occur.SHOULD);
    BooleanQuery bq2 = new BooleanQuery();
    bq2.add(new TermQuery(new Term(F2, "lucene")), BooleanClause.Occur.SHOULD);
    bq2.add(new TermQuery(new Term(F2, "search")), BooleanClause.Occur.SHOULD);
    bq.add(bq2, BooleanClause.Occur.SHOULD);
    Map<Integer,Integer> tfs = getDocCounts(searcher, bq);
    assertEquals(3, tfs.size()); // 3 documents
    assertEquals(3, tfs.get(0).intValue()); // f1:lucene + f2:lucene + f2:search
    assertEquals(2, tfs.get(1).intValue()); // f2:search + f2:lucene
    assertEquals(2, tfs.get(2).intValue()); // f2:search + f2:lucene
  }
  
  public void testConjunctions() throws IOException {
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term(F2, "lucene")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term(F2, "is")), BooleanClause.Occur.MUST);
    Map<Integer,Integer> tfs = getDocCounts(searcher, bq);
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
  
  static class MyCollector extends Collector {
    
    private TopDocsCollector<ScoreDoc> collector;
    private int docBase;

    public final Map<Integer,Integer> docCounts = new HashMap<Integer,Integer>();
    private final Set<Scorer> tqsSet = new HashSet<Scorer>();
    
    MyCollector() {
      collector = TopScoreDocCollector.create(10, true);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return false;
    }

    @Override
    public void collect(int doc) throws IOException {
      int freq = 0;
      for(Scorer scorer : tqsSet) {
        if (doc == scorer.docID()) {
          freq += scorer.freq();
        }
      }
      docCounts.put(doc + docBase, freq);
      collector.collect(doc);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.docBase = context.docBase;
      collector.setNextReader(context);
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      collector.setScorer(scorer);
      tqsSet.clear();
      fillLeaves(scorer, tqsSet);
    }
    
    private void fillLeaves(Scorer scorer, Set<Scorer> set) {
      if (scorer.getWeight().getQuery() instanceof TermQuery) {
        set.add(scorer);
      } else {
        for (ChildScorer child : scorer.getChildren()) {
          fillLeaves(child.child, set);
        }
      }
    }
    
    public TopDocs topDocs(){
      return collector.topDocs();
    }
    
    public int freq(int doc) throws IOException {
      return docCounts.get(doc);
    }
  }
}
