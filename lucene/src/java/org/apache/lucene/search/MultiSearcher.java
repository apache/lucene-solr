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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.DummyConcurrentLock;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

/** Implements search over a set of <code>Searchables</code>.
 *
 * <p>Applications usually need only call the inherited {@link #search(Query,int)}
 * or {@link #search(Query,Filter,int)} methods.
 *
 * @deprecated If you are using MultiSearcher over
 * IndexSearchers, please use MultiReader instead; this class
 * does not properly handle certain kinds of queries (see <a
 * href="https://issues.apache.org/jira/browse/LUCENE-2756">LUCENE-2756</a>).
 */
@Deprecated
public class MultiSearcher extends Searcher {
  
  /**
   * Document Frequency cache acting as a Dummy-Searcher. This class is no
   * full-fledged Searcher, but only supports the methods necessary to
   * initialize Weights.
   */
  private static class CachedDfSource extends Searcher {
    private final Map<Term,Integer> dfMap; // Map from Terms to corresponding doc freqs
    private final int maxDoc; // document count

    public CachedDfSource(Map<Term,Integer> dfMap, int maxDoc, Similarity similarity) {
      this.dfMap = dfMap;
      this.maxDoc = maxDoc;
      setSimilarity(similarity);
    }

    @Override
    public int docFreq(Term term) {
      int df;
      try {
        df = dfMap.get(term).intValue();
      } catch (NullPointerException e) {
        throw new IllegalArgumentException("df for term " + term.text()
            + " not available");
      }
      return df;
    }

    @Override
    public int[] docFreqs(Term[] terms) {
      final int[] result = new int[terms.length];
      for (int i = 0; i < terms.length; i++) {
        result[i] = docFreq(terms[i]);
      }
      return result;
    }

    @Override
    public int maxDoc() {
      return maxDoc;
    }

    @Override
    public Query rewrite(Query query) {
      // this is a bit of a hack. We know that a query which
      // creates a Weight based on this Dummy-Searcher is
      // always already rewritten (see preparedWeight()).
      // Therefore we just return the unmodified query here
      return query;
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Document doc(int i) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public Document doc(int i, FieldSelector fieldSelector) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Explanation explain(Weight weight,int doc) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void search(Weight weight, Filter filter, Collector results) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public TopDocs search(Weight weight,Filter filter,int n) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TopFieldDocs search(Weight weight,Filter filter,int n,Sort sort) {
      throw new UnsupportedOperationException();
    }
  }

  private Searchable[] searchables;
  private int[] starts;
  private int maxDoc = 0;

  /** Creates a searcher which searches <i>searchers</i>. */
  public MultiSearcher(Searchable... searchables) throws IOException {
    this.searchables = searchables;

    starts = new int[searchables.length + 1];	  // build starts array
    for (int i = 0; i < searchables.length; i++) {
      starts[i] = maxDoc;
      maxDoc += searchables[i].maxDoc();          // compute maxDocs
    }
    starts[searchables.length] = maxDoc;
  }
  
  /** Return the array of {@link Searchable}s this searches. */
  public Searchable[] getSearchables() {
    return searchables;
  }
  
  protected int[] getStarts() {
  	return starts;
  }

  // inherit javadoc
  @Override
  public void close() throws IOException {
    for (int i = 0; i < searchables.length; i++)
      searchables[i].close();
  }

  @Override
  public int docFreq(Term term) throws IOException {
    int docFreq = 0;
    for (int i = 0; i < searchables.length; i++)
      docFreq += searchables[i].docFreq(term);
    return docFreq;
  }

  // inherit javadoc
  @Override
  public Document doc(int n) throws CorruptIndexException, IOException {
    int i = subSearcher(n);			  // find searcher index
    return searchables[i].doc(n - starts[i]);	  // dispatch to searcher
  }

  // inherit javadoc
  @Override
  public Document doc(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
    int i = subSearcher(n);			  // find searcher index
    return searchables[i].doc(n - starts[i], fieldSelector);	  // dispatch to searcher
  }
  
  /** Returns index of the searcher for document <code>n</code> in the array
   * used to construct this searcher. */
  public int subSearcher(int n) {                 // find searcher for doc n:
    return ReaderUtil.subIndex(n, starts);
  }

  /** Returns the document number of document <code>n</code> within its
   * sub-index. */
  public int subDoc(int n) {
    return n - starts[subSearcher(n)];
  }

  @Override
  public int maxDoc() throws IOException {
    return maxDoc;
  }

  @Override
  public TopDocs search(Weight weight, Filter filter, int nDocs)
      throws IOException {

    nDocs = Math.min(nDocs, maxDoc());
    final HitQueue hq = new HitQueue(nDocs, false);
    int totalHits = 0;

    for (int i = 0; i < searchables.length; i++) { // search each searcher
      final TopDocs docs = new MultiSearcherCallableNoSort(DummyConcurrentLock.INSTANCE,
        searchables[i], weight, filter, nDocs, hq, i, starts).call();
      totalHits += docs.totalHits; // update totalHits
    }

    final ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
    for (int i = hq.size()-1; i >= 0; i--)	  // put docs in array
      scoreDocs[i] = hq.pop();
    
    float maxScore = (totalHits==0) ? Float.NEGATIVE_INFINITY : scoreDocs[0].score;
    
    return new TopDocs(totalHits, scoreDocs, maxScore);
  }

  @Override
  public TopFieldDocs search (Weight weight, Filter filter, int n, Sort sort) throws IOException {
    n = Math.min(n, maxDoc());
    FieldDocSortedHitQueue hq = new FieldDocSortedHitQueue(n);
    int totalHits = 0;

    float maxScore=Float.NEGATIVE_INFINITY;
    
    for (int i = 0; i < searchables.length; i++) { // search each searcher
      final TopFieldDocs docs = new MultiSearcherCallableWithSort(DummyConcurrentLock.INSTANCE,
        searchables[i], weight, filter, n, hq, sort, i, starts).call();
      totalHits += docs.totalHits; // update totalHits
      maxScore = Math.max(maxScore, docs.getMaxScore());
    }

    final ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
    for (int i = hq.size() - 1; i >= 0; i--)	  // put docs in array
      scoreDocs[i] =  hq.pop();

    return new TopFieldDocs (totalHits, scoreDocs, hq.getFields(), maxScore);
  }

  // inherit javadoc
  @Override
  public void search(Weight weight, Filter filter, final Collector collector)
  throws IOException {
    for (int i = 0; i < searchables.length; i++) {
      
      final int start = starts[i];
      
      final Collector hc = new Collector() {
        @Override
        public void setScorer(Scorer scorer) throws IOException {
          collector.setScorer(scorer);
        }
        @Override
        public void collect(int doc) throws IOException {
          collector.collect(doc);
        }
        @Override
        public void setNextReader(IndexReader reader, int docBase) throws IOException {
          collector.setNextReader(reader, start + docBase);
        }
        @Override
        public boolean acceptsDocsOutOfOrder() {
          return collector.acceptsDocsOutOfOrder();
        }
      };
      
      searchables[i].search(weight, filter, hc);
    }
  }

  @Override
  public Query rewrite(Query original) throws IOException {
    final Query[] queries = new Query[searchables.length];
    for (int i = 0; i < searchables.length; i++) {
      queries[i] = searchables[i].rewrite(original);
    }
    return queries[0].combine(queries);
  }

  @Override
  public Explanation explain(Weight weight, int doc) throws IOException {
    final int i = subSearcher(doc);			  // find searcher index
    return searchables[i].explain(weight, doc - starts[i]); // dispatch to searcher
  }

  /**
   * Create weight in multiple index scenario.
   * 
   * Distributed query processing is done in the following steps:
   * 1. rewrite query
   * 2. extract necessary terms
   * 3. collect dfs for these terms from the Searchables
   * 4. create query weight using aggregate dfs.
   * 5. distribute that weight to Searchables
   * 6. merge results
   *
   * Steps 1-4 are done here, 5+6 in the search() methods
   *
   * @return rewritten queries
   */
  @Override
  public Weight createNormalizedWeight(Query original) throws IOException {
    // step 1
    final Query rewrittenQuery = rewrite(original);

    // step 2
    final Set<Term> terms = new HashSet<Term>();
    rewrittenQuery.extractTerms(terms);

    // step3
    final Map<Term,Integer> dfMap = createDocFrequencyMap(terms);

    // step4
    final int numDocs = maxDoc();
    final CachedDfSource cacheSim = new CachedDfSource(dfMap, numDocs, getSimilarity());

    return cacheSim.createNormalizedWeight(rewrittenQuery);
  }
  /**
   * Collects the document frequency for the given terms form all searchables
   * @param terms term set used to collect the document frequency form all
   *        searchables 
   * @return a map with a term as the key and the terms aggregated document
   *         frequency as a value  
   * @throws IOException if a searchable throws an {@link IOException}
   */
   Map<Term, Integer> createDocFrequencyMap(final Set<Term> terms) throws IOException  {
    final Term[] allTermsArray = terms.toArray(new Term[terms.size()]);
    final int[] aggregatedDfs = new int[allTermsArray.length];
    for (Searchable searchable : searchables) {
      final int[] dfs = searchable.docFreqs(allTermsArray); 
      for(int j=0; j<aggregatedDfs.length; j++){
        aggregatedDfs[j] += dfs[j];
      }
    }
    final HashMap<Term,Integer> dfMap = new HashMap<Term,Integer>();
    for(int i=0; i<allTermsArray.length; i++) {
      dfMap.put(allTermsArray[i], Integer.valueOf(aggregatedDfs[i]));
    }
    return dfMap;
  }
  
  /**
   * A thread subclass for searching a single searchable 
   */
  static final class MultiSearcherCallableNoSort implements Callable<TopDocs> {

    private final Lock lock;
    private final Searchable searchable;
    private final Weight weight;
    private final Filter filter;
    private final int nDocs;
    private final int i;
    private final HitQueue hq;
    private final int[] starts;

    public MultiSearcherCallableNoSort(Lock lock, Searchable searchable, Weight weight,
        Filter filter, int nDocs, HitQueue hq, int i, int[] starts) {
      this.lock = lock;
      this.searchable = searchable;
      this.weight = weight;
      this.filter = filter;
      this.nDocs = nDocs;
      this.hq = hq;
      this.i = i;
      this.starts = starts;
    }

    public TopDocs call() throws IOException {
      final TopDocs docs = searchable.search (weight, filter, nDocs);
      final ScoreDoc[] scoreDocs = docs.scoreDocs;
      for (int j = 0; j < scoreDocs.length; j++) { // merge scoreDocs into hq
        final ScoreDoc scoreDoc = scoreDocs[j];
        scoreDoc.doc += starts[i]; // convert doc 
        //it would be so nice if we had a thread-safe insert 
        lock.lock();
        try {
          if (scoreDoc == hq.insertWithOverflow(scoreDoc))
            break;
        } finally {
          lock.unlock();
        }
      }
      return docs;
    }
  }

  /**
   * A thread subclass for searching a single searchable 
   */
  static final class MultiSearcherCallableWithSort implements Callable<TopFieldDocs> {

    private final Lock lock;
    private final Searchable searchable;
    private final Weight weight;
    private final Filter filter;
    private final int nDocs;
    private final int i;
    private final FieldDocSortedHitQueue hq;
    private final int[] starts;
    private final Sort sort;

    public MultiSearcherCallableWithSort(Lock lock, Searchable searchable, Weight weight,
        Filter filter, int nDocs, FieldDocSortedHitQueue hq, Sort sort, int i, int[] starts) {
      this.lock = lock;
      this.searchable = searchable;
      this.weight = weight;
      this.filter = filter;
      this.nDocs = nDocs;
      this.hq = hq;
      this.i = i;
      this.starts = starts;
      this.sort = sort;
    }

    public TopFieldDocs call() throws IOException {
      final TopFieldDocs docs = searchable.search (weight, filter, nDocs, sort);
      // If one of the Sort fields is FIELD_DOC, need to fix its values, so that
      // it will break ties by doc Id properly. Otherwise, it will compare to
      // 'relative' doc Ids, that belong to two different searchables.
      for (int j = 0; j < docs.fields.length; j++) {
        if (docs.fields[j].getType() == SortField.DOC) {
          // iterate over the score docs and change their fields value
          for (int j2 = 0; j2 < docs.scoreDocs.length; j2++) {
            FieldDoc fd = (FieldDoc) docs.scoreDocs[j2];
            fd.fields[j] = Integer.valueOf(((Integer) fd.fields[j]).intValue() + starts[i]);
          }
          break;
        }
      }

      lock.lock();
      try {
        hq.setFields(docs.fields);
      } finally {
        lock.unlock();
      }

      final ScoreDoc[] scoreDocs = docs.scoreDocs;
      for (int j = 0; j < scoreDocs.length; j++) { // merge scoreDocs into hq
        final FieldDoc fieldDoc = (FieldDoc) scoreDocs[j];
        fieldDoc.doc += starts[i]; // convert doc 
        //it would be so nice if we had a thread-safe insert 
        lock.lock();
        try {
          if (fieldDoc == hq.insertWithOverflow(fieldDoc))
            break;
        } finally {
          lock.unlock();
        }
      }
      return docs;
    }
  }

}
