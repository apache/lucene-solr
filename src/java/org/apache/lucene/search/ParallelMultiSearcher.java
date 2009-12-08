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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.ThreadInterruptedException;

/** Implements parallel search over a set of <code>Searchables</code>.
 *
 * <p>Applications usually need only call the inherited {@link #search(Query,int)}
 * or {@link #search(Query,Filter,int)} methods.
 */
public class ParallelMultiSearcher extends MultiSearcher {
  
  private final ExecutorService executor;
  private final Searchable[] searchables;
  private final int[] starts;

  /** Creates a {@link Searchable} which searches <i>searchables</i>. */
  public ParallelMultiSearcher(Searchable... searchables) throws IOException {
    super(searchables);
    this.searchables = searchables;
    this.starts = getStarts();
    executor = Executors.newCachedThreadPool(new NamedThreadFactory(this.getClass().getSimpleName())); 
  }

  /**
   * Executes each {@link Searchable}'s docFreq() in its own thread and waits for each search to complete and merge
   * the results back together.
   */
  @Override
  public int docFreq(final Term term) throws IOException {
    @SuppressWarnings("unchecked") final Future<Integer>[] searchThreads = new Future[searchables.length];
    for (int i = 0; i < searchables.length; i++) { // search each searchable
      final Searchable searchable = searchables[i];
      searchThreads[i] = executor.submit(new Callable<Integer>() {
        public Integer call() throws IOException {
          return Integer.valueOf(searchable.docFreq(term));
        }
      });
    }
    final CountDocFreq func = new CountDocFreq();
    foreach(func, Arrays.asList(searchThreads));
    return func.docFreq;
  }

  /**
   * A search implementation which executes each 
   * {@link Searchable} in its own thread and waits for each search to complete and merge
   * the results back together.
   */
  @Override
  public TopDocs search(Weight weight, Filter filter, int nDocs) throws IOException {
    final HitQueue hq = new HitQueue(nDocs, false);
    final Lock lock = new ReentrantLock();
    @SuppressWarnings("unchecked") final Future<TopDocs>[] searchThreads = new Future[searchables.length];
    for (int i = 0; i < searchables.length; i++) { // search each searchable
      searchThreads[i] = executor.submit(
          new MultiSearcherCallableNoSort(lock, searchables[i], weight, filter, nDocs, hq, i, starts));
    }

    final CountTotalHits<TopDocs> func = new CountTotalHits<TopDocs>();
    foreach(func, Arrays.asList(searchThreads));

    final ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
    for (int i = hq.size() - 1; i >= 0; i--) // put docs in array
      scoreDocs[i] = hq.pop();

    return new TopDocs(func.totalHits, scoreDocs, func.maxScore);
  }

  /**
   * A search implementation allowing sorting which spans a new thread for each
   * Searchable, waits for each search to complete and merges
   * the results back together.
   */
  @Override
  public TopFieldDocs search(Weight weight, Filter filter, int nDocs, Sort sort) throws IOException {
    if (sort == null) throw new NullPointerException();

    final FieldDocSortedHitQueue hq = new FieldDocSortedHitQueue(nDocs);
    final Lock lock = new ReentrantLock();
    @SuppressWarnings("unchecked") final Future<TopFieldDocs>[] searchThreads = new Future[searchables.length];
    for (int i = 0; i < searchables.length; i++) { // search each searchable
      searchThreads[i] = executor.submit(
          new MultiSearcherCallableWithSort(lock, searchables[i], weight, filter, nDocs, hq, sort, i, starts));
    }

    final CountTotalHits<TopFieldDocs> func = new CountTotalHits<TopFieldDocs>();
    foreach(func, Arrays.asList(searchThreads));

    final ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
    for (int i = hq.size() - 1; i >= 0; i--) // put docs in array
      scoreDocs[i] = hq.pop();

    return new TopFieldDocs(func.totalHits, scoreDocs, hq.getFields(), func.maxScore);
  }

  /** Lower-level search API.
  *
  * <p>{@link Collector#collect(int)} is called for every matching document.
  *
  * <p>Applications should only use this if they need <i>all</i> of the
  * matching documents.  The high-level search API ({@link
  * Searcher#search(Query,int)}) is usually more efficient, as it skips
  * non-high-scoring hits.
  * 
  * <p>This method cannot be parallelized, because {@link Collector}
  * supports no concurrent access.
  *
  * @param weight to match documents
  * @param filter if non-null, a bitset used to eliminate some documents
  * @param collector to receive hits
  */
  @Override
  public void search(final Weight weight, final Filter filter, final Collector collector)
   throws IOException {
   for (int i = 0; i < searchables.length; i++) {

     final int start = starts[i];

     final Collector hc = new Collector() {
       @Override
       public void setScorer(final Scorer scorer) throws IOException {
         collector.setScorer(scorer);
       }
       
       @Override
       public void collect(final int doc) throws IOException {
         collector.collect(doc);
       }
       
       @Override
       public void setNextReader(final IndexReader reader, final int docBase) throws IOException {
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
  HashMap<Term, Integer> createDocFrequencyMap(Set<Term> terms) throws IOException {
    final Term[] allTermsArray = terms.toArray(new Term[terms.size()]);
    final int[] aggregatedDocFreqs = new int[terms.size()];
    final ArrayList<Future<int[]>> searchThreads = new ArrayList<Future<int[]>>(searchables.length);
    for (Searchable searchable : searchables) {
      final Future<int[]> future = executor.submit(
          new DocumentFrequencyCallable(searchable, allTermsArray));
      searchThreads.add(future);
    }
    foreach(new AggregateDocFrequency(aggregatedDocFreqs), searchThreads);

    final HashMap<Term,Integer> dfMap = new HashMap<Term,Integer>();
    for(int i=0; i<allTermsArray.length; i++) {
      dfMap.put(allTermsArray[i], Integer.valueOf(aggregatedDocFreqs[i]));
    }
    return dfMap;
  }
  
  /*
   * apply the function to each element of the list. This method encapsulates the logic how 
   * to wait for concurrently executed searchables.  
   */
  private <T> void foreach(Function<T> func, List<Future<T>> list) throws IOException{
    for (Future<T> future : list) {
      try{
        func.apply(future.get());
      } catch (ExecutionException e) {
        final Throwable throwable = e.getCause();
        if (throwable instanceof IOException)
          throw (IOException) e.getCause();
        throw new RuntimeException(throwable);
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }
    }
  }

  // Both functions could be reduced to Int as other values of TopDocs
  // are not needed. Using sep. functions is more self documenting.
  /**
   * A function with one argument
   * @param <T> the argument type
   */
  private static interface Function<T> {
    abstract void apply(T t);
  }

  /**
   * Counts the total number of hits for all {@link TopDocs} instances
   * provided. 
   */
  private static final class CountTotalHits<T extends TopDocs> implements Function<T> {
    int totalHits = 0;
    float maxScore = Float.NEGATIVE_INFINITY;
    
    public void apply(T t) {
      totalHits += t.totalHits;
      maxScore = Math.max(maxScore, t.getMaxScore());
    }
  }
  
  /**
   * Accumulates the document frequency for a term.
   */
  private static final class CountDocFreq implements Function<Integer>{
    int docFreq = 0;
    
    public void apply(Integer t) {
      docFreq += t.intValue();
    }
  }
  
  /**
   * Aggregates the document frequencies from multiple searchers 
   */
  private static final class AggregateDocFrequency implements Function<int[]>{
    final int[] aggregatedDocFreqs;
    
    public AggregateDocFrequency(int[] aggregatedDocFreqs){
      this.aggregatedDocFreqs = aggregatedDocFreqs;
    }
    
    public void apply(final int[] docFreqs) {
      for(int i=0; i<aggregatedDocFreqs.length; i++){
        aggregatedDocFreqs[i] += docFreqs[i];
      }
    }
  }
  
  /**
   * A {@link Callable} to retrieve the document frequencies for a Term array  
   */
  private static final class DocumentFrequencyCallable implements Callable<int[]> {
    private final Searchable searchable;
    private final Term[] terms;
    
    public DocumentFrequencyCallable(Searchable searchable, Term[] terms) {
      this.searchable = searchable;
      this.terms = terms;
    }
    
    public int[] call() throws Exception {
      return searchable.docFreqs(terms);
    }
  }
}
