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
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
 * 
 * @deprecated Please pass an ExecutorService to {@link
 * IndexSearcher}, instead.
 */
@Deprecated
public class ParallelMultiSearcher extends MultiSearcher {
  private final ExecutorService executor;
  private final Searchable[] searchables;
  private final int[] starts;

  /** Creates a {@link Searchable} which searches <i>searchables</i> with the default 
   * executor service (a cached thread pool). */
  public ParallelMultiSearcher(Searchable... searchables) throws IOException {
    this(Executors.newCachedThreadPool(new NamedThreadFactory(ParallelMultiSearcher.class.getSimpleName())), searchables);
  }

  /**
   * Creates a {@link Searchable} which searches <i>searchables</i> with the specified ExecutorService.
   */
  public ParallelMultiSearcher(ExecutorService executor, Searchable... searchables) throws IOException {
    super(searchables);
    this.searchables = searchables;
    this.starts = getStarts();
    this.executor = executor;
  }
  /**
   * Executes each {@link Searchable}'s docFreq() in its own thread and waits for each search to complete and merge
   * the results back together.
   */
  @Override
  public int docFreq(final Term term) throws IOException {
    final ExecutionHelper<Integer> runner = new ExecutionHelper<Integer>(executor);
    for(int i = 0; i < searchables.length; i++) {
      final Searchable searchable = searchables[i];
      runner.submit(new Callable<Integer>() {
        public Integer call() throws IOException {
          return Integer.valueOf(searchable.docFreq(term));
        }
      });
    }
    int docFreq = 0;
    for (Integer num : runner) {
      docFreq += num.intValue();
    }
    return docFreq;
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
    final ExecutionHelper<TopDocs> runner = new ExecutionHelper<TopDocs>(executor);
    
    for (int i = 0; i < searchables.length; i++) { // search each searchable
      runner.submit(
          new MultiSearcherCallableNoSort(lock, searchables[i], weight, filter, nDocs, hq, i, starts));
    }

    int totalHits = 0;
    float maxScore = Float.NEGATIVE_INFINITY;
    for (final TopDocs topDocs : runner) {
      totalHits += topDocs.totalHits;
      maxScore = Math.max(maxScore, topDocs.getMaxScore());
    }

    final ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
    for (int i = hq.size() - 1; i >= 0; i--) // put docs in array
      scoreDocs[i] = hq.pop();

    return new TopDocs(totalHits, scoreDocs, maxScore);
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
    final ExecutionHelper<TopFieldDocs> runner = new ExecutionHelper<TopFieldDocs>(executor);
    for (int i = 0; i < searchables.length; i++) { // search each searchable
      runner.submit(
          new MultiSearcherCallableWithSort(lock, searchables[i], weight, filter, nDocs, hq, sort, i, starts));
    }
    int totalHits = 0;
    float maxScore = Float.NEGATIVE_INFINITY;
    for (final TopFieldDocs topFieldDocs : runner) {
      totalHits += topFieldDocs.totalHits;
      maxScore = Math.max(maxScore, topFieldDocs.getMaxScore());
    }
    final ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
    for (int i = hq.size() - 1; i >= 0; i--) // put docs in array
      scoreDocs[i] = hq.pop();

    return new TopFieldDocs(totalHits, scoreDocs, hq.getFields(), maxScore);
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
  public void close() throws IOException {
    executor.shutdown();
    super.close();
  }

  @Override
  HashMap<Term, Integer> createDocFrequencyMap(Set<Term> terms) throws IOException {
    final Term[] allTermsArray = terms.toArray(new Term[terms.size()]);
    final int[] aggregatedDocFreqs = new int[terms.size()];
    final ExecutionHelper<int[]> runner = new ExecutionHelper<int[]>(executor);
    for (Searchable searchable : searchables) {
      runner.submit(
          new DocumentFrequencyCallable(searchable, allTermsArray));
    }
    final int docFreqLen = aggregatedDocFreqs.length;
    for (final int[] docFreqs : runner) {
      for(int i=0; i < docFreqLen; i++){
        aggregatedDocFreqs[i] += docFreqs[i];
      }
    }

    final HashMap<Term,Integer> dfMap = new HashMap<Term,Integer>();
    for(int i=0; i<allTermsArray.length; i++) {
      dfMap.put(allTermsArray[i], Integer.valueOf(aggregatedDocFreqs[i]));
    }
    return dfMap;
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
  
  /**
   * A helper class that wraps a {@link CompletionService} and provides an
   * iterable interface to the completed {@link Callable} instances.
   * 
   * @param <T>
   *          the type of the {@link Callable} return value
   */
  private static final class ExecutionHelper<T> implements Iterator<T>, Iterable<T> {
    private final CompletionService<T> service;
    private int numTasks;

    ExecutionHelper(final Executor executor) {
      this.service = new ExecutorCompletionService<T>(executor);
    }

    public boolean hasNext() {
      return numTasks > 0;
    }

    public void submit(Callable<T> task) {
      this.service.submit(task);
      ++numTasks;
    }

    public T next() {
     if(!this.hasNext())
       throw new NoSuchElementException();
      try {
        return service.take().get();
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      } finally {
        --numTasks;
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    public Iterator<T> iterator() {
      // use the shortcut here - this is only used in a privat context
      return this;
    }

  }
}
