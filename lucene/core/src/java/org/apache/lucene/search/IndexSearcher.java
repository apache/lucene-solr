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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader; // javadocs
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.NIOFSDirectory;    // javadoc
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.TermContext;
import org.apache.lucene.util.ThreadInterruptedException;

/** Implements search over a single IndexReader.
 *
 * <p>Applications usually need only call the inherited
 * {@link #search(Query,int)}
 * or {@link #search(Query,Filter,int)} methods. For
 * performance reasons, if your index is unchanging, you
 * should share a single IndexSearcher instance across
 * multiple searches instead of creating a new one
 * per-search.  If your index has changed and you wish to
 * see the changes reflected in searching, you should
 * use {@link DirectoryReader#openIfChanged(DirectoryReader)}
 * to obtain a new reader and
 * then create a new IndexSearcher from that.  Also, for
 * low-latency turnaround it's best to use a near-real-time
 * reader ({@link DirectoryReader#open(IndexWriter,boolean)}).
 * Once you have a new {@link IndexReader}, it's relatively
 * cheap to create a new IndexSearcher from it.
 * 
 * <a name="thread-safety"></a><p><b>NOTE</b>: <code>{@link
 * IndexSearcher}</code> instances are completely
 * thread safe, meaning multiple threads can call any of its
 * methods, concurrently.  If your application requires
 * external synchronization, you should <b>not</b>
 * synchronize on the <code>IndexSearcher</code> instance;
 * use your own (non-Lucene) objects instead.</p>
 */
public class IndexSearcher {
  final IndexReader reader; // package private for testing!
  
  // NOTE: these members might change in incompatible ways
  // in the next release
  protected final IndexReaderContext readerContext;
  protected final AtomicReaderContext[] leafContexts;
  // used with executor - each slice holds a set of leafs executed within one thread
  protected final LeafSlice[] leafSlices;

  // These are only used for multi-threaded search
  private final ExecutorService executor;

  // the default Similarity
  private static final Similarity defaultSimilarity = new DefaultSimilarity();
  
  /**
   * Expert: returns a default Similarity instance.
   * In general, this method is only called to initialize searchers and writers.
   * User code and query implementations should respect
   * {@link IndexSearcher#getSimilarity()}.
   * @lucene.internal
   */
  public static Similarity getDefaultSimilarity() {
    return defaultSimilarity;
  }
  
  /** The Similarity implementation used by this searcher. */
  private Similarity similarity = defaultSimilarity;

  /** Creates a searcher searching the provided index. */
  public IndexSearcher(IndexReader r) {
    this(r, null);
  }

  /** Runs searches for each segment separately, using the
   *  provided ExecutorService.  IndexSearcher will not
   *  shutdown/awaitTermination this ExecutorService on
   *  close; you must do so, eventually, on your own.  NOTE:
   *  if you are using {@link NIOFSDirectory}, do not use
   *  the shutdownNow method of ExecutorService as this uses
   *  Thread.interrupt under-the-hood which can silently
   *  close file descriptors (see <a
   *  href="https://issues.apache.org/jira/browse/LUCENE-2239">LUCENE-2239</a>).
   * 
   * @lucene.experimental */
  public IndexSearcher(IndexReader r, ExecutorService executor) {
    this(r.getTopReaderContext(), executor);
  }

  /**
   * Creates a searcher searching the provided top-level {@link IndexReaderContext}.
   * <p>
   * Given a non-<code>null</code> {@link ExecutorService} this method runs
   * searches for each segment separately, using the provided ExecutorService.
   * IndexSearcher will not shutdown/awaitTermination this ExecutorService on
   * close; you must do so, eventually, on your own. NOTE: if you are using
   * {@link NIOFSDirectory}, do not use the shutdownNow method of
   * ExecutorService as this uses Thread.interrupt under-the-hood which can
   * silently close file descriptors (see <a
   * href="https://issues.apache.org/jira/browse/LUCENE-2239">LUCENE-2239</a>).
   * 
   * @see IndexReaderContext
   * @see IndexReader#getTopReaderContext()
   * @lucene.experimental
   */
  public IndexSearcher(IndexReaderContext context, ExecutorService executor) {
    assert context.isTopLevel: "IndexSearcher's ReaderContext must be topLevel for reader" + context.reader();
    reader = context.reader();
    this.executor = executor;
    this.readerContext = context;
    leafContexts = context.leaves();
    this.leafSlices = executor == null ? null : slices(leafContexts);
  }

  /**
   * Creates a searcher searching the provided top-level {@link IndexReaderContext}.
   *
   * @see IndexReaderContext
   * @see IndexReader#getTopReaderContext()
   * @lucene.experimental
   */
  public IndexSearcher(IndexReaderContext context) {
    this(context, null);
  }
  
  /**
   * Expert: Creates an array of leaf slices each holding a subset of the given leaves.
   * Each {@link LeafSlice} is executed in a single thread. By default there
   * will be one {@link LeafSlice} per leaf ({@link AtomicReaderContext}).
   */
  protected LeafSlice[] slices(AtomicReaderContext...leaves) {
    LeafSlice[] slices = new LeafSlice[leaves.length];
    for (int i = 0; i < slices.length; i++) {
      slices[i] = new LeafSlice(leaves[i]);
    }
    return slices;
  }

  
  /** Return the {@link IndexReader} this searches. */
  public IndexReader getIndexReader() {
    return reader;
  }

  /** Sugar for <code>.getIndexReader().document(docID)</code> */
  public Document doc(int docID) throws CorruptIndexException, IOException {
    return reader.document(docID);
  }

  /** Sugar for <code>.getIndexReader().document(docID, fieldVisitor)</code> */
  public void doc(int docID, StoredFieldVisitor fieldVisitor) throws CorruptIndexException, IOException {
    reader.document(docID, fieldVisitor);
  }

  /** Sugar for <code>.getIndexReader().document(docID, fieldsToLoad)</code> */
  public final Document document(int docID, Set<String> fieldsToLoad) throws CorruptIndexException, IOException {
    return reader.document(docID, fieldsToLoad);
  }

  /** Expert: Set the Similarity implementation used by this Searcher.
   *
   */
  public void setSimilarity(Similarity similarity) {
    this.similarity = similarity;
  }

  public Similarity getSimilarity() {
    return similarity;
  }
  
  /** @lucene.internal */
  protected Query wrapFilter(Query query, Filter filter) {
    return (filter == null) ? query : new FilteredQuery(query, filter);
  }

  /** Finds the top <code>n</code>
   * hits for <code>query</code> where all results are after a previous 
   * result (<code>after</code>).
   * <p>
   * By passing the bottom result from a previous page as <code>after</code>,
   * this method can be used for efficient 'deep-paging' across potentially
   * large result sets.
   *
   * @throws BooleanQuery.TooManyClauses
   */
  public TopDocs searchAfter(ScoreDoc after, Query query, int n) throws IOException {
    return search(createNormalizedWeight(query), after, n);
  }
  
  /** Finds the top <code>n</code>
   * hits for <code>query</code>, applying <code>filter</code> if non-null,
   * where all results are after a previous result (<code>after</code>).
   * <p>
   * By passing the bottom result from a previous page as <code>after</code>,
   * this method can be used for efficient 'deep-paging' across potentially
   * large result sets.
   *
   * @throws BooleanQuery.TooManyClauses
   */
  public TopDocs searchAfter(ScoreDoc after, Query query, Filter filter, int n) throws IOException {
    return search(createNormalizedWeight(wrapFilter(query, filter)), after, n);
  }
  
  /** Finds the top <code>n</code>
   * hits for <code>query</code>.
   *
   * @throws BooleanQuery.TooManyClauses
   */
  public TopDocs search(Query query, int n)
    throws IOException {
    return search(query, null, n);
  }


  /** Finds the top <code>n</code>
   * hits for <code>query</code>, applying <code>filter</code> if non-null.
   *
   * @throws BooleanQuery.TooManyClauses
   */
  public TopDocs search(Query query, Filter filter, int n)
    throws IOException {
    return search(createNormalizedWeight(wrapFilter(query, filter)), null, n);
  }

  /** Lower-level search API.
   *
   * <p>{@link Collector#collect(int)} is called for every matching
   * document.
   * <br>Collector-based access to remote indexes is discouraged.
   *
   * <p>Applications should only use this if they need <i>all</i> of the
   * matching documents.  The high-level search API ({@link
   * IndexSearcher#search(Query, Filter, int)}) is usually more efficient, as it skips
   * non-high-scoring hits.
   *
   * @param query to match documents
   * @param filter if non-null, used to permit documents to be collected.
   * @param results to receive hits
   * @throws BooleanQuery.TooManyClauses
   */
  public void search(Query query, Filter filter, Collector results)
    throws IOException {
    search(leafContexts, createNormalizedWeight(wrapFilter(query, filter)), results);
  }

  /** Lower-level search API.
  *
  * <p>{@link Collector#collect(int)} is called for every matching document.
  *
  * <p>Applications should only use this if they need <i>all</i> of the
  * matching documents.  The high-level search API ({@link
  * IndexSearcher#search(Query, int)}) is usually more efficient, as it skips
  * non-high-scoring hits.
  * <p>Note: The <code>score</code> passed to this method is a raw score.
  * In other words, the score will not necessarily be a float whose value is
  * between 0 and 1.
  * @throws BooleanQuery.TooManyClauses
  */
  public void search(Query query, Collector results)
    throws IOException {
    search(leafContexts, createNormalizedWeight(query), results);
  }
  
  /** Search implementation with arbitrary sorting.  Finds
   * the top <code>n</code> hits for <code>query</code>, applying
   * <code>filter</code> if non-null, and sorting the hits by the criteria in
   * <code>sort</code>.
   * 
   * <p>NOTE: this does not compute scores by default; use
   * {@link IndexSearcher#setDefaultFieldSortScoring} to
   * enable scoring.
   *
   * @throws BooleanQuery.TooManyClauses
   */
  public TopFieldDocs search(Query query, Filter filter, int n,
                             Sort sort) throws IOException {
    return search(createNormalizedWeight(wrapFilter(query, filter)), n, sort);
  }

  /**
   * Search implementation with arbitrary sorting and no filter.
   * @param query The query to search for
   * @param n Return only the top n results
   * @param sort The {@link org.apache.lucene.search.Sort} object
   * @return The top docs, sorted according to the supplied {@link org.apache.lucene.search.Sort} instance
   * @throws IOException
   */
  public TopFieldDocs search(Query query, int n,
                             Sort sort) throws IOException {
    return search(createNormalizedWeight(query), n, sort);
  }

  /** Expert: Low-level search implementation.  Finds the top <code>n</code>
   * hits for <code>query</code>, applying <code>filter</code> if non-null.
   *
   * <p>Applications should usually call {@link IndexSearcher#search(Query,int)} or
   * {@link IndexSearcher#search(Query,Filter,int)} instead.
   * @throws BooleanQuery.TooManyClauses
   */
  protected TopDocs search(Weight weight, ScoreDoc after, int nDocs) throws IOException {
    if (executor == null) {
      return search(leafContexts, weight, after, nDocs);
    } else {
      final HitQueue hq = new HitQueue(nDocs, false);
      final Lock lock = new ReentrantLock();
      final ExecutionHelper<TopDocs> runner = new ExecutionHelper<TopDocs>(executor);
    
      for (int i = 0; i < leafSlices.length; i++) { // search each sub
        runner.submit(
                      new SearcherCallableNoSort(lock, this, leafSlices[i], weight, after, nDocs, hq));
      }

      int totalHits = 0;
      float maxScore = Float.NEGATIVE_INFINITY;
      for (final TopDocs topDocs : runner) {
        if(topDocs.totalHits != 0) {
          totalHits += topDocs.totalHits;
          maxScore = Math.max(maxScore, topDocs.getMaxScore());
        }
      }

      final ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
      for (int i = hq.size() - 1; i >= 0; i--) // put docs in array
        scoreDocs[i] = hq.pop();

      return new TopDocs(totalHits, scoreDocs, maxScore);
    }
  }

  /** Expert: Low-level search implementation.  Finds the top <code>n</code>
   * hits for <code>query</code>.
   *
   * <p>Applications should usually call {@link IndexSearcher#search(Query,int)} or
   * {@link IndexSearcher#search(Query,Filter,int)} instead.
   * @throws BooleanQuery.TooManyClauses
   */
  protected TopDocs search(AtomicReaderContext[] leaves, Weight weight, ScoreDoc after, int nDocs) throws IOException {
    // single thread
    int limit = reader.maxDoc();
    if (limit == 0) {
      limit = 1;
    }
    nDocs = Math.min(nDocs, limit);
    TopScoreDocCollector collector = TopScoreDocCollector.create(nDocs, after, !weight.scoresDocsOutOfOrder());
    search(leaves, weight, collector);
    return collector.topDocs();
  }

  /** Expert: Low-level search implementation with arbitrary sorting.  Finds
   * the top <code>n</code> hits for <code>query</code> and sorting the hits
   * by the criteria in <code>sort</code>.
   *
   * <p>Applications should usually call {@link
   * IndexSearcher#search(Query,Filter,int,Sort)} instead.
   * 
   * @throws BooleanQuery.TooManyClauses
   */
  protected TopFieldDocs search(Weight weight,
      final int nDocs, Sort sort) throws IOException {
    return search(weight, nDocs, sort, true);
  }

  /**
   * Just like {@link #search(Weight, int, Sort)}, but you choose
   * whether or not the fields in the returned {@link FieldDoc} instances should
   * be set by specifying fillFields.
   *
   * <p>NOTE: this does not compute scores by default.  If you
   * need scores, create a {@link TopFieldCollector}
   * instance by calling {@link TopFieldCollector#create} and
   * then pass that to {@link #search(AtomicReaderContext[], Weight,
   * Collector)}.</p>
   */
  protected TopFieldDocs search(Weight weight, int nDocs,
                                Sort sort, boolean fillFields)
      throws IOException {

    if (sort == null) throw new NullPointerException();
    
    if (executor == null) {
      // use all leaves here!
      return search (leafContexts, weight, nDocs, sort, fillFields);
    } else {
      final TopFieldCollector topCollector = TopFieldCollector.create(sort, nDocs,
                                                                      fillFields,
                                                                      fieldSortDoTrackScores,
                                                                      fieldSortDoMaxScore,
                                                                      false);

      final Lock lock = new ReentrantLock();
      final ExecutionHelper<TopFieldDocs> runner = new ExecutionHelper<TopFieldDocs>(executor);
      for (int i = 0; i < leafSlices.length; i++) { // search each leaf slice
        runner.submit(
                      new SearcherCallableWithSort(lock, this, leafSlices[i], weight, nDocs, topCollector, sort));
      }
      int totalHits = 0;
      float maxScore = Float.NEGATIVE_INFINITY;
      for (final TopFieldDocs topFieldDocs : runner) {
        if (topFieldDocs.totalHits != 0) {
          totalHits += topFieldDocs.totalHits;
          maxScore = Math.max(maxScore, topFieldDocs.getMaxScore());
        }
      }

      final TopFieldDocs topDocs = (TopFieldDocs) topCollector.topDocs();

      return new TopFieldDocs(totalHits, topDocs.scoreDocs, topDocs.fields, topDocs.getMaxScore());
    }
  }
  
  
  /**
   * Just like {@link #search(Weight, int, Sort)}, but you choose
   * whether or not the fields in the returned {@link FieldDoc} instances should
   * be set by specifying fillFields.
   *
   * <p>NOTE: this does not compute scores by default.  If you
   * need scores, create a {@link TopFieldCollector}
   * instance by calling {@link TopFieldCollector#create} and
   * then pass that to {@link #search(AtomicReaderContext[], Weight, 
   * Collector)}.</p>
   */
  protected TopFieldDocs search(AtomicReaderContext[] leaves, Weight weight, int nDocs,
      Sort sort, boolean fillFields) throws IOException {
    // single thread
    int limit = reader.maxDoc();
    if (limit == 0) {
      limit = 1;
    }
    nDocs = Math.min(nDocs, limit);

    TopFieldCollector collector = TopFieldCollector.create(sort, nDocs,
                                                           fillFields, fieldSortDoTrackScores, fieldSortDoMaxScore, !weight.scoresDocsOutOfOrder());
    search(leaves, weight, collector);
    return (TopFieldDocs) collector.topDocs();
  }

  /**
   * Lower-level search API.
   * 
   * <p>
   * {@link Collector#collect(int)} is called for every document. <br>
   * Collector-based access to remote indexes is discouraged.
   * 
   * <p>
   * Applications should only use this if they need <i>all</i> of the matching
   * documents. The high-level search API ({@link IndexSearcher#search(Query,int)}) is
   * usually more efficient, as it skips non-high-scoring hits.
   * 
   * <p>
   * NOTE: this method executes the searches on all given leaves exclusively.
   * To search across all the searchers leaves use {@link #leafContexts}.
   * 
   * @param leaves 
   *          the searchers leaves to execute the searches on
   * @param weight
   *          to match documents
   * @param collector
   *          to receive hits
   * @throws BooleanQuery.TooManyClauses
   */
  protected void search(AtomicReaderContext[] leaves, Weight weight, Collector collector)
      throws IOException {

    // TODO: should we make this
    // threaded...?  the Collector could be sync'd?
    // always use single thread:
    for (int i = 0; i < leaves.length; i++) { // search each subreader
      collector.setNextReader(leaves[i]);
      Scorer scorer = weight.scorer(leaves[i], !collector.acceptsDocsOutOfOrder(), true, leaves[i].reader().getLiveDocs());
      if (scorer != null) {
        scorer.score(collector);
      }
    }
  }

  /** Expert: called to re-write queries into primitive queries.
   * @throws BooleanQuery.TooManyClauses
   */
  public Query rewrite(Query original) throws IOException {
    Query query = original;
    for (Query rewrittenQuery = query.rewrite(reader); rewrittenQuery != query;
         rewrittenQuery = query.rewrite(reader)) {
      query = rewrittenQuery;
    }
    return query;
  }

  /** Returns an Explanation that describes how <code>doc</code> scored against
   * <code>query</code>.
   *
   * <p>This is intended to be used in developing Similarity implementations,
   * and, for good performance, should not be displayed with every hit.
   * Computing an explanation is as expensive as executing the query over the
   * entire index.
   */
  public Explanation explain(Query query, int doc) throws IOException {
    return explain(createNormalizedWeight(query), doc);
  }

  /** Expert: low-level implementation method
   * Returns an Explanation that describes how <code>doc</code> scored against
   * <code>weight</code>.
   *
   * <p>This is intended to be used in developing Similarity implementations,
   * and, for good performance, should not be displayed with every hit.
   * Computing an explanation is as expensive as executing the query over the
   * entire index.
   * <p>Applications should call {@link IndexSearcher#explain(Query, int)}.
   * @throws BooleanQuery.TooManyClauses
   */
  protected Explanation explain(Weight weight, int doc) throws IOException {
    int n = ReaderUtil.subIndex(doc, leafContexts);
    int deBasedDoc = doc - leafContexts[n].docBase;
    
    return weight.explain(leafContexts[n], deBasedDoc);
  }

  private boolean fieldSortDoTrackScores;
  private boolean fieldSortDoMaxScore;

  /** By default, no scores are computed when sorting by
   *  field (using {@link #search(Query,Filter,int,Sort)}).
   *  You can change that, per IndexSearcher instance, by
   *  calling this method.  Note that this will incur a CPU
   *  cost.
   * 
   *  @param doTrackScores If true, then scores are
   *  returned for every matching document in {@link
   *  TopFieldDocs}.
   *
   *  @param doMaxScore If true, then the max score for all
   *  matching docs is computed. */
  public void setDefaultFieldSortScoring(boolean doTrackScores, boolean doMaxScore) {
    fieldSortDoTrackScores = doTrackScores;
    fieldSortDoMaxScore = doMaxScore;
  }

  /**
   * Creates a normalized weight for a top-level {@link Query}.
   * The query is rewritten by this method and {@link Query#createWeight} called,
   * afterwards the {@link Weight} is normalized. The returned {@code Weight}
   * can then directly be used to get a {@link Scorer}.
   * @lucene.internal
   */
  public Weight createNormalizedWeight(Query query) throws IOException {
    query = rewrite(query);
    Weight weight = query.createWeight(this);
    float v = weight.getValueForNormalization();
    float norm = getSimilarity().queryNorm(v);
    if (Float.isInfinite(norm) || Float.isNaN(norm)) {
      norm = 1.0f;
    }
    weight.normalize(norm, 1.0f);
    return weight;
  }
  
  /**
   * Returns this searchers the top-level {@link IndexReaderContext}.
   * @see IndexReader#getTopReaderContext()
   */
  /* sugar for #getReader().getTopReaderContext() */
  public IndexReaderContext getTopReaderContext() {
    return readerContext;
  }

  /**
   * A thread subclass for searching a single searchable 
   */
  private static final class SearcherCallableNoSort implements Callable<TopDocs> {

    private final Lock lock;
    private final IndexSearcher searcher;
    private final Weight weight;
    private final ScoreDoc after;
    private final int nDocs;
    private final HitQueue hq;
    private final LeafSlice slice;

    public SearcherCallableNoSort(Lock lock, IndexSearcher searcher, LeafSlice slice,  Weight weight,
        ScoreDoc after, int nDocs, HitQueue hq) {
      this.lock = lock;
      this.searcher = searcher;
      this.weight = weight;
      this.after = after;
      this.nDocs = nDocs;
      this.hq = hq;
      this.slice = slice;
    }

    public TopDocs call() throws IOException {
      final TopDocs docs = searcher.search (slice.leaves, weight, after, nDocs);
      final ScoreDoc[] scoreDocs = docs.scoreDocs;
      //it would be so nice if we had a thread-safe insert 
      lock.lock();
      try {
        for (int j = 0; j < scoreDocs.length; j++) { // merge scoreDocs into hq
          final ScoreDoc scoreDoc = scoreDocs[j];
          if (scoreDoc == hq.insertWithOverflow(scoreDoc)) {
            break;
          }
        }
      } finally {
        lock.unlock();
      }
      return docs;
    }
  }


  /**
   * A thread subclass for searching a single searchable 
   */
  private static final class SearcherCallableWithSort implements Callable<TopFieldDocs> {

    private final Lock lock;
    private final IndexSearcher searcher;
    private final Weight weight;
    private final int nDocs;
    private final TopFieldCollector hq;
    private final Sort sort;
    private final LeafSlice slice;

    public SearcherCallableWithSort(Lock lock, IndexSearcher searcher, LeafSlice slice, Weight weight,
        int nDocs, TopFieldCollector hq, Sort sort) {
      this.lock = lock;
      this.searcher = searcher;
      this.weight = weight;
      this.nDocs = nDocs;
      this.hq = hq;
      this.sort = sort;
      this.slice = slice;
    }

    private final class FakeScorer extends Scorer {
      float score;
      int doc;

      public FakeScorer() {
        super(null);
      }
    
      @Override
      public int advance(int target) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public float freq() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int nextDoc() {
        throw new UnsupportedOperationException();
      }
    
      @Override
      public float score() {
        return score;
      }
    }

    private final FakeScorer fakeScorer = new FakeScorer();

    public TopFieldDocs call() throws IOException {
      assert slice.leaves.length == 1;
      final TopFieldDocs docs = searcher.search (slice.leaves, weight, nDocs, sort, true);
      lock.lock();
      try {
        final int base = slice.leaves[0].docBase;
        hq.setNextReader(slice.leaves[0]);
        hq.setScorer(fakeScorer);
        for(ScoreDoc scoreDoc : docs.scoreDocs) {
          fakeScorer.doc = scoreDoc.doc - base;
          fakeScorer.score = scoreDoc.score;
          hq.collect(scoreDoc.doc-base);
        }
      } finally {
        lock.unlock();
      }
      return docs;
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
      // use the shortcut here - this is only used in a private context
      return this;
    }
  }

  /**
   * A class holding a subset of the {@link IndexSearcher}s leaf contexts to be
   * executed within a single thread.
   * 
   * @lucene.experimental
   */
  public static class LeafSlice {
    final AtomicReaderContext[] leaves;
    
    public LeafSlice(AtomicReaderContext...leaves) {
      this.leaves = leaves;
    }
  }

  @Override
  public String toString() {
    return "IndexSearcher(" + reader + "; executor=" + executor + ")";
  }
  
  /**
   * Returns {@link TermStatistics} for a term.
   * 
   * This can be overridden for example, to return a term's statistics
   * across a distributed collection.
   * @lucene.experimental
   */
  public TermStatistics termStatistics(Term term, TermContext context) throws IOException {
    return new TermStatistics(term.bytes(), context.docFreq(), context.totalTermFreq());
  };
  
  /**
   * Returns {@link CollectionStatistics} for a field.
   * 
   * This can be overridden for example, to return a field's statistics
   * across a distributed collection.
   * @lucene.experimental
   */
  public CollectionStatistics collectionStatistics(String field) throws IOException {
    final int docCount;
    final long sumTotalTermFreq;
    final long sumDocFreq;

    assert field != null;
    
    Terms terms = MultiFields.getTerms(reader, field);
    if (terms == null) {
      docCount = 0;
      sumTotalTermFreq = 0;
      sumDocFreq = 0;
    } else {
      docCount = terms.getDocCount();
      sumTotalTermFreq = terms.getSumTotalTermFreq();
      sumDocFreq = terms.getSumDocFreq();
    }
    return new CollectionStatistics(field, reader.maxDoc(), docCount, sumTotalTermFreq, sumDocFreq);
  }
}
