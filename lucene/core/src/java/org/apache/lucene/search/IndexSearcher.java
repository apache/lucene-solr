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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory; // javadocs
import org.apache.lucene.util.ReaderUtil;
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
 * use {@link IndexReader#openIfChanged} to obtain a new reader and
 * then create a new IndexSearcher from that.  Also, for
 * low-latency turnaround it's best to use a near-real-time
 * reader ({@link IndexReader#open(IndexWriter,boolean)}).
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
public class IndexSearcher extends Searcher {
  IndexReader reader;
  private boolean closeReader;
  
  // NOTE: these members might change in incompatible ways
  // in the next release
  protected final IndexReader[] subReaders;
  protected final int[] docStarts;
  
  // These are only used for multi-threaded search
  private final ExecutorService executor;
  protected final IndexSearcher[] subSearchers;

  private final int docBase;

  /** Creates a searcher searching the index in the named
   *  directory, with readOnly=true
   * @param path directory where IndexReader will be opened
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @deprecated use {@link IndexSearcher#IndexSearcher(IndexReader)} instead.
   */
  @Deprecated
  public IndexSearcher(Directory path) throws CorruptIndexException, IOException {
    this(IndexReader.open(path, true), true, null);
  }

  /** Creates a searcher searching the index in the named
   *  directory.  You should pass readOnly=true, since it
   *  gives much better concurrent performance, unless you
   *  intend to do write operations (delete documents or
   *  change norms) with the underlying IndexReader.
   * @param path directory where IndexReader will be opened
   * @param readOnly if true, the underlying IndexReader
   * will be opened readOnly
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @deprecated Use {@link IndexSearcher#IndexSearcher(IndexReader)} instead.
   */
  @Deprecated
  public IndexSearcher(Directory path, boolean readOnly) throws CorruptIndexException, IOException {
    this(IndexReader.open(path, readOnly), true, null);
  }

  /** Creates a searcher searching the provided index. */
  public IndexSearcher(IndexReader r) {
    this(r, false, null);
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
    this(r, false, executor);
  }

  /** Expert: directly specify the reader, subReaders and
   *  their docID starts.
   * 
   * @lucene.experimental */
  public IndexSearcher(IndexReader reader, IndexReader[] subReaders, int[] docStarts) {
    this(reader, subReaders, docStarts, null);
  }
  
  // Used only when we are an atomic sub-searcher in a parent
  // IndexSearcher that has an ExecutorService, to record
  // our docBase in the parent IndexSearcher:
  private IndexSearcher(IndexReader r, int docBase) {
    reader = r;
    this.executor = null;
    closeReader = false;
    this.docBase = docBase;
    subReaders = new IndexReader[] {r};
    docStarts = new int[] {0};
    subSearchers = null;
  }

  /** Expert: directly specify the reader, subReaders and
   *  their docID starts, and an ExecutorService.  In this
   *  case, each segment will be separately searched using the
   *  ExecutorService.  IndexSearcher will not
   *  shutdown/awaitTermination this ExecutorService on
   *  close; you must do so, eventually, on your own.  NOTE:
   *  if you are using {@link NIOFSDirectory}, do not use
   *  the shutdownNow method of ExecutorService as this uses
   *  Thread.interrupt under-the-hood which can silently
   *  close file descriptors (see <a
   *  href="https://issues.apache.org/jira/browse/LUCENE-2239">LUCENE-2239</a>).
   * 
   * @lucene.experimental */
  public IndexSearcher(IndexReader reader, IndexReader[] subReaders, int[] docStarts, ExecutorService executor) {
    this.reader = reader;
    this.subReaders = subReaders;
    this.docStarts = docStarts;
    if (executor == null) {
      subSearchers = null;
    } else {
      subSearchers = new IndexSearcher[subReaders.length];
      for(int i=0;i<subReaders.length;i++) {
        subSearchers[i] = new IndexSearcher(subReaders[i], docStarts[i]);
      }
    }
    closeReader = false;
    this.executor = executor;
    docBase = 0;
  }

  private IndexSearcher(IndexReader r, boolean closeReader, ExecutorService executor) {
    reader = r;
    this.executor = executor;
    this.closeReader = closeReader;

    List<IndexReader> subReadersList = new ArrayList<IndexReader>();
    gatherSubReaders(subReadersList, reader);
    subReaders = subReadersList.toArray(new IndexReader[subReadersList.size()]);
    docStarts = new int[subReaders.length];
    int maxDoc = 0;
    for (int i = 0; i < subReaders.length; i++) {
      docStarts[i] = maxDoc;
      maxDoc += subReaders[i].maxDoc();
    }
    if (executor == null) {
      subSearchers = null;
    } else {
      subSearchers = new IndexSearcher[subReaders.length];
      for (int i = 0; i < subReaders.length; i++) {
        subSearchers[i] = new IndexSearcher(subReaders[i], docStarts[i]);
      }
    }
    docBase = 0;
  }

  protected void gatherSubReaders(List<IndexReader> allSubReaders, IndexReader r) {
    ReaderUtil.gatherSubReaders(allSubReaders, r);
  }

  /** Return the {@link IndexReader} this searches. */
  public IndexReader getIndexReader() {
    return reader;
  }

  /** Returns the atomic subReaders used by this searcher. */
  public IndexReader[] getSubReaders() {
    return subReaders;
  }

  /** Expert: Returns one greater than the largest possible document number.
   * 
   * @see org.apache.lucene.index.IndexReader#maxDoc()
   */
  @Override
  public int maxDoc() {
    return reader.maxDoc();
  }

  /** Returns total docFreq for this term. */
  @Override
  public int docFreq(final Term term) throws IOException {
    if (executor == null) {
      return reader.docFreq(term);
    } else {
      final ExecutionHelper<Integer> runner = new ExecutionHelper<Integer>(executor);
      for(int i = 0; i < subReaders.length; i++) {
        final IndexSearcher searchable = subSearchers[i];
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
  }

  /* Sugar for .getIndexReader().document(docID) */
  @Override
  public Document doc(int docID) throws CorruptIndexException, IOException {
    return reader.document(docID);
  }
  
  /* Sugar for .getIndexReader().document(docID, fieldSelector) */
  @Override
  public Document doc(int docID, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
    return reader.document(docID, fieldSelector);
  }
  
  /** Expert: Set the Similarity implementation used by this Searcher.
   *
   * @see Similarity#setDefault(Similarity)
   */
  @Override
  public void setSimilarity(Similarity similarity) {
    super.setSimilarity(similarity);
  }

  @Override
  public Similarity getSimilarity() {
    return super.getSimilarity();
  }

  /**
   * Note that the underlying IndexReader is not closed, if
   * IndexSearcher was constructed with IndexSearcher(IndexReader r).
   * If the IndexReader was supplied implicitly by specifying a directory, then
   * the IndexReader is closed.
   */
  @Override
  public void close() throws IOException {
    if (closeReader) {
      reader.close();
    }
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
    return searchAfter(after, query, null, n);
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
    return search(createNormalizedWeight(query), filter, after, n);
  }
  
  /** Finds the top <code>n</code>
   * hits for <code>query</code>.
   *
   * @throws BooleanQuery.TooManyClauses
   */
  @Override
  public TopDocs search(Query query, int n)
    throws IOException {
    return search(query, null, n);
  }


  /** Finds the top <code>n</code>
   * hits for <code>query</code>, applying <code>filter</code> if non-null.
   *
   * @throws BooleanQuery.TooManyClauses
   */
  @Override
  public TopDocs search(Query query, Filter filter, int n)
    throws IOException {
    return search(createNormalizedWeight(query), filter, n);
  }

  /** Lower-level search API.
   *
   * <p>{@link Collector#collect(int)} is called for every matching
   * document.
   * <br>Collector-based access to remote indexes is discouraged.
   *
   * <p>Applications should only use this if they need <i>all</i> of the
   * matching documents.  The high-level search API ({@link
   * Searcher#search(Query, Filter, int)}) is usually more efficient, as it skips
   * non-high-scoring hits.
   *
   * @param query to match documents
   * @param filter if non-null, used to permit documents to be collected.
   * @param results to receive hits
   * @throws BooleanQuery.TooManyClauses
   */
  @Override
  public void search(Query query, Filter filter, Collector results)
    throws IOException {
    search(createNormalizedWeight(query), filter, results);
  }

  /** Lower-level search API.
  *
  * <p>{@link Collector#collect(int)} is called for every matching document.
  *
  * <p>Applications should only use this if they need <i>all</i> of the
  * matching documents.  The high-level search API ({@link
  * Searcher#search(Query, int)}) is usually more efficient, as it skips
  * non-high-scoring hits.
  * <p>Note: The <code>score</code> passed to this method is a raw score.
  * In other words, the score will not necessarily be a float whose value is
  * between 0 and 1.
  * @throws BooleanQuery.TooManyClauses
  */
  @Override
  public void search(Query query, Collector results)
    throws IOException {
    search(createNormalizedWeight(query), null, results);
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
  @Override
  public TopFieldDocs search(Query query, Filter filter, int n,
                             Sort sort) throws IOException {
    return search(createNormalizedWeight(query), filter, n, sort);
  }

  /**
   * Search implementation with arbitrary sorting and no filter.
   * @param query The query to search for
   * @param n Return only the top n results
   * @param sort The {@link org.apache.lucene.search.Sort} object
   * @return The top docs, sorted according to the supplied {@link org.apache.lucene.search.Sort} instance
   * @throws IOException
   */
  @Override
  public TopFieldDocs search(Query query, int n,
                             Sort sort) throws IOException {
    return search(createNormalizedWeight(query), null, n, sort);
  }

  /** Expert: Low-level search implementation.  Finds the top <code>n</code>
   * hits for <code>query</code>, applying <code>filter</code> if non-null.
   *
   * <p>Applications should usually call {@link Searcher#search(Query,int)} or
   * {@link Searcher#search(Query,Filter,int)} instead.
   * @throws BooleanQuery.TooManyClauses
   */
  @Override
  public TopDocs search(Weight weight, Filter filter, int nDocs) throws IOException {
    return search(weight, filter, null, nDocs);
  }
  
  /**
   * Expert: Low-level search implementation.  Finds the top <code>n</code>
   * hits for <code>query</code>, applying <code>filter</code> if non-null,
   * returning results after <code>after</code>.
   * 
   * @throws BooleanQuery.TooManyClauses
   */
  protected TopDocs search(Weight weight, Filter filter, ScoreDoc after, int nDocs) throws IOException {
    if (executor == null) {
      // single thread
      int limit = reader.maxDoc();
      if (limit == 0) {
        limit = 1;
      }
      nDocs = Math.min(nDocs, limit);
      TopScoreDocCollector collector = TopScoreDocCollector.create(nDocs, after, !weight.scoresDocsOutOfOrder());
      search(weight, filter, collector);
      return collector.topDocs();
    } else {
      final HitQueue hq = new HitQueue(nDocs, false);
      final Lock lock = new ReentrantLock();
      final ExecutionHelper<TopDocs> runner = new ExecutionHelper<TopDocs>(executor);
    
      for (int i = 0; i < subReaders.length; i++) { // search each sub
        runner.submit(
                      new MultiSearcherCallableNoSort(lock, subSearchers[i], weight, filter, after, nDocs, hq));
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

  /** Expert: Low-level search implementation with arbitrary sorting.  Finds
   * the top <code>n</code> hits for <code>query</code>, applying
   * <code>filter</code> if non-null, and sorting the hits by the criteria in
   * <code>sort</code>.
   *
   * <p>Applications should usually call {@link
   * Searcher#search(Query,Filter,int,Sort)} instead.
   * 
   * @throws BooleanQuery.TooManyClauses
   */
  @Override
  public TopFieldDocs search(Weight weight, Filter filter,
      final int nDocs, Sort sort) throws IOException {
    return search(weight, filter, nDocs, sort, true);
  }

  /**
   * Just like {@link #search(Weight, Filter, int, Sort)}, but you choose
   * whether or not the fields in the returned {@link FieldDoc} instances should
   * be set by specifying fillFields.
   *
   * <p>NOTE: this does not compute scores by default.  If you
   * need scores, create a {@link TopFieldCollector}
   * instance by calling {@link TopFieldCollector#create} and
   * then pass that to {@link #search(Weight, Filter,
   * Collector)}.</p>
   */
  protected TopFieldDocs search(Weight weight, Filter filter, int nDocs,
                                Sort sort, boolean fillFields)
      throws IOException {

    if (sort == null) throw new NullPointerException();

    if (executor == null) {
      // single thread
      int limit = reader.maxDoc();
      if (limit == 0) {
        limit = 1;
      }
      nDocs = Math.min(nDocs, limit);

      TopFieldCollector collector = TopFieldCollector.create(sort, nDocs,
                                                             fillFields, fieldSortDoTrackScores, fieldSortDoMaxScore, !weight.scoresDocsOutOfOrder());
      search(weight, filter, collector);
      return (TopFieldDocs) collector.topDocs();
    } else {
      final TopFieldCollector topCollector = TopFieldCollector.create(sort, nDocs,
                                                                      fillFields,
                                                                      fieldSortDoTrackScores,
                                                                      fieldSortDoMaxScore,
                                                                      false);

      final Lock lock = new ReentrantLock();
      final ExecutionHelper<TopFieldDocs> runner = new ExecutionHelper<TopFieldDocs>(executor);
      for (int i = 0; i < subReaders.length; i++) { // search each sub
        runner.submit(
                      new MultiSearcherCallableWithSort(lock, subSearchers[i], weight, filter, nDocs, topCollector, sort));
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
   * Lower-level search API.
   * 
   * <p>
   * {@link Collector#collect(int)} is called for every document. <br>
   * Collector-based access to remote indexes is discouraged.
   * 
   * <p>
   * Applications should only use this if they need <i>all</i> of the matching
   * documents. The high-level search API ({@link Searcher#search(Query,int)}) is
   * usually more efficient, as it skips non-high-scoring hits.
   * 
   * @param weight
   *          to match documents
   * @param filter
   *          if non-null, used to permit documents to be collected.
   * @param collector
   *          to receive hits
   * @throws BooleanQuery.TooManyClauses
   */
  @Override
  public void search(Weight weight, Filter filter, Collector collector)
      throws IOException {

    // TODO: should we make this
    // threaded...?  the Collector could be sync'd?

    // always use single thread:
    for (int i = 0; i < subReaders.length; i++) { // search each subreader
      collector.setNextReader(subReaders[i], docBase + docStarts[i]);
      final Scorer scorer = (filter == null) ?
        weight.scorer(subReaders[i], !collector.acceptsDocsOutOfOrder(), true) :
        FilteredQuery.getFilteredScorer(subReaders[i], getSimilarity(), weight, weight, filter);
      if (scorer != null) {
        scorer.score(collector);
      }
    }
  }

  /** Expert: called to re-write queries into primitive queries.
   * @throws BooleanQuery.TooManyClauses
   */
  @Override
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
  @Override
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
   * <p>Applications should call {@link Searcher#explain(Query, int)}.
   * @throws BooleanQuery.TooManyClauses
   */
  @Override
  public Explanation explain(Weight weight, int doc) throws IOException {
    int n = ReaderUtil.subIndex(doc, docStarts);
    int deBasedDoc = doc - docStarts[n];
    
    return weight.explain(subReaders[n], deBasedDoc);
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
    if (subSearchers != null) { // propagate settings to subs
      for (IndexSearcher sub : subSearchers) {
        sub.setDefaultFieldSortScoring(doTrackScores, doMaxScore);
      }
    }
  }

  /**
   * Creates a normalized weight for a top-level {@link Query}.
   * The query is rewritten by this method and {@link Query#createWeight} called,
   * afterwards the {@link Weight} is normalized. The returned {@code Weight}
   * can then directly be used to get a {@link Scorer}.
   * @lucene.internal
   */
  public Weight createNormalizedWeight(Query query) throws IOException {
    return super.createNormalizedWeight(query);
  }

  /**
   * A thread subclass for searching a single searchable 
   */
  private static final class MultiSearcherCallableNoSort implements Callable<TopDocs> {

    private final Lock lock;
    private final IndexSearcher searchable;
    private final Weight weight;
    private final Filter filter;
    private final ScoreDoc after;
    private final int nDocs;
    private final HitQueue hq;

    public MultiSearcherCallableNoSort(Lock lock, IndexSearcher searchable, Weight weight,
        Filter filter, ScoreDoc after, int nDocs, HitQueue hq) {
      this.lock = lock;
      this.searchable = searchable;
      this.weight = weight;
      this.filter = filter;
      this.after = after;
      this.nDocs = nDocs;
      this.hq = hq;
    }

    public TopDocs call() throws IOException {
      final TopDocs docs;
      // we could call the 4-arg method, but we want to invoke the old method
      // for backwards purposes unless someone is using the new searchAfter.
      if (after == null) {
        docs = searchable.search (weight, filter, nDocs);
      } else {
        docs = searchable.search (weight, filter, after, nDocs);
      }
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
  private static final class MultiSearcherCallableWithSort implements Callable<TopFieldDocs> {

    private final Lock lock;
    private final IndexSearcher searchable;
    private final Weight weight;
    private final Filter filter;
    private final int nDocs;
    private final TopFieldCollector hq;
    private final Sort sort;

    public MultiSearcherCallableWithSort(Lock lock, IndexSearcher searchable, Weight weight,
                                         Filter filter, int nDocs, TopFieldCollector hq, Sort sort) {
      this.lock = lock;
      this.searchable = searchable;
      this.weight = weight;
      this.filter = filter;
      this.nDocs = nDocs;
      this.hq = hq;
      this.sort = sort;
    }

    private final class FakeScorer extends Scorer {
      float score;
      int doc;

      public FakeScorer() {
        super(null, null);
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
      final TopFieldDocs docs = searchable.search (weight, filter, nDocs, sort);
      // If one of the Sort fields is FIELD_DOC, need to fix its values, so that
      // it will break ties by doc Id properly. Otherwise, it will compare to
      // 'relative' doc Ids, that belong to two different searchables.
      for (int j = 0; j < docs.fields.length; j++) {
        if (docs.fields[j].getType() == SortField.DOC) {
          // iterate over the score docs and change their fields value
          for (int j2 = 0; j2 < docs.scoreDocs.length; j2++) {
            FieldDoc fd = (FieldDoc) docs.scoreDocs[j2];
            fd.fields[j] = Integer.valueOf(((Integer) fd.fields[j]).intValue());
          }
          break;
        }
      }

      lock.lock();
      try {
        hq.setNextReader(searchable.getIndexReader(), searchable.docBase);
        hq.setScorer(fakeScorer);
        for(ScoreDoc scoreDoc : docs.scoreDocs) {
          final int docID = scoreDoc.doc - searchable.docBase;
          fakeScorer.doc = docID;
          fakeScorer.score = scoreDoc.score;
          hq.collect(docID);
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

  @Override
  public String toString() {
    return "IndexSearcher(" + reader + ")";
  }
}
