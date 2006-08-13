package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Vector;


/**
  An IndexWriter creates and maintains an index.

  The third argument to the 
  <a href="#IndexWriter(org.apache.lucene.store.Directory, org.apache.lucene.analysis.Analyzer, boolean)"><b>constructor</b></a>
  determines whether a new index is created, or whether an existing index is
  opened for the addition of new documents.

  In either case, documents are added with the <a
  href="#addDocument(org.apache.lucene.document.Document)"><b>addDocument</b></a> method.  
  When finished adding documents, <a href="#close()"><b>close</b></a> should be called.

  <p>If an index will not have more documents added for a while and optimal search
  performance is desired, then the <a href="#optimize()"><b>optimize</b></a>
  method should be called before the index is closed.
  
  <p>Opening an IndexWriter creates a lock file for the directory in use. Trying to open
  another IndexWriter on the same directory will lead to an IOException. The IOException
  is also thrown if an IndexReader on the same directory is used to delete documents
  from the index.
  
  @see IndexModifier IndexModifier supports the important methods of IndexWriter plus deletion
  */

public class IndexWriter {

  /**
   * Default value for the write lock timeout (1,000).
   * @see #setDefaultWriteLockTimeout
   */
  public static long WRITE_LOCK_TIMEOUT = 1000;

  private long writeLockTimeout = WRITE_LOCK_TIMEOUT;

  /**
   * Default value for the commit lock timeout (10,000).
   * @see #setDefaultCommitLockTimeout
   */
  public static long COMMIT_LOCK_TIMEOUT = 10000;

  private long commitLockTimeout = COMMIT_LOCK_TIMEOUT;

  public static final String WRITE_LOCK_NAME = "write.lock";
  public static final String COMMIT_LOCK_NAME = "commit.lock";

  /**
   * Default value is 10. Change using {@link #setMergeFactor(int)}.
   */
  public final static int DEFAULT_MERGE_FACTOR = 10;

  /**
   * Default value is 10. Change using {@link #setMaxBufferedDocs(int)}.
   */
  public final static int DEFAULT_MAX_BUFFERED_DOCS = 10;

  /**
   * Default value is {@link Integer#MAX_VALUE}. Change using {@link #setMaxMergeDocs(int)}.
   */
  public final static int DEFAULT_MAX_MERGE_DOCS = Integer.MAX_VALUE;

  /**
   * Default value is 10,000. Change using {@link #setMaxFieldLength(int)}.
   */
  public final static int DEFAULT_MAX_FIELD_LENGTH = 10000;

  /**
   * Default value is 128. Change using {@link #setTermIndexInterval(int)}.
   */
  public final static int DEFAULT_TERM_INDEX_INTERVAL = 128;
  
  private Directory directory;  // where this index resides
  private Analyzer analyzer;    // how to analyze text

  private Similarity similarity = Similarity.getDefault(); // how to normalize

  private SegmentInfos segmentInfos = new SegmentInfos(); // the segments
  private final Directory ramDirectory = new RAMDirectory(); // for temp segs

  private long bufferedDocCount = 0;
  private Lock writeLock;

  private int termIndexInterval = DEFAULT_TERM_INDEX_INTERVAL;

  /** Use compound file setting. Defaults to true, minimizing the number of
   * files used.  Setting this to false may improve indexing performance, but
   * may also cause file handle problems.
   */
  private boolean useCompoundFile = true;
  
  private boolean closeDir;

  /** Get the current setting of whether to use the compound file format.
   *  Note that this just returns the value you set with setUseCompoundFile(boolean)
   *  or the default. You cannot use this to query the status of an existing index.
   *  @see #setUseCompoundFile(boolean)
   */
  public boolean getUseCompoundFile() {
    return useCompoundFile;
  }

  /** Setting to turn on usage of a compound file. When on, multiple files
   *  for each segment are merged into a single file once the segment creation
   *  is finished. This is done regardless of what directory is in use.
   */
  public void setUseCompoundFile(boolean value) {
    useCompoundFile = value;
  }

  /** Expert: Set the Similarity implementation used by this IndexWriter.
   *
   * @see Similarity#setDefault(Similarity)
   */
  public void setSimilarity(Similarity similarity) {
    this.similarity = similarity;
  }

  /** Expert: Return the Similarity implementation used by this IndexWriter.
   *
   * <p>This defaults to the current value of {@link Similarity#getDefault()}.
   */
  public Similarity getSimilarity() {
    return this.similarity;
  }

  /** Expert: Set the interval between indexed terms.  Large values cause less
   * memory to be used by IndexReader, but slow random-access to terms.  Small
   * values cause more memory to be used by an IndexReader, and speed
   * random-access to terms.
   *
   * This parameter determines the amount of computation required per query
   * term, regardless of the number of documents that contain that term.  In
   * particular, it is the maximum number of other terms that must be
   * scanned before a term is located and its frequency and position information
   * may be processed.  In a large index with user-entered query terms, query
   * processing time is likely to be dominated not by term lookup but rather
   * by the processing of frequency and positional data.  In a small index
   * or when many uncommon query terms are generated (e.g., by wildcard
   * queries) term lookup may become a dominant cost.
   *
   * In particular, <code>numUniqueTerms/interval</code> terms are read into
   * memory by an IndexReader, and, on average, <code>interval/2</code> terms
   * must be scanned for each random term access.
   *
   * @see #DEFAULT_TERM_INDEX_INTERVAL
   */
  public void setTermIndexInterval(int interval) {
    this.termIndexInterval = interval;
  }

  /** Expert: Return the interval between indexed terms.
   *
   * @see #setTermIndexInterval(int)
   */
  public int getTermIndexInterval() { return termIndexInterval; }

  /**
   * Constructs an IndexWriter for the index in <code>path</code>.
   * Text will be analyzed with <code>a</code>.  If <code>create</code>
   * is true, then a new, empty index will be created in
   * <code>path</code>, replacing the index already there, if any.
   *
   * @param path the path to the index directory
   * @param a the analyzer to use
   * @param create <code>true</code> to create the index or overwrite
   *  the existing one; <code>false</code> to append to the existing
   *  index
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist, and <code>create</code> is
   *  <code>false</code>
   */
  public IndexWriter(String path, Analyzer a, boolean create)
       throws IOException {
    this(FSDirectory.getDirectory(path, create), a, create, true);
  }

  /**
   * Constructs an IndexWriter for the index in <code>path</code>.
   * Text will be analyzed with <code>a</code>.  If <code>create</code>
   * is true, then a new, empty index will be created in
   * <code>path</code>, replacing the index already there, if any.
   *
   * @param path the path to the index directory
   * @param a the analyzer to use
   * @param create <code>true</code> to create the index or overwrite
   *  the existing one; <code>false</code> to append to the existing
   *  index
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist, and <code>create</code> is
   *  <code>false</code>
   */
  public IndexWriter(File path, Analyzer a, boolean create)
       throws IOException {
    this(FSDirectory.getDirectory(path, create), a, create, true);
  }

  /**
   * Constructs an IndexWriter for the index in <code>d</code>.
   * Text will be analyzed with <code>a</code>.  If <code>create</code>
   * is true, then a new, empty index will be created in
   * <code>d</code>, replacing the index already there, if any.
   *
   * @param d the index directory
   * @param a the analyzer to use
   * @param create <code>true</code> to create the index or overwrite
   *  the existing one; <code>false</code> to append to the existing
   *  index
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist, and <code>create</code> is
   *  <code>false</code>
   */
  public IndexWriter(Directory d, Analyzer a, boolean create)
       throws IOException {
    this(d, a, create, false);
  }
  
  private IndexWriter(Directory d, Analyzer a, final boolean create, boolean closeDir)
    throws IOException {
      this.closeDir = closeDir;
      directory = d;
      analyzer = a;

      Lock writeLock = directory.makeLock(IndexWriter.WRITE_LOCK_NAME);
      if (!writeLock.obtain(writeLockTimeout)) // obtain write lock
        throw new IOException("Index locked for write: " + writeLock);
      this.writeLock = writeLock;                   // save it

      synchronized (directory) {        // in- & inter-process sync
        new Lock.With(directory.makeLock(IndexWriter.COMMIT_LOCK_NAME), commitLockTimeout) {
            public Object doBody() throws IOException {
              if (create)
                segmentInfos.write(directory);
              else
                segmentInfos.read(directory);
              return null;
            }
          }.run();
      }
  }

  /** Determines the largest number of documents ever merged by addDocument().
   * Small values (e.g., less than 10,000) are best for interactive indexing,
   * as this limits the length of pauses while indexing to a few seconds.
   * Larger values are best for batched indexing and speedier searches.
   *
   * <p>The default value is {@link Integer#MAX_VALUE}.
   */
  public void setMaxMergeDocs(int maxMergeDocs) {
    this.maxMergeDocs = maxMergeDocs;
  }

  /**
   * @see #setMaxMergeDocs
   */
  public int getMaxMergeDocs() {
    return maxMergeDocs;
  }

  /**
   * The maximum number of terms that will be indexed for a single field in a
   * document.  This limits the amount of memory required for indexing, so that
   * collections with very large files will not crash the indexing process by
   * running out of memory.<p/>
   * Note that this effectively truncates large documents, excluding from the
   * index terms that occur further in the document.  If you know your source
   * documents are large, be sure to set this value high enough to accomodate
   * the expected size.  If you set it to Integer.MAX_VALUE, then the only limit
   * is your memory, but you should anticipate an OutOfMemoryError.<p/>
   * By default, no more than 10,000 terms will be indexed for a field.
   */
  public void setMaxFieldLength(int maxFieldLength) {
    this.maxFieldLength = maxFieldLength;
  }
  
  /**
   * @see #setMaxFieldLength
   */
  public int getMaxFieldLength() {
    return maxFieldLength;
  }

  /** Determines the minimal number of documents required before the buffered
   * in-memory documents are merging and a new Segment is created.
   * Since Documents are merged in a {@link org.apache.lucene.store.RAMDirectory},
   * large value gives faster indexing.  At the same time, mergeFactor limits
   * the number of files open in a FSDirectory.
   *
   * <p> The default value is 10.
   * 
   * @throws IllegalArgumentException if maxBufferedDocs is smaller than 2
   */
  public void setMaxBufferedDocs(int maxBufferedDocs) {
    if (maxBufferedDocs < 2)
      throw new IllegalArgumentException("maxBufferedDocs must at least be 2");
    this.minMergeDocs = maxBufferedDocs;
  }

  /**
   * @see #setMaxBufferedDocs
   */
  public int getMaxBufferedDocs() {
    return minMergeDocs;
  }

  /** Determines how often segment indices are merged by addDocument().  With
   * smaller values, less RAM is used while indexing, and searches on
   * unoptimized indices are faster, but indexing speed is slower.  With larger
   * values, more RAM is used during indexing, and while searches on unoptimized
   * indices are slower, indexing is faster.  Thus larger values (> 10) are best
   * for batch index creation, and smaller values (< 10) for indices that are
   * interactively maintained.
   *
   * <p>This must never be less than 2.  The default value is 10.
   */
  public void setMergeFactor(int mergeFactor) {
    if (mergeFactor < 2)
      throw new IllegalArgumentException("mergeFactor cannot be less than 2");
    this.mergeFactor = mergeFactor;
  }

  /**
   * @see #setMergeFactor
   */
  public int getMergeFactor() {
    return mergeFactor;
  }

  /** If non-null, information about merges and a message when
   * maxFieldLength is reached will be printed to this.
   */
  public void setInfoStream(PrintStream infoStream) {
    this.infoStream = infoStream;
  }

  /**
   * @see #setInfoStream
   */
  public PrintStream getInfoStream() {
    return infoStream;
  }

  /**
   * Sets the maximum time to wait for a commit lock (in milliseconds) for this instance of IndexWriter.  @see
   * @see #setDefaultCommitLockTimeout to change the default value for all instances of IndexWriter.
   */
  public void setCommitLockTimeout(long commitLockTimeout) {
    this.commitLockTimeout = commitLockTimeout;
  }

  /**
   * @see #setCommitLockTimeout
   */
  public long getCommitLockTimeout() {
    return commitLockTimeout;
  }

  /**
   * Sets the default (for any instance of IndexWriter) maximum time to wait for a commit lock (in milliseconds)
   */
  public static void setDefaultCommitLockTimeout(long commitLockTimeout) {
    IndexWriter.COMMIT_LOCK_TIMEOUT = commitLockTimeout;
  }

  /**
   * @see #setDefaultCommitLockTimeout
   */
  public static long getDefaultCommitLockTimeout() {
    return IndexWriter.COMMIT_LOCK_TIMEOUT;
  }

  /**
   * Sets the maximum time to wait for a write lock (in milliseconds) for this instance of IndexWriter.  @see
   * @see #setDefaultWriteLockTimeout to change the default value for all instances of IndexWriter.
   */
  public void setWriteLockTimeout(long writeLockTimeout) {
    this.writeLockTimeout = writeLockTimeout;
  }

  /**
   * @see #setWriteLockTimeout
   */
  public long getWriteLockTimeout() {
    return writeLockTimeout;
  }

  /**
   * Sets the default (for any instance of IndexWriter) maximum time to wait for a write lock (in
   * milliseconds).
   */
  public static void setDefaultWriteLockTimeout(long writeLockTimeout) {
    IndexWriter.WRITE_LOCK_TIMEOUT = writeLockTimeout;
  }

  /**
   * @see #setDefaultWriteLockTimeout
   */
  public static long getDefaultWriteLockTimeout() {
    return IndexWriter.WRITE_LOCK_TIMEOUT;
  }

  /** Flushes all changes to an index and closes all associated files. */
  public synchronized void close() throws IOException {
    flushRamSegments();
    ramDirectory.close();
    if (writeLock != null) {
      writeLock.release();                          // release write lock
      writeLock = null;
    }
    if(closeDir)
      directory.close();
  }

  /** Release the write lock, if needed. */
  protected void finalize() throws IOException {
    if (writeLock != null) {
      writeLock.release();                        // release write lock
      writeLock = null;
    }
  }

  /** Returns the Directory used by this index. */
  public Directory getDirectory() {
      return directory;
  }

  /** Returns the analyzer used by this index. */
  public Analyzer getAnalyzer() {
      return analyzer;
  }


  /** Returns the number of documents currently in this index. */
  public synchronized int docCount() {
    int count = 0;
    for (int i = 0; i < segmentInfos.size(); i++) {
      SegmentInfo si = segmentInfos.info(i);
      count += si.docCount;
    }
    return count;
  }

  /**
   * The maximum number of terms that will be indexed for a single field in a
   * document.  This limits the amount of memory required for indexing, so that
   * collections with very large files will not crash the indexing process by
   * running out of memory.<p/>
   * Note that this effectively truncates large documents, excluding from the
   * index terms that occur further in the document.  If you know your source
   * documents are large, be sure to set this value high enough to accomodate
   * the expected size.  If you set it to Integer.MAX_VALUE, then the only limit
   * is your memory, but you should anticipate an OutOfMemoryError.<p/>
   * By default, no more than 10,000 terms will be indexed for a field.
   *
   */
  private int maxFieldLength = DEFAULT_MAX_FIELD_LENGTH;

  /**
   * Adds a document to this index.  If the document contains more than
   * {@link #setMaxFieldLength(int)} terms for a given field, the remainder are
   * discarded.
   */
  public void addDocument(Document doc) throws IOException {
    addDocument(doc, analyzer);
  }

  /**
   * Adds a document to this index, using the provided analyzer instead of the
   * value of {@link #getAnalyzer()}.  If the document contains more than
   * {@link #setMaxFieldLength(int)} terms for a given field, the remainder are
   * discarded.
   */
  public void addDocument(Document doc, Analyzer analyzer) throws IOException {
    DocumentWriter dw =
      new DocumentWriter(ramDirectory, analyzer, this);
    dw.setInfoStream(infoStream);
    String segmentName = newSegmentName();
    dw.addDocument(segmentName, doc);
    synchronized (this) {
      segmentInfos.addElement(new SegmentInfo(segmentName, 1, ramDirectory));
      bufferedDocCount++;
      maybeMergeSegments();
    }
  }

  final int getSegmentsCounter(){
    return segmentInfos.counter;
  }
  
  private final synchronized String newSegmentName() {
    return "_" + Integer.toString(segmentInfos.counter++, Character.MAX_RADIX);
  }

  /** Determines how often segment indices are merged by addDocument().  With
   * smaller values, less RAM is used while indexing, and searches on
   * unoptimized indices are faster, but indexing speed is slower.  With larger
   * values, more RAM is used during indexing, and while searches on unoptimized
   * indices are slower, indexing is faster.  Thus larger values (> 10) are best
   * for batch index creation, and smaller values (< 10) for indices that are
   * interactively maintained.
   *
   * <p>This must never be less than 2.  The default value is {@link #DEFAULT_MERGE_FACTOR}.

   */
  private int mergeFactor = DEFAULT_MERGE_FACTOR;

  /** Determines the minimal number of documents required before the buffered
   * in-memory documents are merging and a new Segment is created.
   * Since Documents are merged in a {@link org.apache.lucene.store.RAMDirectory},
   * large value gives faster indexing.  At the same time, mergeFactor limits
   * the number of files open in a FSDirectory.
   *
   * <p> The default value is {@link #DEFAULT_MAX_BUFFERED_DOCS}.

   */
  private int minMergeDocs = DEFAULT_MAX_BUFFERED_DOCS;


  /** Determines the largest number of documents ever merged by addDocument().
   * Small values (e.g., less than 10,000) are best for interactive indexing,
   * as this limits the length of pauses while indexing to a few seconds.
   * Larger values are best for batched indexing and speedier searches.
   *
   * <p>The default value is {@link #DEFAULT_MAX_MERGE_DOCS}.

   */
  private int maxMergeDocs = DEFAULT_MAX_MERGE_DOCS;

  /** If non-null, information about merges will be printed to this.

   */
  private PrintStream infoStream = null;

  /** Merges all segments together into a single segment, optimizing an index
      for search. */
  public synchronized void optimize() throws IOException {
    flushRamSegments();
    while (segmentInfos.size() > 1 ||
           (segmentInfos.size() == 1 &&
            (SegmentReader.hasDeletions(segmentInfos.info(0)) ||
             segmentInfos.info(0).dir != directory ||
             (useCompoundFile &&
              (!SegmentReader.usesCompoundFile(segmentInfos.info(0)) ||
                SegmentReader.hasSeparateNorms(segmentInfos.info(0))))))) {
      int minSegment = segmentInfos.size() - mergeFactor;
      mergeSegments(minSegment < 0 ? 0 : minSegment);
    }
  }

  /** Merges all segments from an array of indexes into this index.
   *
   * <p>This may be used to parallelize batch indexing.  A large document
   * collection can be broken into sub-collections.  Each sub-collection can be
   * indexed in parallel, on a different thread, process or machine.  The
   * complete index can then be created by merging sub-collection indexes
   * with this method.
   *
   * <p>After this completes, the index is optimized. */
  public synchronized void addIndexes(Directory[] dirs)
      throws IOException {
    optimize();					  // start with zero or 1 seg

    int start = segmentInfos.size();

    for (int i = 0; i < dirs.length; i++) {
      SegmentInfos sis = new SegmentInfos();	  // read infos from dir
      sis.read(dirs[i]);
      for (int j = 0; j < sis.size(); j++) {
        segmentInfos.addElement(sis.info(j));	  // add each info
      }
    }
    
    // merge newly added segments in log(n) passes
    while (segmentInfos.size() > start+mergeFactor) {
      for (int base = start; base < segmentInfos.size(); base++) {
        int end = Math.min(segmentInfos.size(), base+mergeFactor);
        if (end-base > 1)
          mergeSegments(base, end);
      }
    }

    optimize();					  // final cleanup
  }

  /** Merges the provided indexes into this index.
   * <p>After this completes, the index is optimized. </p>
   * <p>The provided IndexReaders are not closed.</p>
   */
  public synchronized void addIndexes(IndexReader[] readers)
    throws IOException {

    optimize();					  // start with zero or 1 seg

    final String mergedName = newSegmentName();
    SegmentMerger merger = new SegmentMerger(this, mergedName);

    final Vector segmentsToDelete = new Vector();
    IndexReader sReader = null;
    if (segmentInfos.size() == 1){ // add existing index, if any
        sReader = SegmentReader.get(segmentInfos.info(0));
        merger.add(sReader);
        segmentsToDelete.addElement(sReader);   // queue segment for deletion
    }
      
    for (int i = 0; i < readers.length; i++)      // add new indexes
      merger.add(readers[i]);

    int docCount = merger.merge();                // merge 'em

    segmentInfos.setSize(0);                      // pop old infos & add new
    segmentInfos.addElement(new SegmentInfo(mergedName, docCount, directory));
    
    if(sReader != null)
        sReader.close();

    synchronized (directory) {			  // in- & inter-process sync
      new Lock.With(directory.makeLock(COMMIT_LOCK_NAME), commitLockTimeout) {
	  public Object doBody() throws IOException {
	    segmentInfos.write(directory);	  // commit changes
	    return null;
	  }
	}.run();
    }
    
    deleteSegments(segmentsToDelete);  // delete now-unused segments

    if (useCompoundFile) {
      final Vector filesToDelete = merger.createCompoundFile(mergedName + ".tmp");
      synchronized (directory) { // in- & inter-process sync
        new Lock.With(directory.makeLock(COMMIT_LOCK_NAME), commitLockTimeout) {
          public Object doBody() throws IOException {
            // make compound file visible for SegmentReaders
            directory.renameFile(mergedName + ".tmp", mergedName + ".cfs");
            return null;
          }
        }.run();
      }

      // delete now unused files of segment 
      deleteFiles(filesToDelete);   
    }
  }

  /** Merges all RAM-resident segments. */
  private final void flushRamSegments() throws IOException {
    int minSegment = segmentInfos.size()-1;
    int docCount = 0;
    while (minSegment >= 0 &&
           (segmentInfos.info(minSegment)).dir == ramDirectory) {
      docCount += segmentInfos.info(minSegment).docCount;
      minSegment--;
    }
    if (minSegment < 0 ||			  // add one FS segment?
        (docCount + segmentInfos.info(minSegment).docCount) > mergeFactor ||
        !(segmentInfos.info(segmentInfos.size()-1).dir == ramDirectory))
      minSegment++;
    if (minSegment >= segmentInfos.size())
      return;					  // none to merge
    mergeSegments(minSegment);
  }

  /** Incremental segment merger.  */
  private final void maybeMergeSegments() throws IOException {
    /**
     *  do not bother checking the segment details to determine
     *  if we should merge, but instead honour the maxBufferedDocs(minMergeDocs)
     *  property to ensure we do not spend time checking for merge conditions
     *  
     */
    if(bufferedDocCount<minMergeDocs) {
        return;
    }
    long targetMergeDocs = minMergeDocs;
    while (targetMergeDocs <= maxMergeDocs) {
      // find segments smaller than current target size
      int minSegment = segmentInfos.size();
      int mergeDocs = 0;
      while (--minSegment >= 0) {
        SegmentInfo si = segmentInfos.info(minSegment);
        if (si.docCount >= targetMergeDocs)
          break;
        mergeDocs += si.docCount;
      }

      if (mergeDocs >= targetMergeDocs)		  // found a merge to do
        mergeSegments(minSegment+1);
      else
        break;

      targetMergeDocs *= mergeFactor;		  // increase target size
    }
  }

  /** Pops segments off of segmentInfos stack down to minSegment, merges them,
    and pushes the merged index onto the top of the segmentInfos stack. */
  private final void mergeSegments(int minSegment)
      throws IOException {
    mergeSegments(minSegment, segmentInfos.size());
  }

  /** Merges the named range of segments, replacing them in the stack with a
   * single segment. */
  private final void mergeSegments(int minSegment, int end)
    throws IOException {
    final String mergedName = newSegmentName();
    if (infoStream != null) infoStream.print("merging segments");
    SegmentMerger merger = new SegmentMerger(this, mergedName);

    final Vector segmentsToDelete = new Vector();
    for (int i = minSegment; i < end; i++) {
      SegmentInfo si = segmentInfos.info(i);
      if (infoStream != null)
        infoStream.print(" " + si.name + " (" + si.docCount + " docs)");
      IndexReader reader = SegmentReader.get(si);
      merger.add(reader);
      if ((reader.directory() == this.directory) || // if we own the directory
          (reader.directory() == this.ramDirectory))
        segmentsToDelete.addElement(reader);   // queue segment for deletion
    }

    int mergedDocCount = merger.merge();

    bufferedDocCount -= mergedDocCount; // update bookkeeping about how many docs we have buffered
    
    if (infoStream != null) {
      infoStream.println(" into "+mergedName+" ("+mergedDocCount+" docs)");
    }

    for (int i = end-1; i > minSegment; i--)     // remove old infos & add new
      segmentInfos.remove(i);
    segmentInfos.set(minSegment, new SegmentInfo(mergedName, mergedDocCount,
                                            directory));

    // close readers before we attempt to delete now-obsolete segments
    merger.closeReaders();

    synchronized (directory) {                 // in- & inter-process sync
      new Lock.With(directory.makeLock(COMMIT_LOCK_NAME), commitLockTimeout) {
          public Object doBody() throws IOException {
            segmentInfos.write(directory);     // commit before deleting
            return null;
          }
        }.run();
    }
    
    deleteSegments(segmentsToDelete);  // delete now-unused segments

    if (useCompoundFile) {
      final Vector filesToDelete = merger.createCompoundFile(mergedName + ".tmp");
      synchronized (directory) { // in- & inter-process sync
        new Lock.With(directory.makeLock(COMMIT_LOCK_NAME), commitLockTimeout) {
          public Object doBody() throws IOException {
            // make compound file visible for SegmentReaders
            directory.renameFile(mergedName + ".tmp", mergedName + ".cfs");
            return null;
          }
        }.run();
      }

      // delete now unused files of segment 
      deleteFiles(filesToDelete);   
    }
  }

  /*
   * Some operating systems (e.g. Windows) don't permit a file to be deleted
   * while it is opened for read (e.g. by another process or thread). So we
   * assume that when a delete fails it is because the file is open in another
   * process, and queue the file for subsequent deletion.
   */

  private final void deleteSegments(Vector segments) throws IOException {
    Vector deletable = new Vector();

    deleteFiles(readDeleteableFiles(), deletable); // try to delete deleteable

    for (int i = 0; i < segments.size(); i++) {
      SegmentReader reader = (SegmentReader)segments.elementAt(i);
      if (reader.directory() == this.directory)
        deleteFiles(reader.files(), deletable);	  // try to delete our files
      else
        deleteFiles(reader.files(), reader.directory()); // delete other files
    }

    writeDeleteableFiles(deletable);		  // note files we can't delete
  }
  
  private final void deleteFiles(Vector files) throws IOException {
    Vector deletable = new Vector();
    deleteFiles(readDeleteableFiles(), deletable); // try to delete deleteable
    deleteFiles(files, deletable);     // try to delete our files
    writeDeleteableFiles(deletable);        // note files we can't delete
  }

  private final void deleteFiles(Vector files, Directory directory)
       throws IOException {
    for (int i = 0; i < files.size(); i++)
      directory.deleteFile((String)files.elementAt(i));
  }

  private final void deleteFiles(Vector files, Vector deletable)
       throws IOException {
    for (int i = 0; i < files.size(); i++) {
      String file = (String)files.elementAt(i);
      try {
        directory.deleteFile(file);		  // try to delete each file
      } catch (IOException e) {			  // if delete fails
        if (directory.fileExists(file)) {
          if (infoStream != null)
            infoStream.println(e.toString() + "; Will re-try later.");
          deletable.addElement(file);		  // add to deletable
        }
      }
    }
  }

  private final Vector readDeleteableFiles() throws IOException {
    Vector result = new Vector();
    if (!directory.fileExists(IndexFileNames.DELETABLE))
      return result;

    IndexInput input = directory.openInput(IndexFileNames.DELETABLE);
    try {
      for (int i = input.readInt(); i > 0; i--)	  // read file names
        result.addElement(input.readString());
    } finally {
      input.close();
    }
    return result;
  }

  private final void writeDeleteableFiles(Vector files) throws IOException {
    IndexOutput output = directory.createOutput("deleteable.new");
    try {
      output.writeInt(files.size());
      for (int i = 0; i < files.size(); i++)
        output.writeString((String)files.elementAt(i));
    } finally {
      output.close();
    }
    directory.renameFile("deleteable.new", IndexFileNames.DELETABLE);
  }
}
