package org.apache.lucene.index;

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

  public static final String WRITE_LOCK_NAME = "write.lock";

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

  private SegmentInfos segmentInfos = new SegmentInfos();       // the segments
  private SegmentInfos ramSegmentInfos = new SegmentInfos();    // the segments in ramDirectory
  private final RAMDirectory ramDirectory = new RAMDirectory(); // for temp segs
  private IndexFileDeleter deleter;

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

      try {
        if (create) {
          // Try to read first.  This is to allow create
          // against an index that's currently open for
          // searching.  In this case we write the next
          // segments_N file with no segments:
          try {
            segmentInfos.read(directory);
            segmentInfos.clear();
          } catch (IOException e) {
            // Likely this means it's a fresh directory
          }
          segmentInfos.write(directory);
        } else {
          segmentInfos.read(directory);
        }

        // Create a deleter to keep track of which files can
        // be deleted:
        deleter = new IndexFileDeleter(segmentInfos, directory);
        deleter.setInfoStream(infoStream);
        deleter.findDeletableFiles();
        deleter.deleteFiles();

      } catch (IOException e) {
        this.writeLock.release();
        this.writeLock = null;
        throw e;
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
    int count = ramSegmentInfos.size();
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
    String segmentName = newRAMSegmentName();
    dw.addDocument(segmentName, doc);
    synchronized (this) {
      ramSegmentInfos.addElement(new SegmentInfo(segmentName, 1, ramDirectory, false));
      maybeFlushRamSegments();
    }
  }

  // for test purpose
  final synchronized int getRAMSegmentCount() {
    return ramSegmentInfos.size();
  }

  private final synchronized String newRAMSegmentName() {
    return "_ram_" + Integer.toString(ramSegmentInfos.counter++, Character.MAX_RADIX);
  }

  // for test purpose
  final synchronized int getSegmentCount(){
    return segmentInfos.size();
  }

  // for test purpose
  final synchronized int getDocCount(int i) {
    if (i >= 0 && i < segmentInfos.size()) {
      return segmentInfos.info(i).docCount;
    } else {
      return -1;
    }
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
      mergeSegments(segmentInfos, minSegment < 0 ? 0 : minSegment, segmentInfos.size());
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
          mergeSegments(segmentInfos, base, end);
      }
    }

    optimize();					  // final cleanup
  }

  /**
   * Merges all segments from an array of indexes into this index.
   * <p>
   * This is similar to addIndexes(Directory[]). However, no optimize()
   * is called either at the beginning or at the end. Instead, merges
   * are carried out as necessary.
   * <p>
   * This requires this index not be among those to be added, and the
   * upper bound* of those segment doc counts not exceed maxMergeDocs.
   */
  public synchronized void addIndexesNoOptimize(Directory[] dirs)
      throws IOException {
    // Adding indexes can be viewed as adding a sequence of segments S to
    // a sequence of segments T. Segments in T follow the invariants but
    // segments in S may not since they could come from multiple indexes.
    // Here is the merge algorithm for addIndexesNoOptimize():
    //
    // 1 Flush ram segments.
    // 2 Consider a combined sequence with segments from T followed
    //   by segments from S (same as current addIndexes(Directory[])).
    // 3 Assume the highest level for segments in S is h. Call
    //   maybeMergeSegments(), but instead of starting w/ lowerBound = -1
    //   and upperBound = maxBufferedDocs, start w/ lowerBound = -1 and
    //   upperBound = upperBound of level h. After this, the invariants
    //   are guaranteed except for the last < M segments whose levels <= h.
    // 4 If the invariants hold for the last < M segments whose levels <= h,
    //   if some of those < M segments are from S (not merged in step 3),
    //   properly copy them over*, otherwise done.
    //   Otherwise, simply merge those segments. If the merge results in
    //   a segment of level <= h, done. Otherwise, it's of level h+1 and call
    //   maybeMergeSegments() starting w/ upperBound = upperBound of level h+1.
    //
    // * Ideally, we want to simply copy a segment. However, directory does
    // not support copy yet. In addition, source may use compound file or not
    // and target may use compound file or not. So we use mergeSegments() to
    // copy a segment, which may cause doc count to change because deleted
    // docs are garbage collected.
    //
    // In current addIndexes(Directory[]), segment infos in S are added to
    // T's "segmentInfos" upfront. Then segments in S are merged to T several
    // at a time. Every merge is committed with T's "segmentInfos". So if
    // a reader is opened on T while addIndexes() is going on, it could see
    // an inconsistent index. AddIndexesNoOptimize() has a similar behaviour.

    // 1 flush ram segments
    flushRamSegments();

    // 2 copy segment infos and find the highest level from dirs
    int start = segmentInfos.size();
    int startUpperBound = minMergeDocs;

    try {
      for (int i = 0; i < dirs.length; i++) {
        if (directory == dirs[i]) {
          // cannot add this index: segments may be deleted in merge before added
          throw new IllegalArgumentException("Cannot add this index to itself");
        }

        SegmentInfos sis = new SegmentInfos(); // read infos from dir
        sis.read(dirs[i]);
        for (int j = 0; j < sis.size(); j++) {
          SegmentInfo info = sis.info(j);
          segmentInfos.addElement(info); // add each info

          while (startUpperBound < info.docCount) {
            startUpperBound *= mergeFactor; // find the highest level from dirs
            if (startUpperBound > maxMergeDocs) {
              // upper bound cannot exceed maxMergeDocs
              throw new IllegalArgumentException("Upper bound cannot exceed maxMergeDocs");
            }
          }
        }
      }
    } catch (IllegalArgumentException e) {
      for (int i = segmentInfos.size() - 1; i >= start; i--) {
        segmentInfos.remove(i);
      }
      throw e;
    }

    // 3 maybe merge segments starting from the highest level from dirs
    maybeMergeSegments(startUpperBound);

    // get the tail segments whose levels <= h
    int segmentCount = segmentInfos.size();
    int numTailSegments = 0;
    while (numTailSegments < segmentCount
        && startUpperBound >= segmentInfos.info(segmentCount - 1 - numTailSegments).docCount) {
      numTailSegments++;
    }
    if (numTailSegments == 0) {
      return;
    }

    // 4 make sure invariants hold for the tail segments whose levels <= h
    if (checkNonDecreasingLevels(segmentCount - numTailSegments)) {
      // identify the segments from S to be copied (not merged in 3)
      int numSegmentsToCopy = 0;
      while (numSegmentsToCopy < segmentCount
          && directory != segmentInfos.info(segmentCount - 1 - numSegmentsToCopy).dir) {
        numSegmentsToCopy++;
      }
      if (numSegmentsToCopy == 0) {
        return;
      }

      // copy those segments from S
      for (int i = segmentCount - numSegmentsToCopy; i < segmentCount; i++) {
        mergeSegments(segmentInfos, i, i + 1);
      }
      if (checkNonDecreasingLevels(segmentCount - numSegmentsToCopy)) {
        return;
      }
    }

    // invariants do not hold, simply merge those segments
    mergeSegments(segmentInfos, segmentCount - numTailSegments, segmentCount);

    // maybe merge segments again if necessary
    if (segmentInfos.info(segmentInfos.size() - 1).docCount > startUpperBound) {
      maybeMergeSegments(startUpperBound * mergeFactor);
    }
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
    SegmentInfo info = new SegmentInfo(mergedName, docCount, directory, false);
    segmentInfos.addElement(info);

    if(sReader != null)
        sReader.close();

    String segmentsInfosFileName = segmentInfos.getCurrentSegmentFileName();
    segmentInfos.write(directory);         // commit changes

    deleter.deleteFile(segmentsInfosFileName);    // delete old segments_N file
    deleter.deleteSegments(segmentsToDelete);     // delete now-unused segments

    if (useCompoundFile) {
      Vector filesToDelete = merger.createCompoundFile(mergedName + ".cfs");
      segmentsInfosFileName = segmentInfos.getCurrentSegmentFileName();
      info.setUseCompoundFile(true);
      segmentInfos.write(directory);     // commit again so readers know we've switched this segment to a compound file

      deleter.deleteFile(segmentsInfosFileName);  // delete old segments_N file
      deleter.deleteFiles(filesToDelete); // delete now unused files of segment 
    }
  }

  // Overview of merge policy:
  //
  // A flush is triggered either by close() or by the number of ram segments
  // reaching maxBufferedDocs. After a disk segment is created by the flush,
  // further merges may be triggered.
  //
  // LowerBound and upperBound set the limits on the doc count of a segment
  // which may be merged. Initially, lowerBound is set to 0 and upperBound
  // to maxBufferedDocs. Starting from the rightmost* segment whose doc count
  // > lowerBound and <= upperBound, count the number of consecutive segments
  // whose doc count <= upperBound.
  //
  // Case 1: number of worthy segments < mergeFactor, no merge, done.
  // Case 2: number of worthy segments == mergeFactor, merge these segments.
  //         If the doc count of the merged segment <= upperBound, done.
  //         Otherwise, set lowerBound to upperBound, and multiply upperBound
  //         by mergeFactor, go through the process again.
  // Case 3: number of worthy segments > mergeFactor (in the case mergeFactor
  //         M changes), merge the leftmost* M segments. If the doc count of
  //         the merged segment <= upperBound, consider the merged segment for
  //         further merges on this same level. Merge the now leftmost* M
  //         segments, and so on, until number of worthy segments < mergeFactor.
  //         If the doc count of all the merged segments <= upperBound, done.
  //         Otherwise, set lowerBound to upperBound, and multiply upperBound
  //         by mergeFactor, go through the process again.
  // Note that case 2 can be considerd as a special case of case 3.
  //
  // This merge policy guarantees two invariants if M does not change and
  // segment doc count is not reaching maxMergeDocs:
  // B for maxBufferedDocs, f(n) defined as ceil(log_M(ceil(n/B)))
  //      1: If i (left*) and i+1 (right*) are two consecutive segments of doc
  //         counts x and y, then f(x) >= f(y).
  //      2: The number of committed segments on the same level (f(n)) <= M.

  private final void maybeFlushRamSegments() throws IOException {
    if (ramSegmentInfos.size() >= minMergeDocs) {
      flushRamSegments();
    }
  }

  /** Expert:  Flushes all RAM-resident segments (buffered documents), then may merge segments. */
  public final synchronized void flushRamSegments() throws IOException {
    if (ramSegmentInfos.size() > 0) {
      mergeSegments(ramSegmentInfos, 0, ramSegmentInfos.size());
      maybeMergeSegments(minMergeDocs);
    }
  }

  /** Expert:  Return the total size of all index files currently cached in memory.
   * Useful for size management with flushRamDocs()
   */
  public final long ramSizeInBytes() {
    return ramDirectory.sizeInBytes();
  }

  /** Expert:  Return the number of documents whose segments are currently cached in memory.
   * Useful when calling flushRamSegments()
   */
  public final synchronized int numRamDocs() {
    return ramSegmentInfos.size();
  }
  
  /** Incremental segment merger.  */
  private final void maybeMergeSegments(int startUpperBound) throws IOException {
    long lowerBound = -1;
    long upperBound = startUpperBound;

    while (upperBound < maxMergeDocs) {
      int minSegment = segmentInfos.size();
      int maxSegment = -1;

      // find merge-worthy segments
      while (--minSegment >= 0) {
        SegmentInfo si = segmentInfos.info(minSegment);

        if (maxSegment == -1 && si.docCount > lowerBound && si.docCount <= upperBound) {
          // start from the rightmost* segment whose doc count is in bounds
          maxSegment = minSegment;
        } else if (si.docCount > upperBound) {
          // until the segment whose doc count exceeds upperBound
          break;
        }
      }

      minSegment++;
      maxSegment++;
      int numSegments = maxSegment - minSegment;

      if (numSegments < mergeFactor) {
        break;
      } else {
        boolean exceedsUpperLimit = false;

        // number of merge-worthy segments may exceed mergeFactor when
        // mergeFactor and/or maxBufferedDocs change(s)
        while (numSegments >= mergeFactor) {
          // merge the leftmost* mergeFactor segments
          int docCount = mergeSegments(segmentInfos, minSegment, minSegment + mergeFactor);
          numSegments -= mergeFactor;

          if (docCount > upperBound) {
            // continue to merge the rest of the worthy segments on this level
            minSegment++;
            exceedsUpperLimit = true;
          } else {
            // if the merged segment does not exceed upperBound, consider
            // this segment for further merges on this same level
            numSegments++;
          }
        }

        if (!exceedsUpperLimit) {
          // if none of the merged segments exceed upperBound, done
          break;
        }
      }

      lowerBound = upperBound;
      upperBound *= mergeFactor;
    }
  }

  /**
   * Merges the named range of segments, replacing them in the stack with a
   * single segment.
   */
  private final int mergeSegments(SegmentInfos sourceSegments, int minSegment, int end)
    throws IOException {

    final String mergedName = newSegmentName();
    if (infoStream != null) infoStream.print("merging segments");
    SegmentMerger merger = new SegmentMerger(this, mergedName);
    
    final Vector segmentsToDelete = new Vector();
    for (int i = minSegment; i < end; i++) {
      SegmentInfo si = sourceSegments.info(i);
      if (infoStream != null)
        infoStream.print(" " + si.name + " (" + si.docCount + " docs)");
      IndexReader reader = SegmentReader.get(si);
      merger.add(reader);
      if ((reader.directory() == this.directory) || // if we own the directory
          (reader.directory() == this.ramDirectory))
        segmentsToDelete.addElement(reader);   // queue segment for deletion
    }

    int mergedDocCount = merger.merge();

    if (infoStream != null) {
      infoStream.println(" into "+mergedName+" ("+mergedDocCount+" docs)");
    }

    SegmentInfo newSegment = new SegmentInfo(mergedName, mergedDocCount,
                                             directory, false);
    if (sourceSegments == ramSegmentInfos) {
      sourceSegments.removeAllElements();
      segmentInfos.addElement(newSegment);
    } else {
      for (int i = end-1; i > minSegment; i--)     // remove old infos & add new
        sourceSegments.remove(i);
      segmentInfos.set(minSegment, newSegment);
    }

    // close readers before we attempt to delete now-obsolete segments
    merger.closeReaders();

    String segmentsInfosFileName = segmentInfos.getCurrentSegmentFileName();
    segmentInfos.write(directory);     // commit before deleting

    deleter.deleteFile(segmentsInfosFileName);    // delete old segments_N file
    deleter.deleteSegments(segmentsToDelete);     // delete now-unused segments

    if (useCompoundFile) {
      Vector filesToDelete = merger.createCompoundFile(mergedName + ".cfs");

      segmentsInfosFileName = segmentInfos.getCurrentSegmentFileName();
      newSegment.setUseCompoundFile(true);
      segmentInfos.write(directory);     // commit again so readers know we've switched this segment to a compound file

      deleter.deleteFile(segmentsInfosFileName);  // delete old segments_N file
      deleter.deleteFiles(filesToDelete);  // delete now-unused segments
    }

    return mergedDocCount;
  }

  private final boolean checkNonDecreasingLevels(int start) {
    int lowerBound = -1;
    int upperBound = minMergeDocs;

    for (int i = segmentInfos.size() - 1; i >= start; i--) {
      int docCount = segmentInfos.info(i).docCount;
      if (docCount <= lowerBound) {
        return false;
      }

      while (docCount > upperBound) {
        lowerBound = upperBound;
        upperBound *= mergeFactor;
      }
    }
    return true;
  }
}
