package org.apache.lucene.index;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.IOException;
import java.io.File;
import java.io.PrintStream;
import java.util.Vector;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.InputStream;
import org.apache.lucene.store.OutputStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.Analyzer;

/**
  An IndexWriter creates and maintains an index.

  The third argument to the <a href="#IndexWriter"><b>constructor</b></a>
  determines whether a new index is created, or whether an existing index is
  opened for the addition of new documents.

  In either case, documents are added with the <a
  href="#addDocument"><b>addDocument</b></a> method.  When finished adding
  documents, <a href="#close"><b>close</b></a> should be called.

  If an index will not have more documents added for a while and optimal search
  performance is desired, then the <a href="#optimize"><b>optimize</b></a>
  method should be called before the index is closed.
  */

public final class IndexWriter {
  private Directory directory;			  // where this index resides
  private Analyzer analyzer;			  // how to analyze text

  private SegmentInfos segmentInfos = new SegmentInfos(); // the segments
  private final Directory ramDirectory = new RAMDirectory(); // for temp segs

  /** Constructs an IndexWriter for the index in <code>path</code>.  Text will
    be analyzed with <code>a</code>.  If <code>create</code> is true, then a
    new, empty index will be created in <code>d</code>, replacing the index
    already there, if any. */
  public IndexWriter(String path, Analyzer a, boolean create)
       throws IOException {
    this(FSDirectory.getDirectory(path, create), a, create);
  }

  /** Constructs an IndexWriter for the index in <code>path</code>.  Text will
    be analyzed with <code>a</code>.  If <code>create</code> is true, then a
    new, empty index will be created in <code>d</code>, replacing the index
    already there, if any. */
  public IndexWriter(File path, Analyzer a, boolean create)
       throws IOException {
    this(FSDirectory.getDirectory(path, create), a, create);
  }

  /** Constructs an IndexWriter for the index in <code>d</code>.  Text will be
    analyzed with <code>a</code>.  If <code>create</code> is true, then a new,
    empty index will be created in <code>d</code>, replacing the index already
    there, if any. */
  public IndexWriter(Directory d, Analyzer a, final boolean create)
       throws IOException {
    directory = d;
    analyzer = a;

    Lock writeLock = directory.makeLock("write.lock");
    if (!writeLock.obtain())			  // obtain write lock
      throw new IOException("Index locked for write: " + writeLock);

    synchronized (directory) {			  // in- & inter-process sync
      new Lock.With(directory.makeLock("commit.lock")) {
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

  /** Flushes all changes to an index, closes all associated files, and closes
    the directory that the index is stored in. */
  public final synchronized void close() throws IOException {
    flushRamSegments();
    ramDirectory.close();
    directory.makeLock("write.lock").release();  // release write lock
    directory.close();
  }

  /** Returns the number of documents currently in this index. */
  public final synchronized int docCount() {
    int count = 0;
    for (int i = 0; i < segmentInfos.size(); i++) {
      SegmentInfo si = segmentInfos.info(i);
      count += si.docCount;
    }
    return count;
  }

  /** The maximum number of terms that will be indexed for a single field in a
    document.  This limits the amount of memory required for indexing, so that
    collections with very large files will not crash the indexing process by
    running out of memory.

    <p>By default, no more than 10,000 terms will be indexed for a field. */
  public int maxFieldLength = 10000;

  /** Adds a document to this index.*/
  public final void addDocument(Document doc) throws IOException {
    DocumentWriter dw =
      new DocumentWriter(ramDirectory, analyzer, maxFieldLength);
    String segmentName = newSegmentName();
    dw.addDocument(segmentName, doc);
    synchronized (this) {
      segmentInfos.addElement(new SegmentInfo(segmentName, 1, ramDirectory));
      maybeMergeSegments();
    }
  }

  private final synchronized String newSegmentName() {
    return "_" + Integer.toString(segmentInfos.counter++, Character.MAX_RADIX);
  }

  /** Determines how often segment indexes are merged by addDocument().  With
   * smaller values, less RAM is used while indexing, and searches on
   * unoptimized indexes are faster, but indexing speed is slower.  With larger
   * values more RAM is used while indexing and searches on unoptimized indexes
   * are slower, but indexing is faster.  Thus larger values (> 10) are best
   * for batched index creation, and smaller values (< 10) for indexes that are
   * interactively maintained.
   *
   * <p>This must never be less than 2.  The default value is 10.*/
  public int mergeFactor = 10;

  /** Determines the largest number of documents ever merged by addDocument().
   * Small values (e.g., less than 10,000) are best for interactive indexing,
   * as this limits the length of pauses while indexing to a few seconds.
   * Larger values are best for batched indexing and speedier searches.
   *
   * <p>The default value is {@link Integer#MAX_VALUE}. */
  public int maxMergeDocs = Integer.MAX_VALUE;

  /** If non-null, information about merges will be printed to this. */
  public PrintStream infoStream = null;

  /** Merges all segments together into a single segment, optimizing an index
      for search. */
  public final synchronized void optimize() throws IOException {
    flushRamSegments();
    while (segmentInfos.size() > 1 ||
	   (segmentInfos.size() == 1 &&
	    (SegmentReader.hasDeletions(segmentInfos.info(0)) ||
             segmentInfos.info(0).dir != directory))) {
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
  public final synchronized void addIndexes(Directory[] dirs)
      throws IOException {
    optimize();					  // start with zero or 1 seg
    int minSegment = segmentInfos.size();
    int segmentsAddedSinceMerge = 0;
    for (int i = 0; i < dirs.length; i++) {
      SegmentInfos sis = new SegmentInfos();	  // read infos from dir
      sis.read(dirs[i]);
      for (int j = 0; j < sis.size(); j++) {
	segmentInfos.addElement(sis.info(j));	  // add each info

	// merge whenever mergeFactor segments have been added
	if (++segmentsAddedSinceMerge == mergeFactor) {
	  mergeSegments(minSegment++, false);
	  segmentsAddedSinceMerge = 0;
	}
      }
    }
    optimize();					  // final cleanup
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
    long targetMergeDocs = mergeFactor;
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
  private final void mergeSegments(int minSegment) throws IOException {
    mergeSegments(minSegment, true);
  }

  /** Pops segments off of segmentInfos stack down to minSegment, merges them,
    and pushes the merged index onto the top of the segmentInfos stack. */
  private final void mergeSegments(int minSegment, boolean delete)
      throws IOException {
    String mergedName = newSegmentName();
    int mergedDocCount = 0;
    if (infoStream != null) infoStream.print("merging segments");
    SegmentMerger merger = new SegmentMerger(directory, mergedName);
    final Vector segmentsToDelete = new Vector();
    for (int i = minSegment; i < segmentInfos.size(); i++) {
      SegmentInfo si = segmentInfos.info(i);
      if (infoStream != null)
	infoStream.print(" " + si.name + " (" + si.docCount + " docs)");
      SegmentReader reader = new SegmentReader(si);
      merger.add(reader);
      if (delete)
	segmentsToDelete.addElement(reader);	  // queue for deletion
      mergedDocCount += si.docCount;
    }
    if (infoStream != null) {
      infoStream.println();
      infoStream.println(" into "+mergedName+" ("+mergedDocCount+" docs)");
    }
    merger.merge();

    segmentInfos.setSize(minSegment);		  // pop old infos & add new
    segmentInfos.addElement(new SegmentInfo(mergedName, mergedDocCount,
					    directory));
    
    synchronized (directory) {			  // in- & inter-process sync
      new Lock.With(directory.makeLock("commit.lock")) {
	  public Object doBody() throws IOException {
	    segmentInfos.write(directory);	  // commit before deleting
	    deleteSegments(segmentsToDelete);	  // delete now-unused segments
	    return null;
	  }
	}.run();
    }
  }

  /* Some operating systems (e.g. Windows) don't permit a file to be deleted
     while it is opened for read (e.g. by another process or thread).  So we
     assume that when a delete fails it is because the file is open in another
     process, and queue the file for subsequent deletion. */

  private final void deleteSegments(Vector segments) throws IOException {
    Vector deletable = new Vector();

    deleteFiles(readDeleteableFiles(), deletable); // try to delete deleteable
    
    for (int i = 0; i < segments.size(); i++) {
      SegmentReader reader = (SegmentReader)segments.elementAt(i);
      if (reader.directory == this.directory)
	deleteFiles(reader.files(), deletable);	  // try to delete our files
      else
	deleteFiles(reader.files(), reader.directory); // delete, eg, RAM files
    }

    writeDeleteableFiles(deletable);		  // note files we can't delete
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
	    infoStream.println(e.getMessage() + "; Will re-try later.");
	  deletable.addElement(file);		  // add to deletable
	}
      }
    }
  }

  private final Vector readDeleteableFiles() throws IOException {
    Vector result = new Vector();
    if (!directory.fileExists("deletable"))
      return result;

    InputStream input = directory.openFile("deletable");
    try {
      for (int i = input.readInt(); i > 0; i--)	  // read file names
	result.addElement(input.readString());
    } finally {
      input.close();
    }
    return result;
  }

  private final void writeDeleteableFiles(Vector files) throws IOException {
    OutputStream output = directory.createFile("deleteable.new");
    try {
      output.writeInt(files.size());
      for (int i = 0; i < files.size(); i++)
	output.writeString((String)files.elementAt(i));
    } finally {
      output.close();
    }
    directory.renameFile("deleteable.new", "deletable");
  }
}
