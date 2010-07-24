package org.apache.lucene.index;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

final class DocumentsWriter {
  private long sequenceID;
  private int numDocumentsWriterPerThreads;

  private final BufferedDeletesInRAM deletesInRAM = new BufferedDeletesInRAM();
  private final DocumentsWriterThreadPool threadPool;
  private final Lock sequenceIDLock = new ReentrantLock();

  private final Directory directory;
  final IndexWriter indexWriter;
  final IndexWriterConfig config;

  private int maxBufferedDocs;
  private double maxBufferSizeMB;
  private int maxBufferedDeleteTerms;

  private boolean closed;
  private AtomicInteger numDocsInRAM = new AtomicInteger(0);
  private AtomicLong ramUsed = new AtomicLong(0);

  private long flushedSequenceID = -1;
  private final PrintStream infoStream;

  private Map<DocumentsWriterPerThread, Long> minSequenceIDsPerThread = new HashMap<DocumentsWriterPerThread, Long>();

  public DocumentsWriter(Directory directory, IndexWriter indexWriter, IndexWriterConfig config) {
    this.directory = directory;
    this.indexWriter = indexWriter;
    this.config = config;
    this.maxBufferedDocs = config.getMaxBufferedDocs();
    this.threadPool = config.getIndexerThreadPool();
    this.infoStream = indexWriter.getInfoStream();
  }

  public int getMaxBufferedDocs() {
    return maxBufferedDocs;
  }

  public void setMaxBufferedDocs(int max) {
    this.maxBufferedDocs = max;
  }

  public double getRAMBufferSizeMB() {
    return maxBufferSizeMB;
  }

  public void setRAMBufferSizeMB(double mb) {
    this.maxBufferSizeMB = mb;
  }

  public int getMaxBufferedDeleteTerms() {
    return maxBufferedDeleteTerms;
  }

  public void setMaxBufferedDeleteTerms(int max) {
    this.maxBufferedDeleteTerms = max;
  }

  private final long nextSequenceID() {
    return sequenceID++;
  }
  
  boolean anyChanges() {
    return numDocsInRAM.get() != 0 ||
      deletesInRAM.hasDeletes();
  }

  DocumentsWriterPerThread newDocumentsWriterPerThread() {
    DocumentsWriterPerThread perThread = new DocumentsWriterPerThread(directory, this, config
        .getIndexingChain());
    sequenceIDLock.lock();
    try {
      numDocumentsWriterPerThreads++;
      return perThread;
    } finally {
      sequenceIDLock.unlock();
    }
  }

  long addDocument(final Document doc, final Analyzer analyzer)
      throws CorruptIndexException, IOException {
    return updateDocument(null, doc, analyzer);
  }

  long updateDocument(final Term delTerm, final Document doc, final Analyzer analyzer)
      throws CorruptIndexException, IOException {

    long seqID = threadPool.executePerThread(this, doc,
        new DocumentsWriterThreadPool.PerThreadTask<Long>() {
          @Override
          public Long process(final DocumentsWriterPerThread perThread) throws IOException {
            long perThreadRAMUsedBeforeAdd = perThread.numBytesUsed;
            perThread.addDocument(doc, analyzer);

            final long sequenceID;
            sequenceIDLock.lock();
            try {
              ensureOpen();
              sequenceID = nextSequenceID();
              if (delTerm != null) {
                deletesInRAM.addDeleteTerm(delTerm, sequenceID, numDocumentsWriterPerThreads);
              }
              perThread.commitDocument(sequenceID);
              if (!minSequenceIDsPerThread.containsKey(perThread)) {
                minSequenceIDsPerThread.put(perThread, sequenceID);
              }
              numDocsInRAM.incrementAndGet();
            } finally {
              sequenceIDLock.unlock();
            }

            if (finishAddDocument(perThread, perThreadRAMUsedBeforeAdd)) {
              super.clearThreadBindings();
            }
            return sequenceID;
          }
        });
    
    indexWriter.maybeMerge();
    
    return seqID;
  }

  private final boolean finishAddDocument(DocumentsWriterPerThread perThread,
      long perThreadRAMUsedBeforeAdd) throws IOException {
    int numDocsPerThread = perThread.getNumDocsInRAM();
    boolean flushed = maybeFlushPerThread(perThread);
    if (flushed) {
      int oldValue = numDocsInRAM.get();
      while (!numDocsInRAM.compareAndSet(oldValue, oldValue - numDocsPerThread)) {
        oldValue = numDocsInRAM.get();
      }

      sequenceIDLock.lock();
      try {
        minSequenceIDsPerThread.remove(perThread);
        updateFlushedSequenceID();
      } finally {
        sequenceIDLock.unlock();
      }
    }

    long deltaRAM = perThread.numBytesUsed - perThreadRAMUsedBeforeAdd;
    long oldValue = ramUsed.get();
    while (!ramUsed.compareAndSet(oldValue, oldValue + deltaRAM)) {
      oldValue = ramUsed.get();
    }

    return flushed;
  }

  long bufferDeleteTerms(final Term[] terms) throws IOException {
    sequenceIDLock.lock();
    try {
      ensureOpen();
      final long sequenceID = nextSequenceID();
      deletesInRAM.addDeleteTerms(terms, sequenceID, numDocumentsWriterPerThreads);
      return sequenceID;
    } finally {
      sequenceIDLock.unlock();
    }
  }

  long bufferDeleteTerm(final Term term) throws IOException {
    sequenceIDLock.lock();
    try {
      ensureOpen();
      final long sequenceID = nextSequenceID();
      deletesInRAM.addDeleteTerm(term, sequenceID, numDocumentsWriterPerThreads);
      return sequenceID;
    } finally {
      sequenceIDLock.unlock();
    }
  }

  long bufferDeleteQueries(final Query[] queries) throws IOException {
    sequenceIDLock.lock();
    try {
      ensureOpen();
      final long sequenceID = nextSequenceID();
      for (Query q : queries) {
        deletesInRAM.addDeleteQuery(q, sequenceID, numDocumentsWriterPerThreads);
      }
      return sequenceID;
    } finally {
      sequenceIDLock.unlock();
    }
  }

  long bufferDeleteQuery(final Query query) throws IOException {
    sequenceIDLock.lock();
    try {
      ensureOpen();
      final long sequenceID = nextSequenceID();
      deletesInRAM.addDeleteQuery(query, sequenceID, numDocumentsWriterPerThreads);
      return sequenceID;
    } finally {
      sequenceIDLock.unlock();
    }
  }

  private final void updateFlushedSequenceID() {
    long newFlushedID = Long.MAX_VALUE;
    for (long minSeqIDPerThread : minSequenceIDsPerThread.values()) {
      if (minSeqIDPerThread < newFlushedID) {
        newFlushedID = minSeqIDPerThread;
      }
    }

    this.flushedSequenceID = newFlushedID;
  }

  final boolean flushAllThreads(final boolean flushDeletes)
      throws IOException {
    return threadPool.executeAllThreads(new DocumentsWriterThreadPool.AllThreadsTask<Boolean>() {
      @Override
      public Boolean process(Iterator<DocumentsWriterPerThread> threadsIterator) throws IOException {
        boolean anythingFlushed = false;
        
        if (flushDeletes) {
          if (applyDeletes(indexWriter.segmentInfos)) {
            indexWriter.checkpoint();
          }
        }

        while (threadsIterator.hasNext()) {
          DocumentsWriterPerThread perThread = threadsIterator.next();
          final int numDocs = perThread.getNumDocsInRAM();
          
          // Always flush docs if there are any
          boolean flushDocs = numDocs > 0;
          
          String segment = perThread.getSegment();

          // If we are flushing docs, segment must not be null:
          assert segment != null || !flushDocs;
    
          if (flushDocs) {
            SegmentInfo newSegment = perThread.flush();
            
            if (newSegment != null) {
              anythingFlushed = true;
              
              IndexWriter.setDiagnostics(newSegment, "flush");
              finishFlushedSegment(newSegment, perThread);
            }
          }
        }

        if (anythingFlushed) {
          clearThreadBindings();

          sequenceIDLock.lock();
          try {
            flushedSequenceID = sequenceID;
          } finally {
            sequenceIDLock.unlock();
          }
          numDocsInRAM.set(0);
        }
        
        if (flushDeletes) {
          deletesInRAM.clear();
        }


        return anythingFlushed;
      }
    });
  }

  /** Build compound file for the segment we just flushed */
  void createCompoundFile(String segment, DocumentsWriterPerThread perThread) throws IOException {
    
    CompoundFileWriter cfsWriter = new CompoundFileWriter(directory, 
        IndexFileNames.segmentFileName(segment, "", IndexFileNames.COMPOUND_FILE_EXTENSION));
    for(String fileName : perThread.flushState.flushedFiles) {
      cfsWriter.addFile(fileName);
    }
      
    // Perform the merge
    cfsWriter.close();
  }

  // nocommit
  void finishFlushedSegment(SegmentInfo newSegment, DocumentsWriterPerThread perThread) throws IOException {
    synchronized(indexWriter) {
      indexWriter.segmentInfos.add(newSegment);
      indexWriter.checkpoint();
    
      SegmentReader reader = indexWriter.readerPool.get(newSegment, false);
      boolean any = false;
      try {
        any = applyDeletes(reader, newSegment.getMinSequenceID(), newSegment.getMaxSequenceID(), perThread.sequenceIDs);
      } finally {
        indexWriter.readerPool.release(reader);
      }
      if (any) {
        indexWriter.checkpoint();
      }
  
      if (indexWriter.mergePolicy.useCompoundFile(indexWriter.segmentInfos, newSegment)) {
        // Now build compound file
        boolean success = false;
        try {
          createCompoundFile(newSegment.name, perThread);
          success = true;
        } finally {
          if (!success) {
            if (infoStream != null) {
              message("hit exception " +
              		"reating compound file for newly flushed segment " + newSegment.name);
            }
            indexWriter.deleter.deleteFile(IndexFileNames.segmentFileName(newSegment.name, "", 
                IndexFileNames.COMPOUND_FILE_EXTENSION));
          }
        }
  
        synchronized(indexWriter) {
          newSegment.setUseCompoundFile(true);
          indexWriter.checkpoint();
          // In case the files we just merged into a CFS were
          // not previously checkpointed:
          indexWriter.deleter.deleteNewFiles(perThread.closedFiles());
        }
      }
    }
  }
  
  // Returns true if an abort is in progress
  void pauseAllThreads() {
    threadPool.pauseAllThreads();
  }

  void resumeAllThreads() {
    threadPool.resumeAllThreads();
  }

  void close() {
    sequenceIDLock.lock();
    try {
      closed = true;
    } finally {
      sequenceIDLock.unlock();
    }
  }

  private void ensureOpen() throws AlreadyClosedException {
    if (closed) {
      throw new AlreadyClosedException("this IndexWriter is closed");
    }
  }

  private final boolean maybeFlushPerThread(DocumentsWriterPerThread perThread) throws IOException {
    if (perThread.getNumDocsInRAM() == maxBufferedDocs) {
      flushSegment(perThread);
      assert perThread.getNumDocsInRAM() == 0;
      return true;
    }

    return false;
  }

  private boolean flushSegment(DocumentsWriterPerThread perThread)
      throws IOException {
    if (perThread.getNumDocsInRAM() == 0) {
      return false;
    }

    SegmentInfo newSegment = perThread.flush();
    
    if (newSegment != null) {
      finishFlushedSegment(newSegment, perThread);
      return true;
    }
    return false;
  }

  void abort() throws IOException {
    threadPool.abort();
    try {
      try {
        abortedFiles = openFiles();
      } catch (Throwable t) {
        abortedFiles = null;
      }
  
      deletesInRAM.clear();
      // nocommit
  //        deletesFlushed.clear();
  
      openFiles.clear();
    } finally {
      threadPool.finishAbort();
    }

  }

  final List<String> openFiles = new ArrayList<String>();
  private Collection<String> abortedFiles; // List of files that were written before last abort()

  /*
   * Returns Collection of files in use by this instance,
   * including any flushed segments.
   */
  @SuppressWarnings("unchecked")
  List<String> openFiles() {
    synchronized(openFiles) {
      return (List<String>) ((ArrayList<String>) openFiles).clone();
    }
  }

  
  Collection<String> abortedFiles() {
    return abortedFiles;
  }

  boolean hasDeletes() {
    return deletesInRAM.hasDeletes();
  }

  // nocommit
  int getNumDocsInRAM() {
    return numDocsInRAM.get();
  }

  // nocommit
  long getRAMUsed() {
    return ramUsed.get();
  }

  // nocommit
  // long getRAMUsed() {
  // return numBytesUsed + deletesInRAM.bytesUsed + deletesFlushed.bytesUsed;
  // }

  boolean applyDeletes(SegmentInfos infos) throws IOException {
    synchronized(indexWriter) {
      if (!hasDeletes())
        return false;
  
      final long t0 = System.currentTimeMillis();
  
      if (infoStream != null) {
        message("apply " + deletesInRAM.getNumDeletes() + " buffered deletes on " +
                +infos.size() + " segments.");
      }
  
      final int infosEnd = infos.size();
  
      boolean any = false;
      for (int i = 0; i < infosEnd; i++) {
  
        // Make sure we never attempt to apply deletes to
        // segment in external dir
        assert infos.info(i).dir == directory;
  
        SegmentInfo si = infos.info(i);
        SegmentReader reader = indexWriter.readerPool.get(si, false);
        try {
          any |= applyDeletes(reader, si.getMinSequenceID(), si.getMaxSequenceID(), null);
        } finally {
          indexWriter.readerPool.release(reader);
        }
      }
  
      if (infoStream != null) {
        message("apply deletes took " + (System.currentTimeMillis() - t0) + " msec");
      }
  
      return any;
    }
  }

  // Apply buffered delete terms, queries and docIDs to the
  // provided reader
  final boolean applyDeletes(IndexReader reader, long minSequenceID, long maxSequenceID, long[] sequenceIDs)
      throws CorruptIndexException, IOException {

    assert sequenceIDs == null || sequenceIDs.length >= reader.maxDoc() : "reader.maxDoc="
        + reader.maxDoc() + ",sequenceIDs.length=" + sequenceIDs.length;

    boolean any = false;

    // first: delete the documents that had non-aborting exceptions
    if (sequenceIDs != null) {
      for (int i = 0; i < reader.maxDoc(); i++) {
        if (sequenceIDs[i] == -1) {
          reader.deleteDocument(i);
          any = true;
        }
      }
    }
    
    if (deletesInRAM.hasDeletes()) {
      IndexSearcher searcher = new IndexSearcher(reader);

      SortedMap<Long, BufferedDeletesInRAM.Delete> deletes = deletesInRAM.deletes.getReadCopy();
      
      SortedMap<Term, Long> deleteTerms = new TreeMap<Term, Long>();
      for (Entry<Long, BufferedDeletesInRAM.Delete> entry : deletes.entrySet()) {
        if (minSequenceID < entry.getKey()) {
          BufferedDeletesInRAM.Delete delete = entry.getValue();
          if (delete instanceof BufferedDeletesInRAM.DeleteTerm) {
            BufferedDeletesInRAM.DeleteTerm deleteTerm = (BufferedDeletesInRAM.DeleteTerm) delete;
            deleteTerms.put(deleteTerm.term, entry.getKey());
          } else if (delete instanceof BufferedDeletesInRAM.DeleteTerms) {
            BufferedDeletesInRAM.DeleteTerms terms = (BufferedDeletesInRAM.DeleteTerms) delete;
            for (Term t : terms.terms) {
              deleteTerms.put(t, entry.getKey());
            }
          } else {
            // delete query
            BufferedDeletesInRAM.DeleteQuery deleteQuery = (BufferedDeletesInRAM.DeleteQuery) delete;
            Query query = deleteQuery.query;
            Weight weight = query.weight(searcher);
            Scorer scorer = weight.scorer(reader, true, false);
            if (scorer != null) {
              while (true) {
                int doc = scorer.nextDoc();
                if (doc == DocsEnum.NO_MORE_DOCS) {
                  break;
                }
                if ( (sequenceIDs != null && sequenceIDs[doc] < entry.getKey())
                    || (sequenceIDs == null && maxSequenceID < entry.getKey())) {
                  reader.deleteDocument(doc);
                  any = true;
                }
              }
            }
          }
        }
      }

      // Delete by term
      if (deleteTerms.size() > 0) {
        Fields fields = reader.fields();
        if (fields == null) {
          // This reader has no postings
          return false;
        }

        TermsEnum termsEnum = null;

        String currentField = null;
        BytesRef termRef = new BytesRef();
        DocsEnum docs = null;

        for (Entry<Term, Long> entry : deleteTerms.entrySet()) {
          Term term = entry.getKey();
          // Since we visit terms sorted, we gain performance
          // by re-using the same TermsEnum and seeking only
          // forwards
          if (term.field() != currentField) {
            assert currentField == null || currentField.compareTo(term.field()) < 0;
            currentField = term.field();
            Terms terms = fields.terms(currentField);
            if (terms != null) {
              termsEnum = terms.iterator();
            } else {
              termsEnum = null;
            }
          }

          if (termsEnum == null) {
            continue;
          }
          // assert checkDeleteTerm(term);

          termRef.copy(term.text());

          if (termsEnum.seek(termRef, false) == TermsEnum.SeekStatus.FOUND) {
            DocsEnum docsEnum = termsEnum.docs(reader.getDeletedDocs(), docs);

            if (docsEnum != null) {
              docs = docsEnum;
              // int limit = entry.getValue().getNum();
              while (true) {
                final int doc = docs.nextDoc();
                // if (docID == DocsEnum.NO_MORE_DOCS || docIDStart+docID >= limit) {
                if (doc == DocsEnum.NO_MORE_DOCS) {
                  break;
                }
                if ( (sequenceIDs != null && sequenceIDs[doc] < entry.getValue())
                    || (sequenceIDs == null && maxSequenceID < entry.getValue())) {
                  reader.deleteDocument(doc);
                  any = true;
                }
              }
            }
          }
        }
      }
    }

    return any;
  }

  void message(String message) {
    if (infoStream != null) {
      indexWriter.message("DW: " + message);
    }
  }

}
