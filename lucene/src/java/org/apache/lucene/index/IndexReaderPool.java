package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;

/** Holds shared SegmentReader instances. IndexWriter uses
 *  SegmentReaders for 1) applying deletes, 2) doing
 *  merges, 3) handing out a real-time reader.  This pool
 *  reuses instances of the SegmentReaders in all these
 *  places if it is in "near real-time mode" (getReader()
 *  has been called on this instance). */
public class IndexReaderPool {

  private final Map<SegmentInfo,SegmentReader> readerMap = new HashMap<SegmentInfo,SegmentReader>();

  private final Directory directory;
  private final IndexWriterConfig config;
  private final IndexWriter writer;
  
  public IndexReaderPool(IndexWriter writer, Directory directory, IndexWriterConfig config) {
    this.directory = directory;
    this.config = config;
    this.writer = writer;
  }
  
  /** Forcefully clear changes for the specified segments,
   *  and remove from the pool.   This is called on successful merge. */
  synchronized void clear(SegmentInfos infos) throws IOException {
    if (infos == null) {
      for (Map.Entry<SegmentInfo,SegmentReader> ent: readerMap.entrySet()) {
        ent.getValue().hasChanges = false;
      }
    } else {
      for (final SegmentInfo info: infos) {
        if (readerMap.containsKey(info)) {
          readerMap.get(info).hasChanges = false;
        }
      }     
    }
  }
  
  /**
   * Release the segment reader (i.e. decRef it and close if there
   * are no more references.
   * @param sr
   * @throws IOException
   */
  public synchronized void release(SegmentReader sr) throws IOException {
    release(sr, false);
  }
  
  /**
   * Release the segment reader (i.e. decRef it and close if there
   * are no more references.
   * @param sr
   * @throws IOException
   */
  public synchronized void release(SegmentReader sr, boolean drop) throws IOException {

    final boolean pooled = readerMap.containsKey(sr.getSegmentInfo());

    assert !pooled | readerMap.get(sr.getSegmentInfo()) == sr;

    // Drop caller's ref; for an external reader (not
    // pooled), this decRef will close it
    sr.decRef();

    if (pooled && (drop || (!writer.poolReaders && sr.getRefCount() == 1))) {

      // We are the last ref to this reader; since we're
      // not pooling readers, we release it:
      readerMap.remove(sr.getSegmentInfo());

      // nocommit
      //assert !sr.hasChanges || Thread.holdsLock(IndexWriter.this);

      // Drop our ref -- this will commit any pending
      // changes to the dir
      boolean success = false;
      try {
        sr.close();
        success = true;
      } finally {
        if (!success && sr.hasChanges) {
          // Abandon the changes & retry closing:
          sr.hasChanges = false;
          try {
            sr.close();
          } catch (Throwable ignore) {
            // Keep throwing original exception
          }
        }
      }
    }
  }
  
  /** Remove all our references to readers, and commits
   *  any pending changes. */
  synchronized void close() throws IOException {
    Iterator<Map.Entry<SegmentInfo,SegmentReader>> iter = readerMap.entrySet().iterator();
    while (iter.hasNext()) {
      
      Map.Entry<SegmentInfo,SegmentReader> ent = iter.next();

      SegmentReader sr = ent.getValue();
      if (sr.hasChanges) {
        assert writer.infoIsLive(sr.getSegmentInfo());
        sr.startCommit();
        boolean success = false;
        try {
          sr.doCommit(null);
          success = true;
        } finally {
          if (!success) {
            sr.rollbackCommit();
          }
        }
      }

      iter.remove();

      // NOTE: it is allowed that this decRef does not
      // actually close the SR; this can happen when a
      // near real-time reader is kept open after the
      // IndexWriter instance is closed
      sr.decRef();
    }
  }
  
  /**
   * Commit all segment reader in the pool.
   * @throws IOException
   */
  synchronized void commit() throws IOException {
    for (Map.Entry<SegmentInfo,SegmentReader> ent : readerMap.entrySet()) {

      SegmentReader sr = ent.getValue();
      if (sr.hasChanges) {
        assert writer.infoIsLive(sr.getSegmentInfo());
        sr.startCommit();
        boolean success = false;
        try {
          sr.doCommit(null);
          success = true;
        } finally {
          if (!success) {
            sr.rollbackCommit();
          }
        }
      }
    }
  }
  
  /**
   * Returns a ref to a clone.  NOTE: this clone is not
   * enrolled in the pool, so you should simply close()
   * it when you're done (ie, do not call release()).
   */
  public synchronized SegmentReader getReadOnlyClone(SegmentInfo info, boolean doOpenStores, int termInfosIndexDivisor) throws IOException {
    SegmentReader sr = get(info, doOpenStores, BufferedIndexInput.BUFFER_SIZE, termInfosIndexDivisor);
    try {
      return (SegmentReader) sr.clone(true);
    } finally {
      sr.decRef();
    }
  }
 
  /**
   * Obtain a SegmentReader from the readerPool.  The reader
   * must be returned by calling {@link #release(SegmentReader)}
   * @see #release(SegmentReader)
   * @param info
   * @param doOpenStores
   * @throws IOException
   */
  public synchronized SegmentReader get(SegmentInfo info, boolean doOpenStores) throws IOException {
    return get(info, doOpenStores, BufferedIndexInput.BUFFER_SIZE, config.getReaderTermsIndexDivisor());
  }

  /**
   * Obtain a SegmentReader from the readerPool.  The reader
   * must be returned by calling {@link #release(SegmentReader)}
   * 
   * @see #release(SegmentReader)
   * @param info
   * @param doOpenStores
   * @param readBufferSize
   * @param termsIndexDivisor
   * @throws IOException
   */
  public synchronized SegmentReader get(SegmentInfo info, boolean doOpenStores, int readBufferSize, int termsIndexDivisor) throws IOException {

    if (writer.poolReaders) {
      readBufferSize = BufferedIndexInput.BUFFER_SIZE;
    }

    SegmentReader sr = readerMap.get(info);
    if (sr == null) {
      // TODO: we may want to avoid doing this while
      // synchronized
      // Returns a ref, which we xfer to readerMap:
      sr = SegmentReader.get(false, info.dir, info, readBufferSize, doOpenStores, termsIndexDivisor, config.getCodecProvider());

      if (info.dir == directory) {
        // Only pool if reader is not external
        readerMap.put(info, sr);
      }
    } else {
      if (doOpenStores) {
        sr.openDocStores();
      }
      if (termsIndexDivisor != -1) {
        // If this reader was originally opened because we
        // needed to merge it, we didn't load the terms
        // index.  But now, if the caller wants the terms
        // index (eg because it's doing deletes, or an NRT
        // reader is being opened) we ask the reader to
        // load its terms index.
        sr.loadTermsIndex(termsIndexDivisor);
      }
    }

    // Return a ref to our caller
    if (info.dir == directory) {
      // Only incRef if we pooled (reader is not external)
      sr.incRef();
    }
    return sr;
  }

  // Returns a ref
  public synchronized SegmentReader getIfExists(SegmentInfo info) throws IOException {
    SegmentReader sr = readerMap.get(info);
    if (sr != null) {
      sr.incRef();
    }
    return sr;
  }
}


