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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.MutableBits;

// Used by IndexWriter to hold open SegmentReaders (for
// searching or merging), plus pending deletes,
// for a given segment
class ReadersAndLiveDocs {
  // Not final because we replace (clone) when we need to
  // change it and it's been shared:
  public final SegmentInfo info;

  // Tracks how many consumers are using this instance:
  private final AtomicInteger refCount = new AtomicInteger(1);

  private final IndexWriter writer;

  // Set once (null, and then maybe set, and never set again):
  private SegmentReader reader;

  // TODO: it's sometimes wasteful that we hold open two
  // separate SRs (one for merging one for
  // reading)... maybe just use a single SR?  The gains of
  // not loading the terms index (for merging in the
  // non-NRT case) are far less now... and if the app has
  // any deletes it'll open real readers anyway.

  // Set once (null, and then maybe set, and never set again):
  private SegmentReader mergeReader;

  // Holds the current shared (readable and writable
  // liveDocs).  This is null when there are no deleted
  // docs, and it's copy-on-write (cloned whenever we need
  // to change it but it's been shared to an external NRT
  // reader).
  private Bits liveDocs;

  // How many further deletions we've done against
  // liveDocs vs when we loaded it or last wrote it:
  private int pendingDeleteCount;

  // True if the current liveDocs is referenced by an
  // external NRT reader:
  private boolean shared;

  public ReadersAndLiveDocs(IndexWriter writer, SegmentInfo info) {
    this.info = info;
    this.writer = writer;
    shared = true;
  }

  public void incRef() {
    final int rc = refCount.incrementAndGet();
    assert rc > 1;
  }

  public void decRef() {
    final int rc = refCount.decrementAndGet();
    assert rc >= 0;
  }

  public int refCount() {
    final int rc = refCount.get();
    assert rc >= 0;
    return rc;
  }

  public synchronized int getPendingDeleteCount() {
    return pendingDeleteCount;
  }

  // Call only from assert!
  public synchronized boolean verifyDocCounts() {
    int count;
    if (liveDocs != null) {
      count = 0;
      for(int docID=0;docID<info.docCount;docID++) {
        if (liveDocs.get(docID)) {
          count++;
        }
      }
    } else {
      count = info.docCount;
    }

    assert info.docCount - info.getDelCount() - pendingDeleteCount == count: "info.docCount=" + info.docCount + " info.getDelCount()=" + info.getDelCount() + " pendingDeleteCount=" + pendingDeleteCount + " count=" + count;;
    return true;
  }

  // Get reader for searching/deleting
  public synchronized SegmentReader getReader(IOContext context) throws IOException {
    //System.out.println("  livedocs=" + rld.liveDocs);

    if (reader == null) {
      // We steal returned ref:
      reader = new SegmentReader(info, writer.getConfig().getReaderTermsIndexDivisor(), context);
      if (liveDocs == null) {
        liveDocs = reader.getLiveDocs();
      }
      //System.out.println("ADD seg=" + rld.info + " isMerge=" + isMerge + " " + readerMap.size() + " in pool");
      //System.out.println(Thread.currentThread().getName() + ": getReader seg=" + info.name);
    }

    // Ref for caller
    reader.incRef();
    return reader;
  }

  // Get reader for merging (does not load the terms
  // index):
  public synchronized SegmentReader getMergeReader(IOContext context) throws IOException {
    //System.out.println("  livedocs=" + rld.liveDocs);

    if (mergeReader == null) {

      if (reader != null) {
        // Just use the already opened non-merge reader
        // for merging.  In the NRT case this saves us
        // pointless double-open:
        //System.out.println("PROMOTE non-merge reader seg=" + rld.info);
        // Ref for us:
        reader.incRef();
        mergeReader = reader;
        //System.out.println(Thread.currentThread().getName() + ": getMergeReader share seg=" + info.name);
      } else {
        //System.out.println(Thread.currentThread().getName() + ": getMergeReader seg=" + info.name);
        // We steal returned ref:
        mergeReader = new SegmentReader(info, -1, context);
        if (liveDocs == null) {
          liveDocs = mergeReader.getLiveDocs();
        }
      }
    }

    // Ref for caller
    mergeReader.incRef();
    return mergeReader;
  }

  public synchronized void release(SegmentReader sr) throws IOException {
    assert info == sr.getSegmentInfo();
    sr.decRef();
  }

  public synchronized boolean delete(int docID) {
    assert liveDocs != null;
    assert Thread.holdsLock(writer);
    assert docID >= 0 && docID < liveDocs.length() : "out of bounds: docid=" + docID + " liveDocsLength=" + liveDocs.length() + " seg=" + info.name + " docCount=" + info.docCount;
    assert !shared;
    final boolean didDelete = liveDocs.get(docID);
    if (didDelete) {
      ((MutableBits) liveDocs).clear(docID);
      pendingDeleteCount++;
      //System.out.println("  new del seg=" + info + " docID=" + docID + " pendingDelCount=" + pendingDeleteCount + " totDelCount=" + (info.docCount-liveDocs.count()));
    }
    return didDelete;
  }

  // NOTE: removes callers ref
  public synchronized void dropReaders() throws IOException {
    if (reader != null) {
      //System.out.println("  pool.drop info=" + info + " rc=" + reader.getRefCount());
      reader.decRef();
      reader = null;
    }
    if (mergeReader != null) {
      //System.out.println("  pool.drop info=" + info + " merge rc=" + mergeReader.getRefCount());
      mergeReader.decRef();
      mergeReader = null;
    }
    decRef();
  }

  /**
   * Returns a ref to a clone.  NOTE: this clone is not
   * enrolled in the pool, so you should simply close()
   * it when you're done (ie, do not call release()).
   */
  public synchronized SegmentReader getReadOnlyClone(IOContext context) throws IOException {
    if (reader == null) {
      getReader(context).decRef();
      assert reader != null;
    }
    shared = true;
    if (liveDocs != null) {
      return new SegmentReader(reader.getSegmentInfo(), reader.core, liveDocs, info.docCount - info.getDelCount() - pendingDeleteCount);
    } else {
      assert reader.getLiveDocs() == liveDocs;
      reader.incRef();
      return reader;
    }
  }

  public synchronized void initWritableLiveDocs() throws IOException {
    assert Thread.holdsLock(writer);
    assert info.docCount > 0;
    //System.out.println("initWritableLivedocs seg=" + info + " liveDocs=" + liveDocs + " shared=" + shared);
    if (shared) {
      // Copy on write: this means we've cloned a
      // SegmentReader sharing the current liveDocs
      // instance; must now make a private clone so we can
      // change it:
      LiveDocsFormat liveDocsFormat = info.getCodec().liveDocsFormat();
      if (liveDocs == null) {
        //System.out.println("create BV seg=" + info);
        liveDocs = liveDocsFormat.newLiveDocs(info.docCount);
      } else {
        liveDocs = liveDocsFormat.newLiveDocs(liveDocs);
      }
      shared = false;
    } else {
      assert liveDocs != null;
    }
  }

  public synchronized Bits getLiveDocs() {
    assert Thread.holdsLock(writer);
    return liveDocs;
  }

  public synchronized Bits getReadOnlyLiveDocs() {
    //System.out.println("getROLiveDocs seg=" + info);
    assert Thread.holdsLock(writer);
    shared = true;
    //if (liveDocs != null) {
    //System.out.println("  liveCount=" + liveDocs.count());
    //}
    return liveDocs;
  }

  public synchronized void dropChanges() {
    // Discard (don't save) changes when we are dropping
    // the reader; this is used only on the sub-readers
    // after a successful merge.  If deletes had
    // accumulated on those sub-readers while the merge
    // is running, by now we have carried forward those
    // deletes onto the newly merged segment, so we can
    // discard them on the sub-readers:
    pendingDeleteCount = 0;
  }

  // Commit live docs to the directory (writes new
  // _X_N.del files); returns true if it wrote the file
  // and false if there were no new deletes to write:
  public synchronized boolean writeLiveDocs(Directory dir) throws IOException {
    //System.out.println("rld.writeLiveDocs seg=" + info + " pendingDelCount=" + pendingDeleteCount);
    if (pendingDeleteCount != 0) {
      // We have new deletes
      assert liveDocs.length() == info.docCount;

      // Save in case we need to rollback on failure:
      final SegmentInfo sav = info.clone();
      info.advanceDelGen();
      info.setDelCount(info.getDelCount() + pendingDeleteCount);

      // We can write directly to the actual name (vs to a
      // .tmp & renaming it) because the file is not live
      // until segments file is written:
      boolean success = false;
      try {
        info.getCodec().liveDocsFormat().writeLiveDocs((MutableBits)liveDocs, dir, info, IOContext.DEFAULT);
        success = true;
      } finally {
        if (!success) {
          info.reset(sav);
        }
      }
      pendingDeleteCount = 0;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return "ReadersAndLiveDocs(seg=" + info + " pendingDeleteCount=" + pendingDeleteCount + " shared=" + shared + ")";
  }
}
