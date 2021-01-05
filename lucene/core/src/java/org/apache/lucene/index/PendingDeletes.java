/*
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

package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;

/** This class handles accounting and applying pending deletes for live segment readers */
class PendingDeletes {
  protected final SegmentCommitInfo info;
  // Read-only live docs, null until live docs are initialized or if all docs are alive
  private Bits liveDocs;
  // Writeable live docs, null if this instance is not ready to accept writes, in which
  // case getMutableBits needs to be called
  private FixedBitSet writeableLiveDocs;
  protected int pendingDeleteCount;
  boolean liveDocsInitialized;

  PendingDeletes(SegmentReader reader, SegmentCommitInfo info) {
    this(info, reader.getLiveDocs(), true);
    pendingDeleteCount = reader.numDeletedDocs() - info.getDelCount();
  }

  PendingDeletes(SegmentCommitInfo info) {
    this(info, null, info.hasDeletions() == false);
    // if we don't have deletions we can mark it as initialized since we might receive deletes on a
    // segment
    // without having a reader opened on it ie. after a merge when we apply the deletes that IW
    // received while merging.
    // For segments that were published we enforce a reader in the
    // BufferedUpdatesStream.SegmentState ctor
  }

  PendingDeletes(SegmentCommitInfo info, Bits liveDocs, boolean liveDocsInitialized) {
    this.info = info;
    this.liveDocs = liveDocs;
    pendingDeleteCount = 0;
    this.liveDocsInitialized = liveDocsInitialized;
  }

  protected FixedBitSet getMutableBits() {
    // if we pull mutable bits but we haven't been initialized something is completely off.
    // this means we receive deletes without having the bitset that is on-disk ready to be cloned
    assert liveDocsInitialized : "can't delete if liveDocs are not initialized";
    if (writeableLiveDocs == null) {
      // Copy on write: this means we've cloned a
      // SegmentReader sharing the current liveDocs
      // instance; must now make a private clone so we can
      // change it:
      if (liveDocs != null) {
        writeableLiveDocs = FixedBitSet.copyOf(liveDocs);
      } else {
        writeableLiveDocs = new FixedBitSet(info.info.maxDoc());
        writeableLiveDocs.set(0, info.info.maxDoc());
      }
      liveDocs = writeableLiveDocs.asReadOnlyBits();
    }
    return writeableLiveDocs;
  }

  /**
   * Marks a document as deleted in this segment and return true if a document got actually deleted
   * or if the document was already deleted.
   */
  boolean delete(int docID) throws IOException {
    assert info.info.maxDoc() > 0;
    FixedBitSet mutableBits = getMutableBits();
    assert mutableBits != null;
    assert docID >= 0 && docID < mutableBits.length()
        : "out of bounds: docid="
            + docID
            + " liveDocsLength="
            + mutableBits.length()
            + " seg="
            + info.info.name
            + " maxDoc="
            + info.info.maxDoc();
    final boolean didDelete = mutableBits.get(docID);
    if (didDelete) {
      mutableBits.clear(docID);
      pendingDeleteCount++;
    }
    return didDelete;
  }

  /** Returns a snapshot of the current live docs. */
  Bits getLiveDocs() {
    // Prevent modifications to the returned live docs
    writeableLiveDocs = null;
    return liveDocs;
  }

  /** Returns a snapshot of the hard live docs. */
  Bits getHardLiveDocs() {
    return getLiveDocs();
  }

  /** Returns the number of pending deletes that are not written to disk. */
  protected int numPendingDeletes() {
    return pendingDeleteCount;
  }

  /**
   * Called once a new reader is opened for this segment ie. when deletes or updates are applied.
   */
  void onNewReader(CodecReader reader, SegmentCommitInfo info) throws IOException {
    if (liveDocsInitialized == false) {
      assert writeableLiveDocs == null;
      if (reader.hasDeletions()) {
        // we only initialize this once either in the ctor or here
        // if we use the live docs from a reader it has to be in a situation where we don't
        // have any existing live docs
        assert pendingDeleteCount == 0 : "pendingDeleteCount: " + pendingDeleteCount;
        liveDocs = reader.getLiveDocs();
        assert liveDocs == null
            || assertCheckLiveDocs(liveDocs, info.info.maxDoc(), info.getDelCount());
      }
      liveDocsInitialized = true;
    }
  }

  private boolean assertCheckLiveDocs(Bits bits, int expectedLength, int expectedDeleteCount) {
    assert bits.length() == expectedLength;
    int deletedCount = 0;
    for (int i = 0; i < bits.length(); i++) {
      if (bits.get(i) == false) {
        deletedCount++;
      }
    }
    assert deletedCount == expectedDeleteCount
        : "deleted: " + deletedCount + " != expected: " + expectedDeleteCount;
    return true;
  }

  /** Resets the pending docs */
  void dropChanges() {
    pendingDeleteCount = 0;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("PendingDeletes(seg=").append(info);
    sb.append(" numPendingDeletes=").append(pendingDeleteCount);
    sb.append(" writeable=").append(writeableLiveDocs != null);
    return sb.toString();
  }

  /** Writes the live docs to disk and returns <code>true</code> if any new docs were written. */
  boolean writeLiveDocs(Directory dir) throws IOException {
    if (pendingDeleteCount == 0) {
      return false;
    }

    Bits liveDocs = this.liveDocs;
    assert liveDocs != null;
    // We have new deletes
    assert liveDocs.length() == info.info.maxDoc();

    // Do this so we can delete any created files on
    // exception; this saves all codecs from having to do
    // it:
    TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);

    // We can write directly to the actual name (vs to a
    // .tmp & renaming it) because the file is not live
    // until segments file is written:
    boolean success = false;
    try {
      Codec codec = info.info.getCodec();
      codec
          .liveDocsFormat()
          .writeLiveDocs(liveDocs, trackingDir, info, pendingDeleteCount, IOContext.DEFAULT);
      success = true;
    } finally {
      if (!success) {
        // Advance only the nextWriteDelGen so that a 2nd
        // attempt to write will write to a new file
        info.advanceNextWriteDelGen();

        // Delete any partially created file(s):
        for (String fileName : trackingDir.getCreatedFiles()) {
          IOUtils.deleteFilesIgnoringExceptions(dir, fileName);
        }
      }
    }

    // If we hit an exc in the line above (eg disk full)
    // then info's delGen remains pointing to the previous
    // (successfully written) del docs:
    info.advanceDelGen();
    info.setDelCount(info.getDelCount() + pendingDeleteCount);
    dropChanges();
    return true;
  }

  /**
   * Returns <code>true</code> iff the segment represented by this {@link PendingDeletes} is fully
   * deleted
   */
  boolean isFullyDeleted(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
    return getDelCount() == info.info.maxDoc();
  }

  /**
   * Called for every field update for the given field at flush time
   *
   * @param info the field info of the field that's updated
   * @param iterator the values to apply
   */
  void onDocValuesUpdate(FieldInfo info, DocValuesFieldUpdates.Iterator iterator)
      throws IOException {}

  int numDeletesToMerge(MergePolicy policy, IOSupplier<CodecReader> readerIOSupplier)
      throws IOException {
    return policy.numDeletesToMerge(info, getDelCount(), readerIOSupplier);
  }

  /** Returns true if the given reader needs to be refreshed in order to see the latest deletes */
  final boolean needsRefresh(CodecReader reader) {
    return reader.getLiveDocs() != getLiveDocs() || reader.numDeletedDocs() != getDelCount();
  }

  /** Returns the number of deleted docs in the segment. */
  final int getDelCount() {
    int delCount = info.getDelCount() + info.getSoftDelCount() + numPendingDeletes();
    return delCount;
  }

  /** Returns the number of live documents in this segment */
  final int numDocs() {
    return info.info.maxDoc() - getDelCount();
  }

  // Call only from assert!
  boolean verifyDocCounts(CodecReader reader) {
    int count = 0;
    Bits liveDocs = getLiveDocs();
    if (liveDocs != null) {
      for (int docID = 0; docID < info.info.maxDoc(); docID++) {
        if (liveDocs.get(docID)) {
          count++;
        }
      }
    } else {
      count = info.info.maxDoc();
    }
    assert numDocs() == count
        : "info.maxDoc="
            + info.info.maxDoc()
            + " info.getDelCount()="
            + info.getDelCount()
            + " info.getSoftDelCount()="
            + info.getSoftDelCount()
            + " pendingDeletes="
            + toString()
            + " count="
            + count
            + " numDocs: "
            + numDocs();
    assert reader.numDocs() == numDocs()
        : "reader.numDocs() = " + reader.numDocs() + " numDocs() " + numDocs();
    assert reader.numDeletedDocs() <= info.info.maxDoc()
        : "delCount="
            + reader.numDeletedDocs()
            + " info.maxDoc="
            + info.info.maxDoc()
            + " rld.pendingDeleteCount="
            + numPendingDeletes()
            + " info.getDelCount()="
            + info.getDelCount();
    return true;
  }

  /**
   * Returns {@code true} if we have to initialize this PendingDeletes before {@link #delete(int)};
   * otherwise this PendingDeletes is ready to accept deletes. A PendingDeletes can be initialized
   * by providing it a reader via {@link #onNewReader(CodecReader, SegmentCommitInfo)}.
   */
  boolean mustInitOnDelete() {
    return false;
  }
}
