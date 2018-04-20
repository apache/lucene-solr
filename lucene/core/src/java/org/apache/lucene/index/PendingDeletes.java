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
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.MutableBits;

/**
 * This class handles accounting and applying pending deletes for live segment readers
 */
class PendingDeletes {
  protected final SegmentCommitInfo info;
  // True if the current liveDocs is referenced by an
  // external NRT reader:
  protected boolean liveDocsShared;
  // Holds the current shared (readable and writable)
  // liveDocs.  This is null when there are no deleted
  // docs, and it's copy-on-write (cloned whenever we need
  // to change it but it's been shared to an external NRT
  // reader).
  private Bits liveDocs;
  protected int pendingDeleteCount;
  private boolean liveDocsInitialized;

  PendingDeletes(SegmentReader reader, SegmentCommitInfo info) {
    this(info, reader.getLiveDocs(), true);
    pendingDeleteCount = reader.numDeletedDocs() - info.getDelCount();
  }

  PendingDeletes(SegmentCommitInfo info) {
    this(info, null, info.hasDeletions() == false);
    // if we don't have deletions we can mark it as initialized since we might receive deletes on a segment
    // without having a reader opened on it ie. after a merge when we apply the deletes that IW received while merging.
    // For segments that were published we enforce a reader in the BufferedUpdatesStream.SegmentState ctor
  }

  private PendingDeletes(SegmentCommitInfo info, Bits liveDocs, boolean liveDocsInitialized) {
    this.info = info;
    liveDocsShared = true;
    this.liveDocs = liveDocs;
    pendingDeleteCount = 0;
    this.liveDocsInitialized = liveDocsInitialized;
  }


  protected MutableBits getMutableBits() throws IOException {
    // if we pull mutable bits but we haven't been initialized something is completely off.
    // this means we receive deletes without having the bitset that is on-disk ready to be cloned
    assert liveDocsInitialized : "can't delete if liveDocs are not initialized";
    if (liveDocsShared) {
      // Copy on write: this means we've cloned a
      // SegmentReader sharing the current liveDocs
      // instance; must now make a private clone so we can
      // change it:
      LiveDocsFormat liveDocsFormat = info.info.getCodec().liveDocsFormat();
      MutableBits mutableBits;
      if (liveDocs == null) {
        mutableBits = liveDocsFormat.newLiveDocs(info.info.maxDoc());
      } else {
        mutableBits = liveDocsFormat.newLiveDocs(liveDocs);
      }
      liveDocs = mutableBits;
      liveDocsShared = false;
    }
    return (MutableBits) liveDocs;
  }


  /**
   * Marks a document as deleted in this segment and return true if a document got actually deleted or
   * if the document was already deleted.
   */
  boolean delete(int docID) throws IOException {
    assert info.info.maxDoc() > 0;
    MutableBits mutableBits = getMutableBits();
    assert mutableBits != null;
    assert docID >= 0 && docID < mutableBits.length() : "out of bounds: docid=" + docID + " liveDocsLength=" + mutableBits.length() + " seg=" + info.info.name + " maxDoc=" + info.info.maxDoc();
    assert !liveDocsShared;
    final boolean didDelete = mutableBits.get(docID);
    if (didDelete) {
      mutableBits.clear(docID);
      pendingDeleteCount++;
    }
    return didDelete;
  }

  /**
   * Should be called if the live docs returned from {@link #getLiveDocs()} are shared outside of the
   * {@link ReadersAndUpdates}
   */
  void liveDocsShared() {
    liveDocsShared = true;
  }

  /**
   * Returns the current live docs or null if all docs are live. The returned instance might be mutable or is mutated behind the scenes.
   * If the returned live docs are shared outside of the ReadersAndUpdates {@link #liveDocsShared()} should be called
   * first.
   */
  Bits getLiveDocs() {
    return liveDocs;
  }

  /**
   * Returns the number of pending deletes that are not written to disk.
   */
  int numPendingDeletes() {
    return pendingDeleteCount;
  }

  /**
   * Called once a new reader is opened for this segment ie. when deletes or updates are applied.
   */
  void onNewReader(CodecReader reader, SegmentCommitInfo info) throws IOException {
    if (liveDocsInitialized == false) {
      if (reader.hasDeletions()) {
        // we only initialize this once either in the ctor or here
        // if we use the live docs from a reader it has to be in a situation where we don't
        // have any existing live docs
        assert pendingDeleteCount == 0 : "pendingDeleteCount: " + pendingDeleteCount;
        liveDocs = reader.getLiveDocs();
        assert liveDocs == null || assertCheckLiveDocs(liveDocs, info.info.maxDoc(), info.getDelCount());
        liveDocsShared = true;

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
    assert deletedCount == expectedDeleteCount : "deleted: " + deletedCount + " != expected: " + expectedDeleteCount;
    return true;
  }

  /**
   * Resets the pending docs
   */
  void dropChanges() {
    pendingDeleteCount = 0;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("PendingDeletes(seg=").append(info);
    sb.append(" numPendingDeletes=").append(pendingDeleteCount);
    sb.append(" liveDocsShared=").append(liveDocsShared);
    return sb.toString();
  }

  /**
   * Writes the live docs to disk and returns <code>true</code> if any new docs were written.
   */
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
      codec.liveDocsFormat().writeLiveDocs((MutableBits)liveDocs, trackingDir, info, pendingDeleteCount, IOContext.DEFAULT);
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
   * Returns <code>true</code> iff the segment represented by this {@link PendingDeletes} is fully deleted
   */
  boolean isFullyDeleted(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
    return info.getDelCount() + numPendingDeletes() == info.info.maxDoc();
  }

  /**
   * Called before the given DocValuesFieldUpdates are written to disk
   * @param info the field to apply
   */
  void onDocValuesUpdate(FieldInfo info) {
  }

  /**
   * Called for every field update for the given field
   * @param field the field that's updated
   * @param iterator the values to apply
   */
  void onDocValuesUpdate(String field, DocValuesFieldUpdates.Iterator iterator) throws IOException {
  }

  int numDeletesToMerge(MergePolicy policy, IOSupplier<CodecReader> readerIOSupplier) throws IOException {
    return policy.numDeletesToMerge(info, numPendingDeletes(), readerIOSupplier);
  }
}
