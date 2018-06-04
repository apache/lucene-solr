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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;

final class PendingSoftDeletes extends PendingDeletes {

  private final String field;
  private long dvGeneration = -2;
  private final PendingDeletes hardDeletes;

  PendingSoftDeletes(String field, SegmentCommitInfo info)  {
    super(info);
    this.field = field;
    hardDeletes = new PendingDeletes(info);
  }

  PendingSoftDeletes(String field, SegmentReader reader, SegmentCommitInfo info) {
    super(reader, info);
    this.field = field;
    hardDeletes = new PendingDeletes(reader, info);
  }

  @Override
  boolean delete(int docID) throws IOException {
    FixedBitSet mutableBits = getMutableBits(); // we need to fetch this first it might be a shared instance with hardDeletes
    if (hardDeletes.delete(docID)) {
      if (mutableBits.get(docID)) { // delete it here too!
        mutableBits.clear(docID);
        assert hardDeletes.delete(docID) == false;
      } else {
        // if it was deleted subtract the delCount
        pendingDeleteCount--;
        assert assertPendingDeletes();
      }
      return true;
    }
    return false;
  }

  @Override
  protected int numPendingDeletes() {
    return super.numPendingDeletes() + hardDeletes.numPendingDeletes();
  }

  @Override
  void onNewReader(CodecReader reader, SegmentCommitInfo info) throws IOException {
    super.onNewReader(reader, info);
    hardDeletes.onNewReader(reader, info);
    if (dvGeneration < info.getDocValuesGen()) { // only re-calculate this if we haven't seen this generation
      final DocIdSetIterator iterator = DocValuesFieldExistsQuery.getDocValuesDocIdSetIterator(field, reader);
      int newDelCount;
      if (iterator != null) { // nothing is deleted we don't have a soft deletes field in this segment
        assert info.info.maxDoc() > 0 : "maxDoc is 0";
        newDelCount = applySoftDeletes(iterator, getMutableBits());
        assert newDelCount >= 0 : " illegal pending delete count: " + newDelCount;
      } else {
        newDelCount = 0;
      }
      assert info.getSoftDelCount() == newDelCount : "softDeleteCount doesn't match " + info.getSoftDelCount() + " != " + newDelCount;
      dvGeneration = info.getDocValuesGen();
    }
    assert getDelCount() <= info.info.maxDoc() : getDelCount() + " > " + info.info.maxDoc();
  }

  @Override
  boolean writeLiveDocs(Directory dir) throws IOException {
    // we need to set this here to make sure our stats in SCI are up-to-date otherwise we might hit an assertion
    // when the hard deletes are set since we need to account for docs that used to be only soft-delete but now hard-deleted
    this.info.setSoftDelCount(this.info.getSoftDelCount() + pendingDeleteCount);
    super.dropChanges();
    // delegate the write to the hard deletes - it will only write if somebody used it.
    if (hardDeletes.writeLiveDocs(dir)) {
      return true;
    }
    return false;
  }

  @Override
  void dropChanges() {
    // don't reset anything here - this is called after a merge (successful or not) to prevent
    // rewriting the deleted docs to disk. we only pass it on and reset the number of pending deletes
    hardDeletes.dropChanges();
  }

  /**
   * Clears all bits in the given bitset that are set and are also in the given DocIdSetIterator.
   *
   * @param iterator the doc ID set iterator for apply
   * @param bits the bit set to apply the deletes to
   * @return the number of bits changed by this function
   */
  static int applySoftDeletes(DocIdSetIterator iterator, FixedBitSet bits) throws IOException {
    assert iterator != null;
    int newDeletes = 0;
    int docID;
    DocValuesFieldUpdates.Iterator hasValue = iterator instanceof DocValuesFieldUpdates.Iterator
        ? (DocValuesFieldUpdates.Iterator) iterator : null;
    while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (hasValue == null || hasValue.hasValue()) {
        if (bits.get(docID)) { // doc is live - clear it
          bits.clear(docID);
          newDeletes++;
          // now that we know we deleted it and we fully control the hard deletes we can do correct accounting
          // below.
        }
      } else {
        if (bits.get(docID) == false) {
          bits.set(docID);
          newDeletes--;
        }
      }
    }
    return newDeletes;
  }

  @Override
  void onDocValuesUpdate(FieldInfo info, DocValuesFieldUpdates.Iterator iterator) throws IOException {
    if (this.field.equals(info.name)) {
      pendingDeleteCount += applySoftDeletes(iterator, getMutableBits());
      assert assertPendingDeletes();
      assert dvGeneration < info.getDocValuesGen() : "we have seen this generation update already: " + dvGeneration + " vs. " + info.getDocValuesGen();
      assert dvGeneration != -2 : "docValues generation is still uninitialized";
      dvGeneration = info.getDocValuesGen();
      this.info.setSoftDelCount(this.info.getSoftDelCount() + pendingDeleteCount);
      super.dropChanges();
    }
  }

  private boolean assertPendingDeletes() {
    assert pendingDeleteCount + info.getSoftDelCount() >= 0 : " illegal pending delete count: " + pendingDeleteCount + info.getSoftDelCount();
    assert info.info.maxDoc() >= getDelCount();
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("PendingSoftDeletes(seg=").append(info);
    sb.append(" numPendingDeletes=").append(pendingDeleteCount);
    sb.append(" field=").append(field);
    sb.append(" dvGeneration=").append(dvGeneration);
    sb.append(" hardDeletes=").append(hardDeletes);
    return sb.toString();
  }

  @Override
  int numDeletesToMerge(MergePolicy policy, IOSupplier<CodecReader> readerIOSupplier) throws IOException {
    ensureInitialized(readerIOSupplier); // initialize to ensure we have accurate counts
    return super.numDeletesToMerge(policy, readerIOSupplier);
  }

  private void ensureInitialized(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
    if (dvGeneration == -2) {
      FieldInfos fieldInfos = readFieldInfos();
      FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
      // we try to only open a reader if it's really necessary ie. indices that are mainly append only might have
      // big segments that don't even have any docs in the soft deletes field. In such a case it's simply
      // enough to look at the FieldInfo for the field and check if the field has DocValues
      if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.NONE) {
        // in order to get accurate numbers we need to have a least one reader see here.
        onNewReader(readerIOSupplier.get(), info);
      } else {
        // we are safe here since we don't have any doc values for the soft-delete field on disk
        // no need to open a new reader
        dvGeneration = fieldInfo == null ? -1 : fieldInfo.getDocValuesGen();
      }
    }
  }

  @Override
  boolean isFullyDeleted(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
    ensureInitialized(readerIOSupplier); // initialize to ensure we have accurate counts - only needed in the soft-delete case
    return super.isFullyDeleted(readerIOSupplier);
  }

  private FieldInfos readFieldInfos() throws IOException {
    SegmentInfo segInfo = info.info;
    Directory dir = segInfo.dir;
    if (info.hasFieldUpdates() == false) {
      // updates always outside of CFS
      Closeable toClose;
      if (segInfo.getUseCompoundFile()) {
        toClose = dir = segInfo.getCodec().compoundFormat().getCompoundReader(segInfo.dir, segInfo, IOContext.READONCE);
      } else {
        toClose = null;
        dir = segInfo.dir;
      }
      try {
        return segInfo.getCodec().fieldInfosFormat().read(dir, segInfo, "", IOContext.READONCE);
      } finally {
        IOUtils.close(toClose);
      }
    } else {
      FieldInfosFormat fisFormat = segInfo.getCodec().fieldInfosFormat();
      final String segmentSuffix = Long.toString(info.getFieldInfosGen(), Character.MAX_RADIX);
      return fisFormat.read(dir, segInfo, segmentSuffix, IOContext.READONCE);
    }
  }

  @Override
  Bits getHardLiveDocs() {
    return hardDeletes.getLiveDocs();
  }

  static int countSoftDeletes(DocIdSetIterator softDeletedDocs, Bits hardDeletes) throws IOException {
    int count = 0;
    if (softDeletedDocs != null) {
      int doc;
      while ((doc = softDeletedDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (hardDeletes == null || hardDeletes.get(doc)) {
          count++;
        }
      }
    }
    return count;
  }
}
