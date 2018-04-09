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
import java.util.List;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.MutableBits;

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
    MutableBits mutableBits = getMutableBits(); // we need to fetch this first it might be a shared instance with hardDeletes
    if (hardDeletes.delete(docID)) {
      if (mutableBits.get(docID)) { // delete it here too!
        mutableBits.clear(docID);
        assert hardDeletes.delete(docID) == false;
      } else {
        // if it was deleted subtract the delCount
        pendingDeleteCount--;
      }
      return true;
    }
    return false;
  }

  @Override
  int numPendingDeletes() {
    return super.numPendingDeletes() + hardDeletes.numPendingDeletes();
  }

  @Override
  void onNewReader(SegmentReader reader, SegmentCommitInfo info) throws IOException {
    super.onNewReader(reader, info);
    hardDeletes.onNewReader(reader, info);
    if (dvGeneration != info.getDocValuesGen()) { // only re-calculate this if we haven't seen this generation
      final DocIdSetIterator iterator = DocValuesFieldExistsQuery.getDocValuesDocIdSetIterator(field, reader);
      if (iterator == null) { // nothing is deleted we don't have a soft deletes field in this segment
        this.pendingDeleteCount = 0;
      } else {
        assert info.info.maxDoc() > 0 : "maxDoc is 0";
        pendingDeleteCount += applySoftDeletes(iterator, getMutableBits());
      }
      dvGeneration = info.getDocValuesGen();
    }
    assert numPendingDeletes() + info.getDelCount() <= info.info.maxDoc() :
        numPendingDeletes() + " + " + info.getDelCount() + " > " + info.info.maxDoc();
  }

  @Override
  boolean writeLiveDocs(Directory dir) throws IOException {
    // delegate the write to the hard deletes - it will only write if somebody used it.
    return hardDeletes.writeLiveDocs(dir);
  }

  @Override
  void reset() {
    dvGeneration = -2;
    super.reset();
    hardDeletes.reset();
  }

  /**
   * Clears all bits in the given bitset that are set and are also in the given DocIdSetIterator.
   *
   * @param iterator the doc ID set iterator for apply
   * @param bits the bit set to apply the deletes to
   * @return the number of bits changed by this function
   */
  static int applySoftDeletes(DocIdSetIterator iterator, MutableBits bits) throws IOException {
    assert iterator != null;
    int newDeletes = 0;
    int docID;
    while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (bits.get(docID)) { // doc is live - clear it
        bits.clear(docID);
        newDeletes++;
        // now that we know we deleted it and we fully control the hard deletes we can do correct accounting
        // below.
      }
    }
    return newDeletes;
  }

  @Override
  void onDocValuesUpdate(FieldInfo info, List<DocValuesFieldUpdates> updatesToApply) throws IOException {
    if (field.equals(info.name)) {
      assert dvGeneration < info.getDocValuesGen() : "we have seen this generation update already: " + dvGeneration + " vs. " + info.getDocValuesGen();
      DocValuesFieldUpdates.Iterator[] subs = new DocValuesFieldUpdates.Iterator[updatesToApply.size()];
      for(int i=0; i<subs.length; i++) {
        subs[i] = updatesToApply.get(i).iterator();
      }
      DocValuesFieldUpdates.Iterator iterator = DocValuesFieldUpdates.mergedIterator(subs);
      pendingDeleteCount += applySoftDeletes(new DocIdSetIterator() {
        int docID = -1;
        @Override
        public int docID() {
          return docID;
        }

        @Override
        public int nextDoc() {
          return docID = iterator.nextDoc();
        }

        @Override
        public int advance(int target) {
          throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
          throw new UnsupportedOperationException();
        }
      }, getMutableBits());
      dvGeneration = info.getDocValuesGen();
    }
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
}
