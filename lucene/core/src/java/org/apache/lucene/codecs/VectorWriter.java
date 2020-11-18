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

package org.apache.lucene.codecs;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Writes vectors to an index.
 */
public abstract class VectorWriter implements Closeable {

  /** Sole constructor */
  protected VectorWriter() {}

  /** Write all values contained in the provided reader */
  public abstract void writeField(FieldInfo fieldInfo, VectorValues values) throws IOException;

  /** Called once at the end before close */
  public abstract void finish() throws IOException;

  /** Merge the vector values from multiple segments, for all fields */
  public void merge(MergeState mergeState) throws IOException {
    for (int i = 0; i < mergeState.fieldInfos.length; i++) {
      VectorReader reader = mergeState.vectorReaders[i];
      assert reader != null || mergeState.fieldInfos[i].hasVectorValues() == false;
      if (reader != null) {
        reader.checkIntegrity();
      }
    }
    for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
      if (fieldInfo.hasVectorValues()) {
        mergeVectors(fieldInfo, mergeState);
      }
    }
    finish();
  }

  private void mergeVectors(FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
    if (mergeState.infoStream.isEnabled("VV")) {
      mergeState.infoStream.message("VV", "merging " + mergeState.segmentInfo);
    }
    List<VectorValuesSub> subs = new ArrayList<>();
    int dimension = -1;
    VectorValues.SearchStrategy searchStrategy = null;
    int nonEmptySegmentIndex = 0;
    for (int i = 0; i < mergeState.vectorReaders.length; i++) {
      VectorReader vectorReader = mergeState.vectorReaders[i];
      if (vectorReader != null) {
        if (mergeFieldInfo != null && mergeFieldInfo.hasVectorValues()) {
          int segmentDimension = mergeFieldInfo.getVectorDimension();
          VectorValues.SearchStrategy segmentSearchStrategy = mergeFieldInfo.getVectorSearchStrategy();
          if (dimension == -1) {
            dimension = segmentDimension;
            searchStrategy = mergeFieldInfo.getVectorSearchStrategy();
          } else if (dimension != segmentDimension) {
            throw new IllegalStateException("Varying dimensions for vector-valued field " + mergeFieldInfo.name
                + ": " + dimension + "!=" + segmentDimension);
          } else if (searchStrategy != segmentSearchStrategy) {
            throw new IllegalStateException("Varying search strategys for vector-valued field " + mergeFieldInfo.name
                + ": " + searchStrategy + "!=" + segmentSearchStrategy);
          }
          VectorValues values = vectorReader.getVectorValues(mergeFieldInfo.name);
          if (values != null) {
            subs.add(new VectorValuesSub(nonEmptySegmentIndex++, mergeState.docMaps[i], values));
          }
        }
      }
    }
    // Create a new VectorValues by iterating over the sub vectors, mapping the resulting
    // docids using docMaps in the mergeState.
    if (subs.size() > 0) {
      writeField(mergeFieldInfo, new VectorValuesMerger(subs, mergeState));
    }
    if (mergeState.infoStream.isEnabled("VV")) {
      mergeState.infoStream.message("VV", "merge done " + mergeState.segmentInfo);
    }
  }

  /** Tracks state of one sub-reader that we are merging */
  private static class VectorValuesSub extends DocIDMerger.Sub {

    final MergeState.DocMap docMap;
    final VectorValues values;
    final int segmentIndex;
    int count;

    VectorValuesSub(int segmentIndex, MergeState.DocMap docMap, VectorValues values) {
      super(docMap);
      this.values = values;
      this.segmentIndex = segmentIndex;
      this.docMap = docMap;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      int docId = values.nextDoc();
      if (docId != NO_MORE_DOCS) {
        // Note: this does count deleted docs since they are present in the to-be-merged segment
        ++count;
      }
      return docId;
    }
  }

  /**
   * View over multiple VectorValues supporting iterator-style access via DocIdMerger. Maintains a reverse ordinal
   * mapping for documents having values in order to support random access by dense ordinal.
   */
  private static class VectorValuesMerger extends VectorValues implements RandomAccessVectorValuesProducer {
    private final List<VectorValuesSub> subs;
    private final DocIDMerger<VectorValuesSub> docIdMerger;
    private final int[] ordBase;
    private final int cost;
    private final int size;

    private int docId;
    private VectorValuesSub current;
    // For each doc with a vector, record its ord in the segments being merged. This enables random access into the
    // unmerged segments using the ords from the merged segment.
    private int[] ordMap;
    private int ord;

    VectorValuesMerger(List<VectorValuesSub> subs, MergeState mergeState) throws IOException {
      this.subs = subs;
      docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
      int totalCost = 0, totalSize = 0;
      for (VectorValuesSub sub : subs) {
        totalCost += sub.values.cost();
        totalSize += sub.values.size();
      }
      cost = totalCost;
      size = totalSize;
      ordMap = new int[size];
      ordBase = new int[subs.size()];
      int lastBase = 0;
      for (int k = 0; k < subs.size(); k++) {
        int size = subs.get(k).values.size();
        ordBase[k] = lastBase;
        lastBase += size;
      }
      docId = -1;
    }

    @Override
    public int docID() {
      return docId;
    }

    @Override
    public int nextDoc() throws IOException {
      current = docIdMerger.next();
      if (current == null) {
        docId = NO_MORE_DOCS;
      } else {
        docId = current.mappedDocID;
        ordMap[ord++] = ordBase[current.segmentIndex] + current.count - 1;
      }
      return docId;
    }

    @Override
    public float[] vectorValue() throws IOException {
      return current.values.vectorValue();
    }

    @Override
    public BytesRef binaryValue() throws IOException {
      return current.values.binaryValue();
    }

    @Override
    public RandomAccessVectorValues randomAccess() {
      return new MergerRandomAccess();
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public long cost() {
      return cost;
    }

    @Override
    public int dimension() {
      return subs.get(0).values.dimension();
    }

    @Override
    public SearchStrategy searchStrategy() {
      return subs.get(0).values.searchStrategy();
    }

    @Override
    public TopDocs search(float[] target, int k, int fanout) throws IOException {
      throw new UnsupportedOperationException();
    }

    class MergerRandomAccess implements RandomAccessVectorValues {

      private final List<RandomAccessVectorValues> raSubs;

      MergerRandomAccess() {
        raSubs = new ArrayList<>(subs.size());
        for (VectorValuesSub sub : subs) {
          if (sub.values instanceof RandomAccessVectorValuesProducer) {
            raSubs.add(((RandomAccessVectorValuesProducer) sub.values).randomAccess());
          } else {
            throw new IllegalStateException("Cannot merge VectorValues without support for random access");
          }
        }
      }

      @Override
      public int size() {
        return size;
      }

      @Override
      public int dimension() {
        return VectorValuesMerger.this.dimension();
      }

      @Override
      public SearchStrategy searchStrategy() {
        return VectorValuesMerger.this.searchStrategy();
      }

      @Override
      public float[] vectorValue(int target) throws IOException {
        int unmappedOrd = ordMap[target];
        int segmentOrd = Arrays.binarySearch(ordBase, unmappedOrd);
        if (segmentOrd < 0) {
          // get the index of the greatest lower bound
          segmentOrd = -2 - segmentOrd;
        }
        while(segmentOrd < ordBase.length - 1 && ordBase[segmentOrd + 1] == ordBase[segmentOrd]) {
          // forward over empty segments which will share the same ordBase
          segmentOrd++;
        }
        return raSubs.get(segmentOrd).vectorValue(unmappedOrd - ordBase[segmentOrd]);
      }

      @Override
      public BytesRef binaryValue(int targetOrd) throws IOException {
        throw new UnsupportedOperationException();
      }
    }
  }
}
