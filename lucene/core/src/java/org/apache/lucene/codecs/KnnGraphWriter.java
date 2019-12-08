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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;

/**
 * Writes vectors and knn-graph to an index.
 */
public abstract class KnnGraphWriter implements Closeable {

  /** Sole constructor */
  protected KnnGraphWriter() {}

  /** Write all values contained in the provided reader */
  public abstract void writeField(FieldInfo fieldInfo, KnnGraphReader values) throws IOException;

  /** Called once at the end before close */
  public abstract void finish() throws IOException;

  public void merge(MergeState mergeState) throws IOException {
    for (KnnGraphReader reader : mergeState.knnGraphReaders) {
      if (reader != null) {
        reader.checkIntegrity();
      }
    }

    for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
      mergeOneField(fieldInfo, mergeState);
    }
    finish();
  }

  private void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    List<VectorValuesSub> vectorSubs = new ArrayList<>();
    List<KnnGraphValuesSub> graphSubs = new ArrayList<>();
    int topLevel = 0;
    final Set<EnterPoint> mappedEnterPoints = new HashSet<>();

    long cost = 0;
    for (int i = 0; i < mergeState.knnGraphReaders.length; i++) {
      KnnGraphReader knnGraphReader = mergeState.knnGraphReaders[i];
      if (knnGraphReader != null) {
        FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(fieldInfo.name);
        if (readerFieldInfo != null && readerFieldInfo.getVectorNumDimensions() != 0) {
          VectorValues vectorValues = knnGraphReader.getVectorValues(readerFieldInfo.name);
          KnnGraphValues graphValues = knnGraphReader.getGraphValues(readerFieldInfo.name);
          if (vectorValues != null && graphValues != null && graphValues.getTopLevel() >= 0) {
            cost += vectorValues.cost();
            vectorSubs.add(new VectorValuesSub(mergeState.docMaps[i], vectorValues));
            graphSubs.add(new KnnGraphValuesSub(mergeState.docMaps[i], graphValues));
            if (graphValues.getTopLevel() > topLevel) {
              topLevel = graphValues.getTopLevel();
            }
            int enterPoint = graphValues.getEnterPoints()[0];
            mappedEnterPoints.add(new EnterPoint(mergeState.docMaps[i].get(enterPoint), graphValues.getTopLevel()));
          }
        }
      }
    }

    final DocIDMerger<VectorValuesSub> vectorDocIdMerger = DocIDMerger.of(vectorSubs, mergeState.needsIndexSort);
    final DocIDMerger<KnnGraphValuesSub> graphDocIdMerger = DocIDMerger.of(graphSubs, mergeState.needsIndexSort);
    final long finalCost = cost;
    final int finalTopLevel = topLevel;

    final VectorValues meregdVectorValues = mergedVectorValues(vectorDocIdMerger, finalCost);
    final KnnGraphValues mergedGraphValues = mergedGraphValues(graphDocIdMerger, finalCost, finalTopLevel, mappedEnterPoints);

    writeField(fieldInfo, new KnnGraphReader() {
      @Override
      public void checkIntegrity() throws IOException {
      }

      @Override
      public VectorValues getVectorValues(String field) throws IOException {
        return meregdVectorValues;
      }

      @Override
      public KnnGraphValues getGraphValues(String field) throws IOException {
        return mergedGraphValues;
      }

      @Override
      public void close() throws IOException {
      }

      @Override
      public long ramBytesUsed() {
        return 0L;
      }
    });
  }

  private VectorValues mergedVectorValues(DocIDMerger<VectorValuesSub> docIDMerger, long finalCost) {
    return new VectorValues() {
      private VectorValuesSub current;
      private int docID = -1;

      @Override
      public float[] vectorValue() throws IOException {
        return current.values.vectorValue();
      }

      @Override
      public boolean seek(int target) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int docID() {
        return docID;
      }

      @Override
      public int nextDoc() throws IOException {
        current = docIDMerger.next();
        if (current == null) {
          docID = NO_MORE_DOCS;
        } else {
          docID = current.mappedDocID;
        }
        return docID;
      }

      @Override
      public int advance(int target) throws IOException {
        return 0;
      }

      @Override
      public long cost() {
        return finalCost;
      }
    };
  }

  private KnnGraphValues mergedGraphValues(DocIDMerger<KnnGraphValuesSub> docIDMerger, long finalCost, int finalTopLevel, Set<EnterPoint> enterPoints) throws IOException{
    return new KnnGraphValues() {
      private KnnGraphValuesSub current;
      private int docID = -1;

      @Override
      public int getTopLevel() {
        return finalTopLevel;
      }

      @Override
      public int[] getEnterPoints() {
        return enterPoints.stream().mapToInt(ep -> ep.docId).toArray();
      }

      @Override
      public int getMaxLevel() {
        return current.values.getMaxLevel();
      }

      @Override
      public IntsRef getFriends(int level) {
        // re-map friend ids
        IntsRef friends = current.values.getFriends(level);
        IntsRefBuilder mappedFriends = new IntsRefBuilder();
        mappedFriends.copyInts(friends);
        for (int i = 0; i < friends.length; i++) {
          mappedFriends.setIntAt(i, current.docMap.get(friends.ints[i]));
        }
        // make links to other graphs
        if (enterPoints.stream().anyMatch(ep -> ep.docId == docID)) {
          enterPoints.stream().filter(ep -> ep.docId != docID && ep.topLevel >= level).forEach(ep -> {
            mappedFriends.append(ep.docId);
          });
        }
        return mappedFriends.toIntsRef();
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int docID() {
        return docID;
      }

      @Override
      public int nextDoc() throws IOException {
        current = docIDMerger.next();
        if (current == null) {
          docID = NO_MORE_DOCS;
        } else {
          docID = current.mappedDocID;
        }
        return docID;
      }

      @Override
      public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long cost() {
        return finalCost;
      }
    };
  }

  private static class VectorValuesSub extends DocIDMerger.Sub {

    private final VectorValues values;

    VectorValuesSub(MergeState.DocMap docMap, VectorValues values) {
      super(docMap);
      this.values = values;
      assert values.docID() == -1;
    }


    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  private class KnnGraphValuesSub extends DocIDMerger.Sub {

    private final KnnGraphValues values;
    private final MergeState.DocMap docMap;

    KnnGraphValuesSub(MergeState.DocMap docMap, KnnGraphValues values) {
      super(docMap);
      this.docMap = docMap;
      this.values = values;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  private class EnterPoint {
    final int docId;
    final int topLevel;
    EnterPoint(int docId, int topLevel) {
      this.docId = docId;
      this.topLevel = topLevel;
    }
  }
}
