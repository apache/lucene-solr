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

package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.VectorsWriter;
import org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat;

import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.VectorClusteringMethod;
import org.apache.lucene.index.VectorClusteringMethod.ClusterCentroids;

import org.apache.lucene.util.BytesRef;

/**
 * NOTE: there are some hacks in this class.
 *   - We re-use the existing doc values and postings formats for simplicity. This is fairly fragile since we write
 *     to the same files. For example there could be a conflict if there were both a vector field and a doc values
 *     field with the same name.
 *   - To write the postings list, we compute the map from centroid to documents in memory. We then expose it through
 *     a hacky {@link Fields} implementation called {@link ClusterBackedFields} and pass it to the postings writer. It
 *     would be better to avoid this hack and not to compute cluster information using a map.
 */
public class Lucene90VectorsWriter extends VectorsWriter {
  private final DocValuesConsumer docValuesWriter;
  private final FieldsConsumer postingsWriter;
  private final VectorClusteringMethod clusteringMethod;

  public Lucene90VectorsWriter(SegmentWriteState state) throws IOException {
    int numDocs = state.segmentInfo.maxDoc();
    clusteringMethod = new VectorClusteringMethod(numDocs);

    docValuesWriter = new Lucene80DocValuesFormat().fieldsConsumer(state);
    postingsWriter = new Lucene84PostingsFormat().fieldsConsumer(state);
  }

  @Override
  public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    docValuesWriter.addBinaryField(field, valuesProducer);
    ClusterCentroids centroids = clusteringMethod.computeCentroids(field, valuesProducer);

    SortedMap<BytesRef, List<Integer>> clusters = new TreeMap<>();
    BinaryDocValues vectorValues = valuesProducer.getBinary(field);
    int docCount = 0;

    while (vectorValues.nextDoc() != BinaryDocValues.NO_MORE_DOCS) {
      BytesRef encodedCentroid = centroids.findClosestCentroid(vectorValues);
      if (clusters.containsKey(encodedCentroid) == false) {
        clusters.put(encodedCentroid, new ArrayList<>());
      }
      clusters.get(encodedCentroid).add(vectorValues.docID());
      docCount++;
    }

    Fields clusterFields = new ClusterBackedFields(field, clusters, docCount);
    postingsWriter.write(clusterFields, null);
  }

  @Override
  public void close() throws IOException {
    docValuesWriter.close();
    postingsWriter.close();
  }

  private static class ClusterBackedFields extends Fields {
    private final String fieldName;
    private final ClusterTerms clusterTerms;

    ClusterBackedFields(FieldInfo fieldInfo,
                        SortedMap<BytesRef, List<Integer>> centroids,
                        int docCount) {
      this.fieldName = fieldInfo.name;
      this.clusterTerms = new ClusterTerms(centroids, docCount);
    }

    @Override
    public Iterator<String> iterator() {
      return Collections.singleton(fieldName).iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      return clusterTerms;
    }

    @Override
    public int size() {
      return 1;
    }

    private static class ClusterTerms extends Terms {
      private final SortedMap<BytesRef, List<Integer>> centroids;
      private final int docCount;

      ClusterTerms(SortedMap<BytesRef, List<Integer>> centroids,
                   int docCount) {
        this.centroids = centroids;
        this.docCount = docCount;
      }

      @Override
      public TermsEnum iterator() throws IOException {
        return new ClusterTermsEnum(centroids);
      }

      @Override
      public long size() throws IOException {
        return centroids.size();
      }

      @Override
      public long getSumTotalTermFreq() throws IOException {
        return 0;
      }

      @Override
      public long getSumDocFreq() throws IOException {
        return 0;
      }

      @Override
      public int getDocCount() throws IOException {
        return docCount;
      }

      @Override
      public boolean hasFreqs() {
        return false;
      }

      @Override
      public boolean hasOffsets() {
        return false;
      }

      @Override
      public boolean hasPositions() {
        return false;
      }

      @Override
      public boolean hasPayloads() {
        return false;
      }

      private static class ClusterTermsEnum extends BaseTermsEnum {
        private final Iterator<Map.Entry<BytesRef, List<Integer>>> iterator;
        private Map.Entry<BytesRef, List<Integer>> currentCluster;

        ClusterTermsEnum(SortedMap<BytesRef, List<Integer>> centroids) {
          this.iterator = centroids.entrySet().iterator();
        }

        @Override
        public SeekStatus seekCeil(BytesRef term) throws IOException {
          throw new UnsupportedOperationException();
        }

        @Override
        public void seekExact(long ord) throws IOException {
          throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef term() throws IOException {
          return currentCluster.getKey();
        }

        @Override
        public long ord() throws IOException {
          throw new UnsupportedOperationException();
        }

        @Override
        public int docFreq() throws IOException {
          return currentCluster.getValue().size();
        }

        @Override
        public long totalTermFreq() throws IOException {
          return docFreq();
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
          return new ClusterPostingsEnum(currentCluster.getValue());
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
          throw new IllegalArgumentException();
        }

        @Override
        public BytesRef next() throws IOException {
          if (iterator.hasNext() == false) {
            return null;
          }

          currentCluster = iterator.next();
          return currentCluster.getKey();
        }

        private static class ClusterPostingsEnum extends PostingsEnum {
          private final List<Integer> docIds;
          private int docIndex = -1;

          public ClusterPostingsEnum(List<Integer> docIds) {
            this.docIds = docIds;
          }

          @Override
          public int freq() throws IOException {
            return 0;
          }

          @Override
          public int nextPosition() throws IOException {
            return 0;
          }

          @Override
          public int startOffset() throws IOException {
            return 0;
          }

          @Override
          public int endOffset() throws IOException {
            return 0;
          }

          @Override
          public BytesRef getPayload() throws IOException {
            return null;
          }

          @Override
          public int docID() {
            return docIds.get(docIndex);
          }

          @Override
          public int nextDoc() throws IOException {
            if (docIndex >= docIds.size() - 1) {
              return NO_MORE_DOCS;
            } else {
              docIndex++;
              return docID();
            }
          }

          @Override
          public int advance(int target) throws IOException {
            throw new IllegalArgumentException();
          }

          @Override
          public long cost() {
            throw new IllegalArgumentException();
          }
        }
      }
    }
  }
}
