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
import java.util.List;
import java.util.PriorityQueue;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.VectorValues;
import org.apache.lucene.codecs.VectorsReader;
import org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

public class Lucene90VectorsReader extends VectorsReader {
  private final DocValuesProducer docValuesReader;
  private final FieldsProducer postingsReader;

  public Lucene90VectorsReader(SegmentReadState state) throws IOException {
    this.docValuesReader = new Lucene80DocValuesFormat().fieldsProducer(state);
    this.postingsReader = new Lucene84PostingsFormat().fieldsProducer(state);
  }

  @Override
  public void checkIntegrity() throws IOException {
    docValuesReader.checkIntegrity();
    postingsReader.checkIntegrity();
  }

  @Override
  public VectorValues getVectorValues(FieldInfo field) {
    return new VectorValues() {
      @Override
      public BinaryDocValues getValues() throws IOException {
        return docValuesReader.getBinary(field);
      }

      @Override
      public DocIdSetIterator getNearestVectors(float[] query, int numProbes) throws IOException {
        Terms clusterTerms = postingsReader.terms(field.name);
        TermsEnum clusters = clusterTerms.iterator();

        List<PostingsEnum> closestClusters = findClosestClusters(query, numProbes, clusters);
        DisiPriorityQueue queue = new DisiPriorityQueue(closestClusters.size());
        for (PostingsEnum cluster : closestClusters) {
          queue.add(new DisiWrapper(cluster));
        }
        return new DisjunctionDISIApproximation(queue);
      }
    };
  }

  @Override
  public void close() throws IOException {
    docValuesReader.close();
    postingsReader.close();
  }

  private static class ClusterWithDistance {
    double distance;
    PostingsEnum postings;

    public ClusterWithDistance(double distance, PostingsEnum postings) {
      this.distance = distance;
      this.postings = postings;
    }
  }

  private List<PostingsEnum> findClosestClusters(float[] queryVector,
                                                 int numProbes,
                                                 TermsEnum centroids) throws IOException {
    PriorityQueue<ClusterWithDistance> queue = new PriorityQueue<>(
        (first, second) -> -1 * Double.compare(first.distance, second.distance));

    while (true) {
      BytesRef encodedCentroid = centroids.next();
      if (encodedCentroid == null) {
        break;
      }

      double dist = VectorValues.l2norm(queryVector, encodedCentroid);
      if (queue.size() < numProbes) {
        queue.add(new ClusterWithDistance(dist, centroids.postings(null, PostingsEnum.NONE)));
      } else {
        ClusterWithDistance head = queue.peek();
        if (dist < head.distance) {
          queue.poll();
          queue.add(new ClusterWithDistance(dist, centroids.postings(null, PostingsEnum.NONE)));
        }
      }
    }

    List<PostingsEnum> closestCentroids = new ArrayList<>(queue.size());
    for (ClusterWithDistance cluster : queue) {
      closestCentroids.add(cluster.postings);
    }
    return closestCentroids;
  }

}
