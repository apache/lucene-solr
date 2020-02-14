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
import java.util.Random;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.VectorValues;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

public class VectorClusteringMethod {
  private final static int NUM_ITERS = 10;

  private final int numDocs;
  private final int numCentroids;
  private final Random random;

  public VectorClusteringMethod(int numDocs) {
    this.numDocs = numDocs;
    this.numCentroids = (int) Math.sqrt(numDocs);
    this.random = new Random(42L);
  }

  public ClusterCentroids computeCentroids(FieldInfo field, DocValuesProducer vectorsReader) throws IOException {
    BinaryDocValues values = vectorsReader.getBinary(field);

    float[][] initialCentroids = new float[numCentroids][];
    int index = 0;
    while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      BytesRef bytes = values.binaryValue();

      if (index < numCentroids) {
        initialCentroids[index] = VectorValues.decode(bytes);
      } else if (random.nextDouble() < numCentroids * (1.0 / index)) {
        int c = random.nextInt(numCentroids);
        initialCentroids[c] = VectorValues.decode(bytes);
      }
      index++;
    }

    float[][] centroids = runKMeans(field, vectorsReader, initialCentroids);
    return new ClusterCentroids(centroids);
  }

  private float[][] runKMeans(FieldInfo field,
                              DocValuesProducer vectorsReader,
                              float[][] initialCentroids) throws IOException {
    System.out.println("Running k-means with [" + initialCentroids.length + "] centroids on [" + numDocs + "] docs...");
    int[] documentCentroids = new int[numDocs];

    for (int iter = 0; iter < NUM_ITERS; iter++) {
      initialCentroids = runKMeansStep(iter, field, vectorsReader, initialCentroids, documentCentroids);
    }

    System.out.println("Finished k-means.");
    return initialCentroids;
  }

  /**
   * Runs one iteration of k-means. For each document vector, we first find the
   * nearest centroid, then update the location of the new centroid.
   */
  private float[][] runKMeansStep(int iter,
                                  FieldInfo field,
                                  DocValuesProducer vectorsReader,
                                  float[][] centroids,
                                  int[] documentCentroids) throws IOException {
    double distToCentroid = 0.0;
    double distToOtherCentroids = 0.0;
    int numDocs = 0;

    float[][] newCentroids = new float[centroids.length][centroids[0].length];
    int[] newCentroidSize = new int[centroids.length];

    BinaryDocValues values = vectorsReader.getBinary(field);
    while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      BytesRef bytes = values.binaryValue();
      float[] vector = VectorValues.decode(bytes);

      int bestCentroid = -1;
      double bestDist = Double.MAX_VALUE;
      for (int c = 0; c < centroids.length; c++) {
        double dist = VectorValues.l2norm(centroids[c], vector);
        distToOtherCentroids += dist;

        if (dist < bestDist) {
          bestCentroid = c;
          bestDist = dist;
        }
      }

      newCentroidSize[bestCentroid]++;
      for (int v = 0; v < vector.length; v++) {
        newCentroids[bestCentroid][v] += vector[v];
      }

      distToCentroid += bestDist;
      distToOtherCentroids -= bestDist;
      numDocs++;

      documentCentroids[values.docID()] = bestCentroid;
    }

    for (int c = 0; c < newCentroids.length; c++) {
      for (int v = 0; v < newCentroids[c].length; v++) {
        newCentroids[c][v] /= newCentroidSize[c];
      }
    }

    distToCentroid /= numDocs;
    distToOtherCentroids /= numDocs * (centroids.length - 1);

    System.out.println("Finished iteration [" + iter + "]. Dist to centroid [" + distToCentroid +
        "], dist to other centroids [" + distToOtherCentroids + "].");
    return newCentroids;
  }

  public static class ClusterCentroids {
    private final float[][] centroids;
    private final BytesRef[] encodedCentroids;

    ClusterCentroids(float[][] centroids) {
      this.centroids = centroids;

      this.encodedCentroids = new BytesRef[centroids.length];
      for (int c = 0; c < centroids.length; c++) {
        this.encodedCentroids[c] = VectorField.encode(centroids[c]);
      }
    }

    public BytesRef findClosestCentroid(BinaryDocValues values) throws IOException {
      BytesRef bytes = values.binaryValue();
      float[] vector = VectorValues.decode(bytes);

      double bestDist = Double.POSITIVE_INFINITY;
      int bestCentroid = -1;

      for (int c = 0; c < centroids.length; c++) {
        double dist = VectorValues.l2norm(vector, centroids[c]);
        if (dist < bestDist) {
          bestDist = dist;
          bestCentroid = c;
        }
      }
      return encodedCentroids[bestCentroid];
    }
  }


}
