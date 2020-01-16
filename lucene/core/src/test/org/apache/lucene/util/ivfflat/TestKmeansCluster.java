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

package org.apache.lucene.util.ivfflat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.cluster.Centroid;
import org.apache.lucene.util.cluster.KMeansCluster;

public class TestKmeansCluster extends LuceneTestCase {
  private Random random = new Random();

  private static final int dims = 256;

  private static final int dataSize = 10000;

  public void testCluster() {
    final List<float[]> vectors = randomVectors(dims, dataSize);
    assertEquals(dataSize, vectors.size());

    final List<ImmutableClusterableVector> clusterableVectors = new ArrayList<>(dataSize);
    int counter = 0;
    for (float[] vector : vectors) {
      clusterableVectors.add(new ImmutableClusterableVector(counter++, vector));
    }

    KMeansCluster<ImmutableClusterableVector> cluster = new KMeansCluster<>(VectorValues.DistanceFunction.EUCLIDEAN);

    long startTime = System.currentTimeMillis();
    List<Centroid<ImmutableClusterableVector>> centroids = cluster.cluster(clusterableVectors);
    long costTime = System.currentTimeMillis() - startTime;

    System.out.println("cluster cost -> " + costTime + " msec");

    assertEquals(cluster.getK(), centroids.size());
  }

  private List<float[]> randomVectors(int dims, int totalCnt) {
    List<float[]> vectors = new ArrayList<>(totalCnt);
    for (int i = 0; i < totalCnt; ++i) {
      vectors.add(randomVector(dims));
    }

    return vectors;
  }

  private float[] randomVector(int dims) {
    float[] vector = new float[dims];
    for(int i =0; i < dims; i++) {
      vector[i] = random.nextFloat();
    }

    return vector;
  }
}
