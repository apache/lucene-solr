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

package org.apache.lucene.util.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.ivfflat.ImmutableClusterableVector;

/**
 * Migrate from {@link org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer}
 * with minor refactoring, thereby avoid introducing external dependencies.
 */
public class KMeansCluster<T extends Clusterable> implements Clusterer<T> {
  private static final int MAX_KMEANS_ITERATIONS = 1000;

  private static final int MIN_KMEANS_K = 200;

  private int maxIterations;

  private int k;

  private Random random;

  DistanceMeasure distanceMeasure;

  public KMeansCluster(VectorValues.DistanceFunction distFunc) {
    this(MAX_KMEANS_ITERATIONS, MIN_KMEANS_K, distFunc);
  }

  public KMeansCluster(int k, VectorValues.DistanceFunction distFunc) {
    this(MAX_KMEANS_ITERATIONS, k, distFunc);
  }

  public KMeansCluster(int maxIterations, int k, VectorValues.DistanceFunction distFunc) {
    this.maxIterations = maxIterations;
    this.k = k;
    this.distanceMeasure = DistanceFactory.instance(distFunc);
    this.random = new Random();
  }

  @Override
  public List<Centroid<T>> cluster(Collection<T> points) throws NoSuchElementException {
    assert !points.isEmpty();

    /// adaptive choosing value for k
    this.k = (int) Math.max(this.k, Math.sqrt(points.size()));

    if (points.size() < this.k) {
      return Collections.EMPTY_LIST;
    } else {
      List<Centroid<T>> clusters = this.initCenters(points);
      int[] assignments = new int[points.size()];
      this.assignPointsToClusters(clusters, points, assignments);
      int max = this.maxIterations < 0 ? 2147483647 : this.maxIterations;

      for (int count = 0; count < max; ++count) {
        boolean emptyCluster = false;
        List<Centroid<T>> newClusters = new ArrayList<>();

        Clusterable newCenter;
        for (Iterator<Centroid<T>> i$ = clusters.iterator(); i$.hasNext();
             newClusters.add(new Centroid<>(newCenter))) {
          final Centroid<T> cluster = i$.next();
          if (cluster.getPoints().isEmpty()) {
            newCenter = this.getPointFromLargestNumberCluster(clusters);
            emptyCluster = true;
          } else {
            newCenter = this.centroidOf(cluster.getPoints(), cluster.getCenter().getPoint().length);
          }
        }

        int changes = this.assignPointsToClusters(newClusters, points, assignments);
        clusters = newClusters;
        if (changes == 0 && !emptyCluster) {
          return newClusters;
        }
      }

      return clusters;
    }
  }

  private List<Centroid<T>> initCenters(Collection<T> points) {
    final List<T> pointList = List.copyOf(points);
    int numPoints = pointList.size();
    boolean[] visited = new boolean[numPoints];
    List<Centroid<T>> resultSet = new ArrayList<>(this.k);
    int firstPointIndex = this.random.nextInt(numPoints);
    T firstPoint = pointList.get(firstPointIndex);
    resultSet.add(new Centroid<>(firstPoint));
    visited[firstPointIndex] = true;
    double[] minDistSquared = new double[numPoints];

    for (int i = 0; i < numPoints; ++i) {
      if (i != firstPointIndex) {
        double d = this.distance(firstPoint, pointList.get(i));
        minDistSquared[i] = d * d;
      }
    }

    while (resultSet.size() < this.k) {
      double distSqSum = 0.0D;

      for (int i = 0; i < numPoints; ++i) {
        if (!visited[i]) {
          distSqSum += minDistSquared[i];
        }
      }

      double r = this.random.nextDouble() * distSqSum;
      int nextPointIndex = -1;
      double sum = 0.0D;

      int i;
      for (i = 0; i < numPoints; ++i) {
        if (!visited[i]) {
          sum += minDistSquared[i];
          if (sum >= r) {
            nextPointIndex = i;
            break;
          }
        }
      }

      if (nextPointIndex == -1) {
        for (i = numPoints - 1; i >= 0; --i) {
          if (!visited[i]) {
            nextPointIndex = i;
            break;
          }
        }
      }

      if (nextPointIndex < 0) {
        break;
      }

      T p = pointList.get(nextPointIndex);
      resultSet.add(new Centroid<>(p));
      visited[nextPointIndex] = true;
      if (resultSet.size() < this.k) {
        for (int j = 0; j < numPoints; ++j) {
          if (!visited[j]) {
            double d = this.distance(p, pointList.get(j));
            double d2 = d * d;
            if (d2 < minDistSquared[j]) {
              minDistSquared[j] = d2;
            }
          }
        }
      }
    }

    return resultSet;
  }

  private int assignPointsToClusters(List<Centroid<T>> clusters, final Collection<T> points, int[] assignments) {
    int assignedDifferently = 0;
    int pointIndex = 0;

    int clusterIndex;
    for (Iterator<T> i$ = points.iterator(); i$.hasNext(); assignments[pointIndex++] = clusterIndex) {
      T p = i$.next();
      clusterIndex = this.getNearestCluster(clusters, p);
      if (clusterIndex != assignments[pointIndex]) {
        ++assignedDifferently;
      }

      Centroid<T> cluster = clusters.get(clusterIndex);
      cluster.addPoint(p);
    }

    return assignedDifferently;
  }

  private int getNearestCluster(final Collection<Centroid<T>> clusters, T point) {
    double minDistance = 1.7976931348623157E308D;
    int clusterIndex = 0, minCluster = 0;

    for (Iterator<Centroid<T>> i$ = clusters.iterator(); i$.hasNext(); ++clusterIndex) {
      double distance = this.distance(point, i$.next().getCenter());
      if (distance < minDistance) {
        minDistance = distance;
        minCluster = clusterIndex;
      }
    }

    return minCluster;
  }

  private Clusterable getPointFromLargestNumberCluster(Collection<? extends Cluster<T>> clusters) throws NoSuchElementException {
    int maxNumber = 0;
    Cluster<T> selected = null;

    for (Cluster<T> cluster : clusters) {
      int number = cluster.getPoints().size();
      if (number > maxNumber) {
        maxNumber = number;
        selected = cluster;
      }
    }

    if (selected == null) {
      throw new NoSuchElementException("Cannot find point from largest number cluster");
    } else {
      final List<T> selectedPoints = selected.getPoints();
      return selectedPoints.remove(this.random.nextInt(selectedPoints.size()));
    }
  }

  private Clusterable centroidOf(Collection<T> points, int dimension) {
    float[] centroid = new float[dimension];

    for (T p : points) {
      float[] point = p.getPoint();

      for (int i = 0; i < centroid.length; ++i) {
        centroid[i] += point[i];
      }
    }

    for (int i = 0; i < centroid.length; ++i) {
      centroid[i] /= (float) points.size();
    }

    return new ImmutableClusterableVector(0, centroid);
  }

  @Override
  public double distance(Clusterable p1, Clusterable p2) {
    return this.distanceMeasure.compute(p1.getPoint(), p2.getPoint());
  }

  public int getK() {
    return k;
  }

  public int getMaxIterations() {
    return maxIterations;
  }

  public DistanceMeasure getDistanceMeasure() {
    return distanceMeasure;
  }
}
