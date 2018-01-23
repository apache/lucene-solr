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

package org.apache.solr.client.solrj.io.eval;

import java.io.IOException;
import java.util.Locale;

import java.util.List;

import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class GetCentroidsEvaluator extends RecursiveObjectEvaluator implements OneValueWorker {
  private static final long serialVersionUID = 1;

  public GetCentroidsEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
  }

  @Override
  public Object doWork(Object value) throws IOException {
    if(!(value instanceof KmeansEvaluator.ClusterTuple)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for value, expecting a clustering result",toExpression(constructingFactory), value.getClass().getSimpleName()));
    } else {
      KmeansEvaluator.ClusterTuple clusterTuple = (KmeansEvaluator.ClusterTuple)value;
      List<CentroidCluster<KmeansEvaluator.ClusterPoint>> clusters = clusterTuple.getClusters();
      double[][] data = new double[clusters.size()][];
      for(int i=0; i<clusters.size(); i++) {
        CentroidCluster<KmeansEvaluator.ClusterPoint> centroidCluster = clusters.get(i);
        Clusterable clusterable = centroidCluster.getCenter();
        data[i] = clusterable.getPoint();
      }
      Matrix centroids = new Matrix(data);
      centroids.setColumnLabels(clusterTuple.getColumnLabels());
      return centroids;
    }
  }
}