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
import java.util.ArrayList;

import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class GetClusterEvaluator extends RecursiveObjectEvaluator implements TwoValueWorker {
  private static final long serialVersionUID = 1;

  public GetClusterEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
  }

  @Override
  public Object doWork(Object value1, Object value2) throws IOException {
    if(!(value1 instanceof KmeansEvaluator.ClusterTuple)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for value, expecting a cluster result.",toExpression(constructingFactory), value1.getClass().getSimpleName()));
    } else {

      KmeansEvaluator.ClusterTuple clusterTuple = (KmeansEvaluator.ClusterTuple)value1;
      List<CentroidCluster<KmeansEvaluator.ClusterPoint>> clusters = clusterTuple.getClusters();

      Number index = (Number)value2;
      @SuppressWarnings({"rawtypes"})
      CentroidCluster cluster = clusters.get(index.intValue());
      @SuppressWarnings({"rawtypes"})
      List points = cluster.getPoints();
      List<String> rowLabels = new ArrayList<>();
      double[][] data = new double[points.size()][];

      for(int i=0; i<points.size(); i++) {
        KmeansEvaluator.ClusterPoint p = (KmeansEvaluator.ClusterPoint)points.get(i);
        data[i] = p.getPoint();
        rowLabels.add(p.getId());
      }

      Matrix matrix = new Matrix(data);
      matrix.setRowLabels(rowLabels);
      matrix.setColumnLabels(clusterTuple.getColumnLabels());
      return matrix;
    }
  }
}