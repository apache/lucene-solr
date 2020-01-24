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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.ml.clustering.FuzzyKMeansClusterer;
import org.apache.solr.client.solrj.io.stream.ZplotStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class FuzzyKmeansEvaluator extends RecursiveObjectEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;


  private int maxIterations = 1000;
  private double fuzziness = 1.2;

  public FuzzyKmeansEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);

    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(namedParam.getName().equals("fuzziness")){
        this.fuzziness = Double.parseDouble(namedParam.getParameter().toString().trim());
      } else if(namedParam.getName().equals("maxIterations")) {
        this.maxIterations = Integer.parseInt(namedParam.getParameter().toString().trim());
      } else {
        throw new IOException("Unexpected named parameter:"+namedParam.getName());
      }
    }
  }

  @Override
  public Object doWork(Object value1, Object value2) throws IOException {


    Matrix matrix = null;
    int k = 0;


    if(value1 instanceof Matrix) {
      matrix = (Matrix)value1;
    } else {
      throw new IOException("The first parameter for fuzzyKmeans should be the observation matrix.");
    }

    if(value2 instanceof Number) {
      k = ((Number)value2).intValue();
    } else {
      throw new IOException("The second parameter for fuzzyKmeans should be k.");
    }

    FuzzyKMeansClusterer<KmeansEvaluator.ClusterPoint> kmeans = new FuzzyKMeansClusterer(k,
                                                                                         fuzziness,
                                                                                         maxIterations,
                                                                                         new EuclideanDistance());
    List<KmeansEvaluator.ClusterPoint> points = new ArrayList();
    double[][] data = matrix.getData();

    List<String> ids = matrix.getRowLabels();

    for(int i=0; i<data.length; i++) {
      double[] vec = data[i];
      points.add(new KmeansEvaluator.ClusterPoint(ids.get(i), vec));
    }

    Map fields = new HashMap();

    fields.put("k", k);
    fields.put("fuzziness", fuzziness);
    fields.put("distance", "euclidean");
    fields.put("maxIterations", maxIterations);

    List<CentroidCluster<KmeansEvaluator.ClusterPoint>> clusters = kmeans.cluster(points);
    RealMatrix realMatrix = kmeans.getMembershipMatrix();
    double[][] mmData = realMatrix.getData();
    Matrix mmMatrix = new Matrix(mmData);
    mmMatrix.setRowLabels(matrix.getRowLabels());
    List<String> clusterCols = new ArrayList();
    for(int i=0; i<clusters.size(); i++) {
      clusterCols.add("cluster"+ ZplotStream.pad(Integer.toString(i), clusters.size()));
    }
    mmMatrix.setRowLabels(clusterCols);
    return new KmeansEvaluator.ClusterTuple(fields, clusters, matrix.getColumnLabels(),mmMatrix);
  }
}

