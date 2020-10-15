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

import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.clustering.MultiKMeansPlusPlusClusterer;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class MultiKmeansEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  private int maxIterations = 1000;

  public MultiKmeansEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);

    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(namedParam.getName().equals("maxIterations")) {
        this.maxIterations = Integer.parseInt(namedParam.getParameter().toString().trim());
      } else {
        throw new IOException("Unexpected named parameter:"+namedParam.getName());
      }
    }
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Object doWork(Object... values) throws IOException {

    if(values.length != 3) {
      throw new IOException("The multiKmeans function expects three parameters; a matrix to cluster, k and number of trials.");
    }

    Object value1 = values[0];
    Object value2 = values[1];
    Object value3 = values[2];

    Matrix matrix = null;
    int k = 0;
    int trials=0;

    if(value1 instanceof Matrix) {
      matrix = (Matrix)value1;
    } else {
      throw new IOException("The first parameter for multiKmeans should be the observation matrix.");
    }

    if(value2 instanceof Number) {
      k = ((Number)value2).intValue();
    } else {
      throw new IOException("The second parameter for multiKmeans should be k.");
    }

    if(value3 instanceof Number) {
      trials= ((Number)value3).intValue();
    } else {
      throw new IOException("The third parameter for multiKmeans should be trials.");
    }

    @SuppressWarnings({"rawtypes"})
    KMeansPlusPlusClusterer<KmeansEvaluator.ClusterPoint> kmeans = new KMeansPlusPlusClusterer(k, maxIterations);
    @SuppressWarnings({"rawtypes"})
    MultiKMeansPlusPlusClusterer multiKmeans = new MultiKMeansPlusPlusClusterer(kmeans, trials);

    List<KmeansEvaluator.ClusterPoint> points = new ArrayList<>();
    double[][] data = matrix.getData();

    List<String> ids = matrix.getRowLabels();

    for(int i=0; i<data.length; i++) {
      double[] vec = data[i];
      points.add(new KmeansEvaluator.ClusterPoint(ids.get(i), vec));
    }

    @SuppressWarnings({"rawtypes"})
    Map fields = new HashMap();

    fields.put("k", k);
    fields.put("trials", trials);
    fields.put("distance", "euclidean");
    fields.put("maxIterations", maxIterations);

    return new KmeansEvaluator.ClusterTuple(fields, multiKmeans.cluster(points), matrix.getColumnLabels());
  }

}

