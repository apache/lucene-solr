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
import java.util.List;

import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.correlation.Covariance;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class CovarianceEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public CovarianceEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object ... values) throws IOException{

    if(values.length == 2) {
      Object first = values[0];
      Object second = values[1];
      Covariance covariance = new Covariance();

      return covariance.covariance(
          ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray(),
          ((List) second).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray()
      );
    } else if(values.length == 1) {
      Matrix matrix = (Matrix) values[0];
      double[][] data = matrix.getData();
      Covariance covariance = new Covariance(data, true);
      RealMatrix coMatrix = covariance.getCovarianceMatrix();
      double[][] coData = coMatrix.getData();
      Matrix realMatrix = new Matrix(coData);
      List<String> labels = CorrelationEvaluator.getColumnLabels(matrix.getColumnLabels(), coData.length);
      realMatrix.setColumnLabels(labels);
      realMatrix.setRowLabels(labels);
      return realMatrix;
    } else {
      throw new IOException("The cov function expects either two numeric arrays or a matrix as parameters.");
    }
  }
}
