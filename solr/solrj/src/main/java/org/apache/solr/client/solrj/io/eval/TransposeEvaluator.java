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

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class TransposeEvaluator extends RecursiveObjectEvaluator implements OneValueWorker {
  protected static final long serialVersionUID = 1L;

  public TransposeEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(1 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting exactly 1 value but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object value) throws IOException{
    if(null == value){
      return null;
    } else if(value instanceof Matrix) {
      Matrix matrix = (Matrix) value;
      double[][] data = matrix.getData();
      Array2DRowRealMatrix amatrix = new Array2DRowRealMatrix(data, false);
      Array2DRowRealMatrix tmatrix = (Array2DRowRealMatrix)amatrix.transpose();
      Matrix newMatrix = new Matrix(tmatrix.getDataRef());
      //Switch the row and column labels
      newMatrix.setColumnLabels(matrix.getRowLabels());
      newMatrix.setRowLabels(matrix.getColumnLabels());
      return newMatrix;
    } else {
      throw new IOException("matrix parameter expected for transpose function");
    }
  }
}
