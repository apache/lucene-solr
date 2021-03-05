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
import java.util.Locale;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class MatrixMultiplyEvaluator extends RecursiveObjectEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;

  public MatrixMultiplyEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object first, Object second) throws IOException {
    if(null == first){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null found for the first value",toExpression(constructingFactory)));
    }
    if(null == second){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null found for the second value",toExpression(constructingFactory)));
    }

    Array2DRowRealMatrix realMatrix1 = getMatrix(first);
    Array2DRowRealMatrix realMatrix2 = getMatrix(second);
    Array2DRowRealMatrix realMatrix3 = realMatrix1.multiply(realMatrix2);
    return new Matrix(realMatrix3.getDataRef());

  }

  private Array2DRowRealMatrix getMatrix(Object o) throws IOException {
    if(o instanceof Matrix) {
      Matrix matrix = (Matrix)o;
      return new Array2DRowRealMatrix(matrix.getData(), false);
    } else if(o instanceof List) {
      @SuppressWarnings({"unchecked"})
      List<Number> vec = (List<Number>)o;
      double[][] data1 = new double[1][vec.size()];
      for(int i=0; i<vec.size(); i++) {
        data1[0][i] = vec.get(i).doubleValue();
      }
      return new Array2DRowRealMatrix(data1, false);
    } else {
      throw new IOException("The matrixMult function can only be applied to numeric arrays and matrices:"+o.getClass().toString());
    }
  }
}
