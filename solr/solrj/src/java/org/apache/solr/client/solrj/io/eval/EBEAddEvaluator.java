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
import java.util.Locale;

import org.apache.commons.math3.util.MathArrays;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class EBEAddEvaluator extends RecursiveObjectEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;

  public EBEAddEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Object doWork(Object first, Object second) throws IOException{
    if(null == first){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null found for the first value",toExpression(constructingFactory)));
    }
    if(null == second){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null found for the second value",toExpression(constructingFactory)));
    }

    if(first instanceof List && second instanceof List) {
      double[] result = MathArrays.ebeAdd(
          ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray(),
          ((List) second).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray()
      );

      List<Number> numbers = new ArrayList<>();
      for (double d : result) {
        numbers.add(d);
      }

      return numbers;
    } else if(first instanceof Matrix && second instanceof Matrix) {
      double[][] data1 = ((Matrix) first).getData();
      double[][] data2 = ((Matrix) second).getData();
      Array2DRowRealMatrix matrix1 = new Array2DRowRealMatrix(data1, false);
      Array2DRowRealMatrix matrix2 = new Array2DRowRealMatrix(data2, false);
      Array2DRowRealMatrix matrix3 = matrix1.add(matrix2);
      return new Matrix(matrix3.getDataRef());
    } else {
      throw new IOException("Parameters for ebeAdd must either be two numeric arrays or two matrices. ");
    }
  }
}
