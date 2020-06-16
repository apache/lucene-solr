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

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ScalarAddEvaluator extends RecursiveObjectEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;

  public ScalarAddEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(2 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expects exactly 2 values but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object value1, Object value2) throws IOException{

    double d = ((Number)value1).doubleValue();
    if(value2 instanceof List){
      @SuppressWarnings({"unchecked"})
      List<Number> nums = (List<Number>)value2;
      List<Number> out = new ArrayList<>();
      for(Number num : nums) {
        out.add(operate(num.doubleValue(), d));
      }

      return out;

    } else if (value2 instanceof Matrix) {
      Matrix matrix = (Matrix) value2;
      double[][] data = matrix.getData();
      double[][] newData = new double[data.length][];
      for(int i=0; i<data.length; i++) {
        double[] row = data[i];
        double[] newRow = new double[row.length];

        for(int j=0; j<row.length; j++) {
          newRow[j] = operate(row[j], d);
        }

        newData[i] = newRow;
      }
      return new Matrix(newData);
    } else {
      throw new IOException("scalar add, subtract, multiply and divide operate on numeric arrays and matrices only.");
    }
  }

  protected double operate(double value, double d) {
    return value+d;
  }
}
