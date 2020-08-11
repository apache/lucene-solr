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

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class SumColumnsEvaluator extends RecursiveObjectEvaluator implements OneValueWorker {
  protected static final long serialVersionUID = 1L;

  public SumColumnsEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(1 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting exactly 1 value but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object value) throws IOException{
    if(null == value){
      return null;
    } else if (value instanceof Matrix) {

      //First transpose the matrix
      Matrix matrix = (Matrix) value;
      double[][] data = matrix.getData();
      RealMatrix realMatrix = new Array2DRowRealMatrix(data, false);

      List<Number> sums = new ArrayList<>(data[0].length);

      for(int i=0; i<data[0].length; i++) {
        double sum = 0;
        double[] col = realMatrix.getColumn(i);
        for(int j=0; j<col.length; j++){
          sum+=col[j];
        }

        sums.add(sum);
      }

      return sums;
    } else {
      throw new IOException("Grand sum function only operates on a matrix");
    }
  }
}
