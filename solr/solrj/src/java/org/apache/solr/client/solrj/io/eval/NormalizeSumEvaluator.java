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

import org.apache.commons.math3.util.MathArrays;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import java.util.List;
import java.util.ArrayList;

public class NormalizeSumEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public NormalizeSumEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(2 < containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at most two parameters but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object... values) throws IOException{

    Object value = values[0];

    double sumTo = 1.0;

    if(values.length == 2) {
      Number n = (Number)values[1];
      sumTo = n.doubleValue();
    }

    if(null == value){
      return null;
    } else if(value instanceof Matrix) {
      Matrix matrix = (Matrix) value;

      double[][] data = matrix.getData();
      double[][] unitData = new double[data.length][];
      for(int i=0; i<data.length; i++) {
        double[] row = data[i];
        double[] unitRow = MathArrays.normalizeArray(row, sumTo);
        unitData[i] = unitRow;
      }

      Matrix m = new Matrix(unitData);
      m.setRowLabels(matrix.getRowLabels());
      m.setColumnLabels(matrix.getColumnLabels());
      return m;
    } else if(value instanceof List) {
      @SuppressWarnings({"unchecked"})
      List<Number> vals = (List<Number>)value;
      double[] doubles = new double[vals.size()];
      for(int i=0; i<doubles.length; i++) {
        doubles[i] = vals.get(i).doubleValue();
      }

      List<Number> unitList = new ArrayList<>(doubles.length);
      double[] unitArray = MathArrays.normalizeArray(doubles, sumTo);
      for(double d : unitArray) {
        unitList.add(d);
      }

      return unitList;
    } else {
      throw new IOException("The unit function expects either a numeric array or matrix as a parameter");
    }
  }
}
