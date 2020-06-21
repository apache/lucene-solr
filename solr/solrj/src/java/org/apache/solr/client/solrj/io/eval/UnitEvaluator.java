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

import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import java.util.List;
import java.util.ArrayList;

public class UnitEvaluator extends RecursiveObjectEvaluator implements OneValueWorker {
  protected static final long serialVersionUID = 1L;

  public UnitEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
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
      double[][] unitData = new double[data.length][];
      for(int i=0; i<data.length; i++) {
        double[] row = data[i];
        ArrayRealVector vector = new ArrayRealVector(row);
        double[] unitRow = vector.unitVector().toArray();
        unitData[i] = unitRow;
      }

      Matrix m = new Matrix(unitData);
      m.setRowLabels(matrix.getRowLabels());
      m.setColumnLabels(matrix.getColumnLabels());
      return m;
    } else if(value instanceof List) {
      @SuppressWarnings({"unchecked"})
      List<Number> values = (List<Number>)value;
      double[] doubles = new double[values.size()];
      for(int i=0; i<doubles.length; i++) {
       doubles[i] = values.get(i).doubleValue();
      }

      ArrayRealVector vector = new ArrayRealVector(doubles);
      RealVector unitVector = vector.unitVector();
      List<Number> unitList = new ArrayList<>(doubles.length);
      double[] unitArray = unitVector.toArray();
      for(double d : unitArray) {
        unitList.add(d);
      }

      return unitList;
    } else {
      throw new IOException("The unit function expects either a numeric array or matrix as a parameter");
    }
  }
}
