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
import java.util.ArrayList;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class TimeDifferencingEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker{

  protected static final long serialVersionUID = 1L;

  public TimeDifferencingEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
    if (!(1 == containedEvaluators.size() ||  containedEvaluators.size() == 2)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting one or two values but found %d",expression, containedEvaluators.size()));
    }
  }
  @Override
  public Object doWork(Object... values) throws IOException {
    if (!(1 == values.length || values.length == 2)) {
      throw new IOException(String.format(Locale.ROOT, "%s(...) only works with 1 or 2 values but %d were provided", constructingFactory.getFunctionName(getClass()), values.length));
    }
    if (values[0] instanceof List) {
      @SuppressWarnings({"unchecked"})
      List<Number> timeseriesValues = (List<Number>) values[0];
      Number lagValue = 1;

      if (1 == values.length) {
        if (!(timeseriesValues instanceof List<?>)) {
          throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the first value, expecting a List", toExpression(constructingFactory), values[0].getClass().getSimpleName()));
        }
        if (!(timeseriesValues.size() > 1)) {
          throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found list size of %s for the first value, expecting a List of size > 0.", toExpression(constructingFactory), timeseriesValues.size()));
        }
      }
      if (2 == values.length) {
        lagValue = (Number) values[1];
        if (!(lagValue instanceof Number)) {
          throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the second value, expecting a Number", toExpression(constructingFactory), values[1].getClass().getSimpleName()));
        }
        if (lagValue.intValue() > timeseriesValues.size()) {
          throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found a lag size of %s for the second value, the first value has a List size of %s, expecting a lag value less than the List size", toExpression(constructingFactory), lagValue.intValue(), timeseriesValues.size()));
        }
      }
      final int lag = lagValue.intValue();
      return IntStream.range(lag, timeseriesValues.size())
          .mapToObj(n -> (timeseriesValues.get(n).doubleValue() - timeseriesValues.get(n - lag).doubleValue()))
          .collect(Collectors.toList());
    } else if(values[0] instanceof Matrix) {

      //Diff each row of the matrix

      Matrix matrix = (Matrix)values[0];
      double[][] data = matrix.getData();
      double[][] diffedData = new double[data.length][];
      Number lagValue = 1;

      if (2 == values.length) {
        lagValue = (Number) values[1];
        if (!(lagValue instanceof Number)) {
          throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the second value, expecting a Number", toExpression(constructingFactory), values[1].getClass().getSimpleName()));
        }
      }

      int lag = lagValue.intValue();

      for(int i=0; i<data.length; i++) {
        double[] row = data[i];
        List<Double> timeseriesValues = new ArrayList<>(row.length);
        for(double d : row) {
          timeseriesValues.add(d);
        }

        List<Number> diffedList = IntStream.range(lag, timeseriesValues.size())
            .mapToObj(n -> (timeseriesValues.get(n).doubleValue() - timeseriesValues.get(n - lag).doubleValue()))
            .collect(Collectors.toList());
        double[] diffedRow = new double[diffedList.size()];
        for(int r=0; r<diffedList.size(); r++) {
          diffedRow[r] = diffedList.get(r).doubleValue();
        }
        diffedData[i] = diffedRow;
      }

      Matrix diffedMatrix = new Matrix(diffedData);
      diffedMatrix.setRowLabels(matrix.getRowLabels());
      List<String> columns = matrix.getColumnLabels();
      if(columns != null) {
        List<String> newColumns = new ArrayList<>(columns.size() - lag);

        for (int i = lag; i < columns.size(); i++) {
          newColumns.add(columns.get(i));
        }

        diffedMatrix.setColumnLabels(newColumns);
      }
      return diffedMatrix;

    } else {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - first parameter must be list of matrix", toExpression(constructingFactory)));
    }
  }
}
