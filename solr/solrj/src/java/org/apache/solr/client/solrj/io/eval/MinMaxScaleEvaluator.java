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

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class MinMaxScaleEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public MinMaxScaleEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object... values) throws IOException {

    if(null == values){
      return null;
    }

    double min = 0;
    double max = 1;

    if(values.length == 3) {
      min = ((Number)values[1]).doubleValue();
      max = ((Number)values[2]).doubleValue();
    }

    if(values[0] instanceof Matrix) {
      Matrix matrix = (Matrix)values[0];
      double[][] data = matrix.getData();
      double[][] scaled = new double[data.length][];
      for(int i=0; i<scaled.length; i++) {
        double[] row = data[i];
        scaled[i] = scale(row, min, max);
      }

      return new Matrix(scaled);

    } else if(values[0] instanceof List) {
      @SuppressWarnings({"unchecked"})
      List<Number> vec = (List)values[0];
      double[] data = new double[vec.size()];

      for(int i=0; i<vec.size(); i++) {
        data[i] = vec.get(i).doubleValue();
      }

      data = scale(data, min, max);
      List<Number> scaled = new ArrayList<>(data.length);
      for(double d : data) {
        scaled.add(d);
      }

      return scaled;
    } else {
      throw new IOException();
    }
  }

  public static double[] scale(double[] values, double min, double max) {

    double localMin = Double.MAX_VALUE;
    double localMax = Double.MIN_VALUE;
    for (double d : values) {
      if (d > localMax) {
        localMax = d;
      }

      if (d < localMin) {
        localMin = d;
      }
    }

    //First scale between 0 and 1

    double[] scaled = new double[values.length];

    for (int i = 0; i < scaled.length; i++) {
      double x = values[i];
      double s = (x - localMin) / (localMax - localMin);
      scaled[i] = s;
    }

    if (min != 0 || max != 1) {
      //Next scale between specific min/max
      double scale = max - min;

      for (int i = 0; i < scaled.length; i++) {
        double d = scaled[i];
        scaled[i] = (scale * d) + min;
      }
    }

    return scaled;
  }
}
