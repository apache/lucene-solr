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
import java.util.Locale;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map;
import java.util.HashMap;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.Tuple;

public class PivotEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public  PivotEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(4 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting exactly 4 values but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object... values) throws IOException {
    if(values.length != 4) {
      throw new IOException("The pivot function requires four parameters.");
    }

    Object value1 = values[0];
    Object value2 = values[1];
    Object value3 = values[2];
    Object value4 = values[3];

    if(value1 instanceof List) {
      @SuppressWarnings({"unchecked"})
      List<Tuple> tuples = (List<Tuple>)value1;
      String x = (String)value2;
      x = x.replace("\"", "");
      String y = (String)value3;
      y = y.replace("\"", "");

      String vlabel = (String)value4;
      vlabel = vlabel.replace("\"", "");

      Set<String> xset = new TreeSet<>();
      Set<String> yset = new TreeSet<>();

      for(int i=0; i<tuples.size(); i++) {
        Tuple tuple = tuples.get(i);
        xset.add(tuple.getString(x));
        yset.add(tuple.getString(y));
      }

      double[][] data = new double[xset.size()][yset.size()];

      List<String> xlabels = new ArrayList<>(xset.size());
      Map<String, Integer> xindexes = new HashMap<>();
      int xindex = 0;
      for (String xlabel :xset) {
        xlabels.add(xlabel);
        xindexes.put(xlabel, xindex);
        ++xindex;
      }

      List<String> ylabels = new ArrayList<>(yset.size());
      Map<String, Integer> yindexes = new HashMap<>();
      int yindex = 0;
      for (String ylabel : yset) {
        ylabels.add(ylabel);
        yindexes.put(ylabel, yindex);
        ++yindex;
      }

      for(Tuple tuple : tuples) {
        String xlabel = tuple.getString(x);
        String ylabel = tuple.getString(y);
        int xi = xindexes.get(xlabel);
        int yi = yindexes.get(ylabel);
        double val = tuple.getDouble(vlabel);
        data[xi][yi] = val;
      }

      Matrix matrix = new Matrix(data);
      matrix.setRowLabels(xlabels);
      matrix.setColumnLabels(ylabels);
      return matrix;
    } else {
      throw new IOException("The getValue function expects a list of tuples as the first parameter");
    }
  }
}
