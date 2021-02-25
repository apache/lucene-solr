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
import java.util.HashSet;
import java.util.Locale;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;

public class FeatureSelectEvaluator extends RecursiveObjectEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;

  public FeatureSelectEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(2 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting exactly 2 values but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object value1, Object value2) throws IOException {

    if(value1 instanceof Matrix) {
      Matrix matrix = (Matrix) value1;
      double[][] data = matrix.getData();

      List<String> labels = matrix.getColumnLabels();
      Set<String> features = new HashSet<>();
      loadFeatures(value2, features);

      List<String> newColumnLabels = new ArrayList<>();

      for(String label : labels) {
        if(features.contains(label)) {
         newColumnLabels.add(label);
        }
      }

      double[][] selectFeatures = new double[data.length][newColumnLabels.size()];

      for(int i=0; i<data.length; i++) {
        double[] currentRow = data[i];
        double[] newRow = new double[newColumnLabels.size()];

        int index = -1;
        for(int l=0; l<currentRow.length; l++) {
          String label = labels.get(l);
          if(features.contains(label)) {
            newRow[++index] = currentRow[l];
          }
        }
        selectFeatures[i] = newRow;
      }

      Matrix newMatrix = new Matrix(selectFeatures);
      newMatrix.setRowLabels(matrix.getRowLabels());
      newMatrix.setColumnLabels(newColumnLabels);
      return newMatrix;
    } else {
      throw new IOException("The featureSelect function expects a matrix as a parameter");
    }
  }

  private void loadFeatures(Object o, Set<String> features) {
    @SuppressWarnings({"rawtypes"})
    List list = (List)o;
    for(Object v : list) {
      if(v instanceof List) {
        loadFeatures(v, features);
      } else {
        features.add((String)v);
      }
    }
  }
}
