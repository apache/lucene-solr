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

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * The LatLonVectorsEvaluator maps to the latlonVectors Math Expression. The latlonVectors expression
 * takes a list of Tuples that contain a lat,lon point field and a named parameter called field
 * as input. The field parameter specifies which field to parse the lat,lon vectors from in the Tuples.
 *
 * The latlonVectors function returns a matrix of lat,lon vectors. Each row in the matrix
 * contains a single lat,lon pair.
 *
 **/

public class LatLonVectorsEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  private String field;

  public LatLonVectorsEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);

    for (StreamExpressionNamedParameter namedParam : namedParams) {
      if(namedParam.getName().equals("field")) {
        this.field = namedParam.getParameter().toString();
      } else {
        throw new IOException("Unexpected named parameter:" + namedParam.getName());
      }
    }

    if(field == null) {
      throw new IOException("The named parameter \"field\" must be set for the latlonVectors function.");
    }
  }

  @Override
  public Object doWork(Object... objects) throws IOException {

    if (objects.length == 1) {
      //Just docs
      if(!(objects[0] instanceof List)) {
        throw new IOException("The latlonVectors function expects a list of Tuples as a parameter.");
      } else {
        @SuppressWarnings({"rawtypes"})
        List list = (List)objects[0];
        if(list.size() > 0) {
          Object o = list.get(0);
          if(!(o instanceof Tuple)) {
            throw new IOException("The latlonVectors function expects a list of Tuples as a parameter.");
          }
        } else {
          throw new IOException("Empty list was passed as a parameter to termVectors function.");
        }
      }

      @SuppressWarnings({"unchecked"})
      List<Tuple> tuples = (List<Tuple>) objects[0];

      double[][] locationVectors = new double[tuples.size()][2];
      List<String> features = new ArrayList<>();
      features.add("lat");
      features.add("lon");

      List<String> rowLabels = new ArrayList<>();

      for(int i=0; i< tuples.size(); i++) {
        Tuple tuple = tuples.get(i);
        String value = tuple.getString(field);
        String[] latLong = null;
        if(value.contains(",")) {
          latLong = value.split(",");
        } else {
          latLong = value.split(" ");
        }

        locationVectors[i][0] = Double.parseDouble(latLong[0].trim());
        locationVectors[i][1] = Double.parseDouble(latLong[1].trim());
        if(tuple.get("id") != null) {
          rowLabels.add(tuple.get("id").toString());
        }
      }

      Matrix matrix = new Matrix(locationVectors);
      matrix.setColumnLabels(features);
      matrix.setRowLabels(rowLabels);
      return matrix;
    } else {
      throw new IOException("The latlonVectors function takes a single positional parameter.");
    }
  }
}
