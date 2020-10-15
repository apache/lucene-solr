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

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.correlation.KendallsCorrelation;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;

import org.apache.solr.client.solrj.io.stream.ZplotStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class CorrelationEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public enum CorrelationType {pearsons, kendalls, spearmans}
  private CorrelationType type;

  public CorrelationEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    if(namedParams.size() > 0) {
      if (namedParams.size() > 1) {
        throw new IOException("corr function expects only one named parameter 'type'.");
      }

      StreamExpressionNamedParameter namedParameter = namedParams.get(0);
      String name = namedParameter.getName();
      if (!name.equalsIgnoreCase("type")) {
        throw new IOException("corr function expects only one named parameter 'type'.");
      }

      String typeParam = namedParameter.getParameter().toString().trim();
      this.type= CorrelationType.valueOf(typeParam);
    } else {
      this.type = CorrelationType.pearsons;
    }
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Object doWork(Object ... values) throws IOException{

    if(values.length == 2) {
      Object first = values[0];
      Object second = values[1];

      if (null == first) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - null found for the first value", toExpression(constructingFactory)));
      }
      if (null == second) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - null found for the second value", toExpression(constructingFactory)));
      }
      if (!(first instanceof List<?>)) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the first value, expecting a list of numbers", toExpression(constructingFactory), first.getClass().getSimpleName()));
      }
      if (!(second instanceof List<?>)) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the second value, expecting a list of numbers", toExpression(constructingFactory), first.getClass().getSimpleName()));
      }

      if (type.equals(CorrelationType.pearsons)) {
        PearsonsCorrelation pearsonsCorrelation = new PearsonsCorrelation();
        return pearsonsCorrelation.correlation(
            ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray(),
            ((List) second).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray()
        );
      } else if (type.equals(CorrelationType.kendalls)) {
        KendallsCorrelation kendallsCorrelation = new KendallsCorrelation();
        return kendallsCorrelation.correlation(
            ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray(),
            ((List) second).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray()
        );

      } else if (type.equals(CorrelationType.spearmans)) {
        SpearmansCorrelation spearmansCorrelation = new SpearmansCorrelation();
        return spearmansCorrelation.correlation(
            ((List) first).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray(),
            ((List) second).stream().mapToDouble(value -> ((Number) value).doubleValue()).toArray()
        );
      } else {
        return null;
      }
    } else if(values.length == 1) {
      if(values[0] instanceof Matrix) {
        Matrix matrix = (Matrix)values[0];
        double[][] data = matrix.getData();
        if (type.equals(CorrelationType.pearsons)) {
          PearsonsCorrelation pearsonsCorrelation = new PearsonsCorrelation(data);
          RealMatrix corrMatrix = pearsonsCorrelation.getCorrelationMatrix();
          double[][] corrMatrixData = corrMatrix.getData();
          Matrix realMatrix = new Matrix(corrMatrixData);
          realMatrix.setAttribute("corr", pearsonsCorrelation);
          List<String> labels = getColumnLabels(matrix.getColumnLabels(), corrMatrixData.length);
          realMatrix.setColumnLabels(labels);
          realMatrix.setRowLabels(labels);
          return realMatrix;
        } else if (type.equals(CorrelationType.kendalls)) {
          KendallsCorrelation kendallsCorrelation = new KendallsCorrelation(data);
          RealMatrix corrMatrix = kendallsCorrelation.getCorrelationMatrix();
          double[][] corrMatrixData = corrMatrix.getData();
          Matrix realMatrix =  new Matrix(corrMatrixData);
          realMatrix.setAttribute("corr", kendallsCorrelation);
          List<String> labels = getColumnLabels(matrix.getColumnLabels(), corrMatrixData.length);
          realMatrix.setColumnLabels(labels);
          realMatrix.setRowLabels(labels);
          return realMatrix;
        } else if (type.equals(CorrelationType.spearmans)) {
          SpearmansCorrelation spearmansCorrelation = new SpearmansCorrelation(new Array2DRowRealMatrix(data, false));
          RealMatrix corrMatrix = spearmansCorrelation.getCorrelationMatrix();
          double[][] corrMatrixData = corrMatrix.getData();
          Matrix realMatrix =  new Matrix(corrMatrixData);
          realMatrix.setAttribute("corr", spearmansCorrelation.getRankCorrelation());
          List<String> labels = getColumnLabels(matrix.getColumnLabels(), corrMatrixData.length);
          realMatrix.setColumnLabels(labels);
          realMatrix.setRowLabels(labels);
          return realMatrix;
        } else {
          return null;
        }
      } else {
        throw new IOException("corr function operates on either two numeric arrays or a single matrix as parameters.");
      }
    } else {
      throw new IOException("corr function operates on either two numeric arrays or a single matrix as parameters.");
    }
  }

  public static List<String> getColumnLabels(List<String> labels, int length) {
    if(labels != null) {
      return labels;
    } else {
      List<String> l = new ArrayList<>();
      for(int i=0; i<length; i++) {
        String label = "col"+ ZplotStream.pad(Integer.toString(i), length);
        l.add(label);
      }

      return l;
    }
  }
}
