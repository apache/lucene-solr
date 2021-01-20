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
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;

public class KnnRegressionEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  private boolean robust=false;
  private boolean scale=false;

  public KnnRegressionEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);

    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(namedParam.getName().equals("scale")){
        this.scale = Boolean.parseBoolean(namedParam.getParameter().toString().trim());
      } else if(namedParam.getName().equals("robust")) {
        this.robust = Boolean.parseBoolean(namedParam.getParameter().toString().trim());
      } else {
        throw new IOException("Unexpected named parameter:"+namedParam.getName());
      }
    }
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Object doWork(Object ... values) throws IOException {

    if(values.length < 3) {
      throw new IOException("knnRegress expects atleast three parameters: an observation matrix, an outcomes vector and k.");
    }

    Matrix observations = null;
    List<Number> outcomes = null;
    int k = 5;
    DistanceMeasure distanceMeasure = new EuclideanDistance();
    boolean bivariate = false;

    if(values[0] instanceof Matrix) {
      observations = (Matrix)values[0];
    } else if(values[0] instanceof List) {
      bivariate = true;
      List<Number> vec = (List<Number>)values[0];
      double[][] data = new double[vec.size()][1];
      for(int i=0; i<vec.size(); i++) {
        data[i][0] = vec.get(i).doubleValue();
      }
      observations = new Matrix(data);
    } else {
      throw new IOException("The first parameter for knnRegress should be the observation vector or matrix.");
    }

    if(values[1] instanceof List) {
      outcomes = (List) values[1];
    } else {
      throw new IOException("The second parameter for knnRegress should be outcome array. ");
    }

    if(values[2] instanceof Number) {
      k = ((Number) values[2]).intValue();
    } else {
      throw new IOException("The third parameter for knnRegress should be k. ");
    }

    if(values.length == 4) {
      if(values[3] instanceof DistanceMeasure) {
        distanceMeasure = (DistanceMeasure) values[3];
      } else {
        throw new IOException("The fourth parameter for knnRegress should be a distance measure. ");
      }
    }

    double[] outcomeData = new double[outcomes.size()];
    for(int i=0; i<outcomeData.length; i++) {
      outcomeData[i] = outcomes.get(i).doubleValue();
    }

    @SuppressWarnings({"rawtypes"})
    Map map = new HashMap();
    map.put("k", k);
    map.put("observations", observations.getRowCount());
    map.put("features", observations.getColumnCount());
    map.put("distance", distanceMeasure.getClass().getSimpleName());
    map.put("robust", robust);
    map.put("scale", scale);

    return new KnnRegressionTuple(observations, outcomeData, k, distanceMeasure, map, scale, robust, bivariate);
  }


  public static class KnnRegressionTuple extends Tuple {

    private Matrix observations;
    private Matrix scaledObservations;
    private double[] outcomes;
    private int k;
    private DistanceMeasure distanceMeasure;
    private boolean scale;
    private boolean robust;
    private boolean bivariate;

    public KnnRegressionTuple(Matrix observations,
                              double[] outcomes,
                              int k,
                              DistanceMeasure distanceMeasure,
                              Map<?,?> map,
                              boolean scale,
                              boolean robust,
                              boolean bivariate) {
      super(map);
      this.observations = observations;
      this.outcomes = outcomes;
      this.k = k;
      this.distanceMeasure = distanceMeasure;
      this.scale = scale;
      this.robust = robust;
      this.bivariate = bivariate;
    }

    public boolean getScale() {
      return this.scale;
    }
    public boolean getBivariate() {
      return this.bivariate;
    }

    //MinMax Scale both the observations and the predictors

    public double[] scale(double[] predictors) {
      double[][] data = observations.getData();
      //We need to scale the columns of the data matrix with along with the predictors
      Array2DRowRealMatrix matrix = new Array2DRowRealMatrix(data);
      Array2DRowRealMatrix transposed = (Array2DRowRealMatrix) matrix.transpose();
      double[][] featureRows = transposed.getDataRef();

      double[] scaledPredictors = new double[predictors.length];

      for(int i=0; i<featureRows.length; i++) {
        double[] featureRow = featureRows[i];
        double[] combinedFeatureRow = new double[featureRow.length+1];
        System.arraycopy(featureRow, 0, combinedFeatureRow, 0, featureRow.length);
        combinedFeatureRow[featureRow.length] = predictors[i];  // Add the last feature from the predictor
        double[] scaledFeatures = MinMaxScaleEvaluator.scale(combinedFeatureRow, 0, 1);
        scaledPredictors[i] = scaledFeatures[featureRow.length];
        System.arraycopy(scaledFeatures, 0, featureRow, 0, featureRow.length);
      }

      Array2DRowRealMatrix scaledFeatureMatrix = new Array2DRowRealMatrix(featureRows);


      Array2DRowRealMatrix scaledObservationsMatrix= (Array2DRowRealMatrix)scaledFeatureMatrix.transpose();
      this.scaledObservations = new Matrix(scaledObservationsMatrix.getDataRef());
      return scaledPredictors;
    }


    public Matrix scale(Matrix predictors) {
      double[][] observationData = observations.getData();
      //We need to scale the columns of the data matrix with along with the predictors
      Array2DRowRealMatrix observationMatrix = new Array2DRowRealMatrix(observationData);
      Array2DRowRealMatrix observationTransposed = (Array2DRowRealMatrix) observationMatrix.transpose();
      double[][] observationFeatureRows = observationTransposed.getDataRef();

      double[][] predictorsData = predictors.getData();
      //We need to scale the columns of the data matrix with along with the predictors
      Array2DRowRealMatrix predictorMatrix = new Array2DRowRealMatrix(predictorsData);
      Array2DRowRealMatrix predictorTransposed = (Array2DRowRealMatrix) predictorMatrix.transpose();
      double[][] predictorFeatureRows = predictorTransposed.getDataRef();

      for(int i=0; i<observationFeatureRows.length; i++) {
        double[] observationFeatureRow = observationFeatureRows[i];
        double[] predictorFeatureRow = predictorFeatureRows[i];
        double[] combinedFeatureRow = new double[observationFeatureRow.length+predictorFeatureRow.length];
        System.arraycopy(observationFeatureRow, 0, combinedFeatureRow, 0, observationFeatureRow.length);
        System.arraycopy(predictorFeatureRow, 0, combinedFeatureRow, observationFeatureRow.length, predictorFeatureRow.length);

        double[] scaledFeatures = MinMaxScaleEvaluator.scale(combinedFeatureRow, 0, 1);
        System.arraycopy(scaledFeatures, 0, observationFeatureRow, 0, observationFeatureRow.length);
        System.arraycopy(scaledFeatures, observationFeatureRow.length, predictorFeatureRow, 0, predictorFeatureRow.length);
      }

      Array2DRowRealMatrix scaledFeatureMatrix = new Array2DRowRealMatrix(observationFeatureRows);
      Array2DRowRealMatrix scaledObservationsMatrix= (Array2DRowRealMatrix)scaledFeatureMatrix.transpose();
      this.scaledObservations = new Matrix(scaledObservationsMatrix.getDataRef());

      Array2DRowRealMatrix scaledPredictorMatrix = new Array2DRowRealMatrix(predictorFeatureRows);
      Array2DRowRealMatrix scaledTransposedPredictorMatrix= (Array2DRowRealMatrix)scaledPredictorMatrix.transpose();
      return new Matrix(scaledTransposedPredictorMatrix.getDataRef());
    }


    public double predict(double[] values) {

      Matrix obs = scaledObservations != null ? scaledObservations : observations;
      Matrix knn = KnnEvaluator.search(obs, values, k, distanceMeasure);
      @SuppressWarnings({"unchecked"})
      List<Number> indexes = (List<Number>)knn.getAttribute("indexes");

      if(robust) {
        //Get the median of the results.
        double[] vals = new double[indexes.size()];
        Percentile percentile = new Percentile();
        int i=0;
        for (Number n : indexes) {
           vals[i++]=outcomes[n.intValue()];
        }

        //Return 50 percentile.
        return percentile.evaluate(vals, 50);
      } else {
        //Get the average of the results
        double sum = 0;

        //Collect the outcomes for the nearest neighbors
        for (Number n : indexes) {
          sum += outcomes[n.intValue()];
        }

        //Return the average of the outcomes as the prediction.
        return sum / ((double) indexes.size());
      }
    }
  }
}

