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
package org.apache.solr.ltr.model;

import java.lang.Math;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.norm.Normalizer;

/**
 * A scoring model that computes document scores using a neural network.
 * <p>
 * Example configuration:
<pre>{
    "class" : "org.apache.solr.ltr.model.NeuralNetworkModel",
    "name" : "rankNetModel",
    "features" : [
        { "name" : "documentRecency" },
        { "name" : "isBook" },
        { "name" : "originalScore" }
    ],
    "params" : {
        "weights" : [
            [ [ 1.0, 2.0, 3.0, 4.0 ], [ 5.0, 6.0, 7.0, 8.0 ], [ 9.0, 10.0, 11.0, 12.0 ], [ 13.0, 14.0, 15.0, 16.0 ] ],
            [ [ 13.0, 14.0, 15.0, 16.0, 17.0 ], [ 18.0, 19.0, 20.0, 21.0, 22.0 ] ],
            [ [ 23.0, 24.0, 25.0 ] ]
        ],
        "nonlinearity": "relu"
    }
}</pre>
 * <p>
 * Training libraries:
 * <ul>
 * <li> <a href="https://github.com/airalcorn2/RankNet">Keras Implementation of RankNet</a>
 * </ul>
 * <p>
 * Background reading:
 * <ul>
 * <li> <a href="http://icml.cc/2015/wp-content/uploads/2015/06/icml_ranking.pdf">
 * C. Burges, T. Shaked, E. Renshaw, A. Lazier, M. Deeds, N. Hamilton, and G. Hullender. Learning to Rank Using Gradient Descent.
 * Proceedings of the 22nd International Conference on Machine Learning (ICML), ACM, 2005.</a>
 * </ul>
 */
public class NeuralNetworkModel extends LTRScoringModel {

  protected ArrayList<float[][]> weightMatrices;
  protected String nonlinearity;

  public void setWeights(Object weights) {
    final List<List<List<Double>>> matrixList = (List<List<List<Double>>>) weights;

    weightMatrices = new ArrayList<float[][]>();

    for (List<List<Double>> matrix : matrixList) {
      int numRows = matrix.size();
      int numCols = matrix.get(0).size();;

      float[][] weightMatrix = new float[numRows][numCols];

      for (int i = 0; i < numRows; i++) {
        for (int j = 0; j < numCols; j++) {
          weightMatrix[i][j] = matrix.get(i).get(j).floatValue();
        }
      }

      weightMatrices.add(weightMatrix);
    }
  }

  public void setNonlinearity(Object nonlinearityStr) {
    nonlinearity = (String) nonlinearityStr;
  }

  private float[] dot(float[][] matrix, float[] inputVec) {

    int matrixRows = matrix.length;
    int matrixCols = matrix[0].length;
    float[] outputVec = new float[matrixRows];

    for (int i = 0; i < matrixRows; i++) {
      float outputVal = matrix[i][matrixCols - 1]; // Bias.
      for (int j = 0; j < matrixCols - 1; j++) {
        outputVal += matrix[i][j] * inputVec[j];
      }
      outputVec[i] = outputVal;
    }

    return outputVec;
  }

  private float doNonlinearity(float x) {
    if (nonlinearity.equals("relu")) {
      return x < 0 ? 0 : x;
    } else {
      return (float) (1 / (1 + Math.exp(-x)));
    }
  }

  public NeuralNetworkModel(String name, List<Feature> features,
                 List<Normalizer> norms,
                 String featureStoreName, List<Feature> allFeatures,
                 Map<String,Object> params) {
    super(name, features, norms, featureStoreName, allFeatures, params);
  }

  @Override
  protected void validate() throws ModelException {
    super.validate();

    if (!nonlinearity.matches("relu|sigmoid")) {
      throw new ModelException("Invalid nonlinearity for model " + name + ". " +
                               "\"" + nonlinearity + "\" is not \"relu\" or \"sigmoid\".");
    }

    int inputDim = features.size();

    for (int i = 0; i < weightMatrices.size(); i++) {
      float[][] weightMatrix = weightMatrices.get(i);

      int numRows = weightMatrix.length;
      int numCols = weightMatrix[0].length;

      if (inputDim != numCols - 1) {
        if (i == 0) {
          throw new ModelException("Dimension mismatch. Input for model " + name + " has " + Integer.toString(inputDim)
                                   + " features, but matrix #0 has " + Integer.toString(numCols - 1) +
                                   " non-bias columns.");
        } else {
          throw new ModelException("Dimension mismatch. Matrix #" + Integer.toString(i - 1) + " for model " + name +
                                   " has " + Integer.toString(inputDim) + " rows, but matrix #" + Integer.toString(i) +
                                   " has " + Integer.toString(numCols - 1) + " non-bias columns.");
        }
      }
      
      if (i == weightMatrices.size() - 1 & numRows != 1) {
        throw new ModelException("Final matrix for model " + name + " has " + Integer.toString(numRows) +
                                 " rows, but should have 1 row.");
      }
      
      inputDim = numRows;
    }
  }

  @Override
  public float score(float[] inputFeatures) {

    float[] outputVec = inputFeatures;
    float[][] weightMatrix;
    int layers = weightMatrices.size();

    for (int layer = 0; layer < layers; layer++) {

      weightMatrix = weightMatrices.get(layer);
      outputVec = dot(weightMatrix, outputVec);

      if (layer < layers - 1) {
        for (int i = 0; i < outputVec.length; i++) {
          outputVec[i] = doNonlinearity(outputVec[i]);
        }
      }
    }

    return outputVec[0];
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc,
                             float finalScore, List<Explanation> featureExplanations) {

    String modelDescription = "";

    for (int layer = 0; layer < weightMatrices.size(); layer++) {

      float[][] weightMatrix = weightMatrices.get(layer);
      int numCols = weightMatrix[0].length;

      if (layer == 0) {
        modelDescription += "Input has " + Integer.toString(numCols - 1) + " features.";
      } else {
        modelDescription += System.lineSeparator();
        modelDescription += "Hidden layer #" + Integer.toString(layer) + " has " + Integer.toString(numCols - 1);
        modelDescription += " fully connected units.";
      }
    }
    return Explanation.match(finalScore, modelDescription);
  }

}
