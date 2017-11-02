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
 * A scoring model that computes scores using a neural network.
 * <p>
 * Example configuration:
<pre>{
    "class" : "org.apache.solr.ltr.model.RankNet",
    "name" : "rankNetModel",
    "features" : [
        { "name" : "documentRecency" },
        { "name" : "isBook" },
        { "name" : "originalScore" }
    ],
    "params" : {
        "weights" : [
            "1,2,3\n4,5,6\n7,8,9\n10,11,12",
            "13,14,15,16\n17,18,19,20",
            "21,22"
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
public class RankNet extends LTRScoringModel {

  protected ArrayList<float[][]> weightMatrices;
  protected String nonlinearity;

  public void setWeights(Object weights) {

    final List<String> weightStrings = (List<String>) weights;
    weightMatrices = new ArrayList<float[][]>();

    for (String matrixString : weightStrings) {

      String[] rows = matrixString.split("\n");
      int numRows = rows.length;
      int numCols = rows[0].split(",").length;

      float[][] weightMatrix = new float[numRows][numCols];
      for (int i = 0; i < numRows; i++) {
        String[] vals = rows[i].split(",");
        for (int j = 0; j < numCols; j++) {
          weightMatrix[i][j] = Float.parseFloat(vals[j]);
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
    } else if (nonlinearity.equals("sigmoid")) {
      return (float)(1 / (1 + Math.exp(-x)));
    } else {
      return x;
    }
  }

  public RankNet(String name, List<Feature> features,
                 List<Normalizer> norms,
                 String featureStoreName, List<Feature> allFeatures,
                 Map<String,Object> params) {
    super(name, features, norms, featureStoreName, allFeatures, params);
  }

  @Override
  public float score(float[] modelFeatureValuesNormalized) {

    float[] outputVec = modelFeatureValuesNormalized;
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
      int numRows = weightMatrix.length;
      int numCols = weightMatrix[layer].length;
      if (layer == 0) {
        modelDescription += String.format("Input has %1$d features.", numCols - 1);
      } else {
        modelDescription += String.format("%nHidden layer #%1$d has %2$d units.", layer, numCols);
      }
    }
    return Explanation.match(finalScore, modelDescription);
  }

}
