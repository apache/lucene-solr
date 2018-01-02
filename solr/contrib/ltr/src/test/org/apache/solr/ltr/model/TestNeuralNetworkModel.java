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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.norm.IdentityNormalizer;
import org.apache.solr.ltr.norm.Normalizer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNeuralNetworkModel extends TestRerankBase {

  public static LTRScoringModel createNeuralNetworkModel(String name, List<Feature> features,
      List<Normalizer> norms,
      String featureStoreName, List<Feature> allFeatures,
      Map<String,Object> params) throws ModelException {
    final LTRScoringModel model = LTRScoringModel.getInstance(solrResourceLoader,
        NeuralNetworkModel.class.getCanonicalName(),
        name,
        features, norms, featureStoreName, allFeatures, params);
    return model;
  }

  @BeforeClass
  public static void setup() throws Exception {
    setuptest(false);
  }
  
  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void testLinearAlgebra() {
    final ArrayList<double[][]> rawMatrices = new ArrayList<double[][]>();
    final double layer1Node1Weight1 = 1.0;
    final double layer1Node1Weight2 = 2.0;
    final double layer1Node1Weight3 = 3.0;
    final double layer1Node1Weight4 = 4.0;
    final double layer1Node1Bias    = 5.0;
    final double layer1Node2Weight1 = 6.0;
    final double layer1Node2Weight2 = 7.0;
    final double layer1Node2Weight3 = 8.0;
    final double layer1Node2Weight4 = 9.0;
    final double layer1Node2Bias    = 10.0;
    final double layer1Node3Weight1 = 11.0;
    final double layer1Node3Weight2 = 12.0;
    final double layer1Node3Weight3 = 13.0;
    final double layer1Node3Weight4 = 14.0;
    final double layer1Node3Bias    = 15.0;
    double[][] matrixOne = { { layer1Node1Weight1, layer1Node1Weight2, layer1Node1Weight3, layer1Node1Weight4, layer1Node1Bias },
                             { layer1Node2Weight1, layer1Node2Weight2, layer1Node2Weight3, layer1Node2Weight4, layer1Node2Bias },
                             { layer1Node3Weight1, layer1Node3Weight2, layer1Node3Weight3, layer1Node3Weight4, layer1Node3Bias } };
    final double outputNodeWeight1 = 1.0;
    final double outputNodeWeight2 = 2.0;
    final double outputNodeWeight3 = 3.0;
    final double outputNodeBias = 4.0;
    double[][] matrixTwo = { { outputNodeWeight1, outputNodeWeight2, outputNodeWeight3, outputNodeBias } };
    rawMatrices.add(matrixOne);
    rawMatrices.add(matrixTwo);
    
    final ArrayList<ArrayList<ArrayList<Double>>> weights = new ArrayList<ArrayList<ArrayList<Double>>>();
    for (int matrixNum = 0; matrixNum < rawMatrices.size(); matrixNum++) {
      double[][] matrix = rawMatrices.get(matrixNum);
      weights.add(new ArrayList<ArrayList<Double>>());
      for (int row = 0; row < matrix.length; row++) {
        weights.get(matrixNum).add(new ArrayList<Double>());
        for (int col = 0; col < matrix[row].length; col++) {
          weights.get(matrixNum).get(row).add(matrix[row][col]);
        }
      }
    }

    Map<String,Object> params = new HashMap<String,Object>();
    params.put("weights", weights);
    String nonlinearity = "relu";
    params.put("nonlinearity", nonlinearity);

    final List<Feature> allFeaturesInStore
       = getFeatures(new String[] {"constantOne", "constantTwo",
          "constantThree", "constantFour", "constantFive"});
    
    final List<Feature> featuresInModel = new ArrayList<>(allFeaturesInStore);
    Collections.shuffle(featuresInModel, random()); // store and model order of features can vary
    featuresInModel.remove(0); // models need not use all the store's features
    assertEquals(4, featuresInModel.size()); // the test model uses four features

    final List<Normalizer> norms =
        new ArrayList<Normalizer>(
            Collections.nCopies(featuresInModel.size(),IdentityNormalizer.INSTANCE));
    final LTRScoringModel ltrScoringModel = createNeuralNetworkModel("test_score",
        featuresInModel, norms, "test_score", allFeaturesInStore, params);

    {
      // pretend all features scored zero
      float[] testVec = {0.0f, 0.0f, 0.0f, 0.0f};
      // with all zero inputs the layer1 node outputs are layer1 node biases only
      final double layer1Node1Output = layer1Node1Bias;
      final double layer1Node2Output = layer1Node2Bias;
      final double layer1Node3Output = layer1Node3Bias;
      // with just one layer the output node calculation is easy
      final double outputNodeOutput =
          (layer1Node1Output*outputNodeWeight1) +
          (layer1Node2Output*outputNodeWeight2) +
          (layer1Node3Output*outputNodeWeight3) +
          outputNodeBias;
      assertEquals(74.0, outputNodeOutput, 0.001);
      // and the expected score is that of the output node
      final double expectedScore = outputNodeOutput;
      float score = ltrScoringModel.score(testVec);
      assertEquals(expectedScore, score, 0.001);
    }

    {
      // pretend all features scored one
      float[] testVec = {1.0f, 1.0f, 1.0f, 1.0f};
      // with all one inputs the layer1 node outputs are simply sum of weights and biases
      final double layer1Node1Output = layer1Node1Weight1 + layer1Node1Weight2 + layer1Node1Weight3 + layer1Node1Weight4 + layer1Node1Bias;
      final double layer1Node2Output = layer1Node2Weight1 + layer1Node2Weight2 + layer1Node2Weight3 + layer1Node2Weight4 + layer1Node2Bias;
      final double layer1Node3Output = layer1Node3Weight1 + layer1Node3Weight2 + layer1Node3Weight3 + layer1Node3Weight4 + layer1Node3Bias;
      // with just one layer the output node calculation is easy
      final double outputNodeOutput =
          (layer1Node1Output*outputNodeWeight1) +
          (layer1Node2Output*outputNodeWeight2) +
          (layer1Node3Output*outputNodeWeight3) +
          outputNodeBias;
      assertEquals(294.0, outputNodeOutput, 0.001);
      // and the expected score is that of the output node
      final double expectedScore = outputNodeOutput;
      float score = ltrScoringModel.score(testVec);
      assertEquals(expectedScore, score, 0.001);
    }

    {
      // pretend all features scored random numbers in 0.0 to 1.0 range
      final float input1 = random().nextFloat();
      final float input2 = random().nextFloat();
      final float input3 = random().nextFloat();
      final float input4 = random().nextFloat();
      float[] testVec = {input1, input2, input3, input4};
      // the layer1 node outputs are sum of input-times-weight plus bias
      final double layer1Node1Output = input1*layer1Node1Weight1 + input2*layer1Node1Weight2 + input3*layer1Node1Weight3 + input4*layer1Node1Weight4 + layer1Node1Bias;
      final double layer1Node2Output = input1*layer1Node2Weight1 + input2*layer1Node2Weight2 + input3*layer1Node2Weight3 + input4*layer1Node2Weight4 + layer1Node2Bias;
      final double layer1Node3Output = input1*layer1Node3Weight1 + input2*layer1Node3Weight2 + input3*layer1Node3Weight3 + input4*layer1Node3Weight4 + layer1Node3Bias;
      // with just one layer the output node calculation is easy
      final double outputNodeOutput =
          (layer1Node1Output*outputNodeWeight1) +
          (layer1Node2Output*outputNodeWeight2) +
          (layer1Node3Output*outputNodeWeight3) +
          outputNodeBias;
      assertTrue("outputNodeOutput="+outputNodeOutput, 74.0 <= outputNodeOutput); // inputs between zero and one produced output less than 74
      assertTrue("outputNodeOutput="+outputNodeOutput, outputNodeOutput <= 294.0); // inputs between zero and one produced output less than 294
      // and the expected score is that of the output node
      final double expectedScore = outputNodeOutput;
      float score = ltrScoringModel.score(testVec);
      assertEquals(expectedScore, score, 0.001);
    }
  }

  @Test
  public void badNonlinearityTest() throws Exception {
    final ModelException expectedException =
        new ModelException("Invalid nonlinearity for model neuralnetworkmodel_bad_nonlinearity. " +
                           "\"sig\" is not \"relu\" or \"sigmoid\".");
    try {
        createModelFromFiles("neuralnetworkmodel_bad_nonlinearity.json",
              "neuralnetworkmodel_features.json");
        fail("badNonlinearityTest failed to throw exception: "+expectedException);
    } catch (Exception actualException) {
      Throwable rootError = getRootCause(actualException);
      assertEquals(expectedException.toString(), rootError.toString());
    }
  }

  @Test
  public void inputDimensionMismatchTest() throws Exception {
    final ModelException expectedException =
        new ModelException("Dimension mismatch. Input for model neuralnetworkmodel_mismatch_input has " + 
                           "4 features, but matrix #0 has 3 non-bias columns.");
    try {
        createModelFromFiles("neuralnetworkmodel_mismatch_input.json",
              "neuralnetworkmodel_features.json");
        fail("inputDimensionMismatchTest failed to throw exception: "+expectedException);
    } catch (Exception actualException) {
      Throwable rootError = getRootCause(actualException);
      assertEquals(expectedException.toString(), rootError.toString());
    }
  }

  @Test
  public void layerDimensionMismatchTest() throws Exception {
    final ModelException expectedException =
        new ModelException("Dimension mismatch. Matrix #0 for model neuralnetworkmodel_mismatch_layers has " + 
                           "2 rows, but matrix #1 has 3 non-bias columns.");
    try {
        createModelFromFiles("neuralnetworkmodel_mismatch_layers.json",
              "neuralnetworkmodel_features.json");
        fail("layerDimensionMismatchTest failed to throw exception: "+expectedException);
    } catch (Exception actualException) {
      Throwable rootError = getRootCause(actualException);
      assertEquals(expectedException.toString(), rootError.toString());
    }
  }
  
  @Test
  public void tooManyRowsTest() throws Exception {
    final ModelException expectedException =
        new ModelException("Final matrix for model neuralnetworkmodel_too_many_rows has 2 rows, " +
                           "but should have 1 row.");
    try {
        createModelFromFiles("neuralnetworkmodel_too_many_rows.json",
              "neuralnetworkmodel_features.json");
        fail("layerDimensionMismatchTest failed to throw exception: "+expectedException);
    } catch (Exception actualException) {
      Throwable rootError = getRootCause(actualException);
      assertEquals(expectedException.toString(), rootError.toString());
    }
  }
}
