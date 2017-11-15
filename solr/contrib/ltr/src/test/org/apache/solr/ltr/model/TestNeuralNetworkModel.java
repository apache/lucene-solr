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
    double[][] matrixOne = { { 1.0, 2.0, 3.0, 4.0, 5.0 },
                             { 6.0, 7.0, 8.0, 9.0, 10.0 },
                            { 11.0, 12.0, 13.0, 14.0, 15.0 } };
    double[][] matrixTwo = { { 1.0, 2.0, 3.0, 4.0 } };
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
    final List<Feature> features = getFeatures(new String[] {"constantOne", "constantTwo",
                                                             "constantThree", "constantFour"});
    final List<Normalizer> norms =
        new ArrayList<Normalizer>(
            Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));
    
    params.put("weights", weights);
    String nonlinearity = "relu";
    params.put("nonlinearity", nonlinearity);
    
    final LTRScoringModel ltrScoringModel = createNeuralNetworkModel("test_score",
        features, norms, "test_score", features, params);

    float[] testVec = {1.0f, 1.0f, 1.0f, 1.0f};
    ltrScoringModel.score(testVec);
    assertEquals(294, ltrScoringModel.score(testVec), 0.001);
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
