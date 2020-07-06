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

import org.apache.lucene.search.Explanation;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.norm.IdentityNormalizer;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.util.SolrPluginUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNeuralNetworkModel extends TestRerankBase {

  public static LTRScoringModel createNeuralNetworkModel(String name, List<Feature> features,
      List<Normalizer> norms,
      String featureStoreName, List<Feature> allFeatures,
      Map<String,Object> params) throws ModelException {
    return LTRScoringModel.getInstance(solrResourceLoader,
        NeuralNetworkModel.class.getName(),
        name,
        features, norms, featureStoreName, allFeatures, params);
  }

  @Before
  public void setup() throws Exception {
    setuptest(false);
  }
  
  @After
  public void after() throws Exception {
    aftertest();
  }

  protected static Map<String,Object> createLayerParams(double[][] matrix, double[] bias, String activation) {

    final ArrayList<ArrayList<Double>> matrixList = new ArrayList<ArrayList<Double>>();
    for (int row = 0; row < matrix.length; row++) {
      matrixList.add(new ArrayList<Double>());
      for (int col = 0; col < matrix[row].length; col++) {
        matrixList.get(row).add(matrix[row][col]);
      }
    }

    final ArrayList<Double> biasList = new ArrayList<Double>();
    for (int i = 0; i < bias.length; i++) {
      biasList.add(bias[i]);
    }

    final Map<String,Object> layer = new HashMap<String,Object>();
    layer.put("matrix", matrixList);
    layer.put("bias", biasList);
    layer.put("activation", activation);

    return layer;
  }

  @Test
  public void testLinearAlgebra() {

    final double layer1Node1Weight1 = 1.0;
    final double layer1Node1Weight2 = 2.0;
    final double layer1Node1Weight3 = 3.0;
    final double layer1Node1Weight4 = 4.0;
    final double layer1Node2Weight1 = 5.0;
    final double layer1Node2Weight2 = 6.0;
    final double layer1Node2Weight3 = 7.0;
    final double layer1Node2Weight4 = 8.0;
    final double layer1Node3Weight1 = 9.0;
    final double layer1Node3Weight2 = 10.0;
    final double layer1Node3Weight3 = 11.0;
    final double layer1Node3Weight4 = 12.0;

    double[][] matrixOne = { { layer1Node1Weight1, layer1Node1Weight2, layer1Node1Weight3, layer1Node1Weight4 },
                             { layer1Node2Weight1, layer1Node2Weight2, layer1Node2Weight3, layer1Node2Weight4 },
                             { layer1Node3Weight1, layer1Node3Weight2, layer1Node3Weight3, layer1Node3Weight4 } };

    final double layer1Node1Bias = 13.0;
    final double layer1Node2Bias = 14.0;
    final double layer1Node3Bias = 15.0;

    double[] biasOne = { layer1Node1Bias, layer1Node2Bias, layer1Node3Bias };

    final double outputNodeWeight1 = 16.0;
    final double outputNodeWeight2 = 17.0;
    final double outputNodeWeight3 = 18.0;

    double[][] matrixTwo = { { outputNodeWeight1, outputNodeWeight2, outputNodeWeight3 } };

    final double outputNodeBias = 19.0;

    double[] biasTwo = { outputNodeBias };

    Map<String,Object> params = new HashMap<String,Object>();
    ArrayList<Map<String,Object>> layers = new ArrayList<Map<String,Object>>();

    layers.add(createLayerParams(matrixOne, biasOne, "relu"));
    layers.add(createLayerParams(matrixTwo, biasTwo, "relu"));

    params.put("layers", layers);

    final List<Feature> allFeaturesInStore
       = getFeatures(new String[] {"constantOne", "constantTwo", "constantThree", "constantFour", "constantFive"});

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
      float[] testVec = { 0.0f, 0.0f, 0.0f, 0.0f };
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
      assertEquals(735.0, outputNodeOutput, 0.001);
      // and the expected score is that of the output node
      final double expectedScore = outputNodeOutput;
      float score = ltrScoringModel.score(testVec);
      assertEquals(expectedScore, score, 0.001);
    }

    {
      // pretend all features scored one
      float[] testVec = { 1.0f, 1.0f, 1.0f, 1.0f };
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
      assertEquals(2093.0, outputNodeOutput, 0.001);
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
      assertTrue("outputNodeOutput="+outputNodeOutput, 735.0 <= outputNodeOutput); // inputs between zero and one produced output greater than 74
      assertTrue("outputNodeOutput="+outputNodeOutput, outputNodeOutput <= 2093.0); // inputs between zero and one produced output less than 294
      // and the expected score is that of the output node
      final double expectedScore = outputNodeOutput;
      float score = ltrScoringModel.score(testVec);
      assertEquals(expectedScore, score, 0.001);
    }
  }

  @Test
  public void badActivationTest() throws Exception {
    final ModelException expectedException =
            new ModelException("Invalid activation function (\"sig\") in layer 0 of model \"neuralnetworkmodel_bad_activation\".");
    Exception ex = expectThrows(Exception.class, () -> {
      createModelFromFiles("neuralnetworkmodel_bad_activation.json",
          "neuralnetworkmodel_features.json");
    });
    Throwable rootError = getRootCause(ex);
    assertEquals(expectedException.toString(), rootError.toString());
  }

  @Test
  public void biasDimensionMismatchTest() throws Exception {
    final ModelException expectedException =
            new ModelException("Dimension mismatch in model \"neuralnetworkmodel_mismatch_bias\". " +
                               "Layer 0 has 2 bias weights but 3 weight matrix rows.");
    Exception ex = expectThrows(Exception.class, () -> {
      createModelFromFiles("neuralnetworkmodel_mismatch_bias.json",
          "neuralnetworkmodel_features.json");
    });
    Throwable rootError = getRootCause(ex);
    assertEquals(expectedException.toString(), rootError.toString());
  }

  @Test
  public void inputDimensionMismatchTest() throws Exception {
    final ModelException expectedException =
        new ModelException("Dimension mismatch in model \"neuralnetworkmodel_mismatch_input\". The input has " +
                           "4 features, but the weight matrix for layer 0 has 3 columns.");
    Exception ex = expectThrows(Exception.class,  () -> {
      createModelFromFiles("neuralnetworkmodel_mismatch_input.json",
          "neuralnetworkmodel_features.json");
    });
    Throwable rootError = getRootCause(ex);
    assertEquals(expectedException.toString(), rootError.toString());
  }

  @Test
  public void layerDimensionMismatchTest() throws Exception {
    final ModelException expectedException =
        new ModelException("Dimension mismatch in model \"neuralnetworkmodel_mismatch_layers\". The weight matrix " +
                           "for layer 0 has 2 rows, but the weight matrix for layer 1 has 3 columns.");
    Exception ex = expectThrows(Exception.class, () -> {
      createModelFromFiles("neuralnetworkmodel_mismatch_layers.json",
          "neuralnetworkmodel_features.json");
    });
    Throwable rootError = getRootCause(ex);
    assertEquals(expectedException.toString(), rootError.toString());
  }
  
  @Test
  public void tooManyRowsTest() throws Exception {
    final ModelException expectedException =
        new ModelException("Dimension mismatch in model \"neuralnetworkmodel_too_many_rows\". " +
                           "Layer 1 has 1 bias weights but 2 weight matrix rows.");
    Exception ex = expectThrows(Exception.class, () -> {
      createModelFromFiles("neuralnetworkmodel_too_many_rows.json",
          "neuralnetworkmodel_features.json");
    });
    Throwable rootError = getRootCause(ex);
    assertEquals(expectedException.toString(), rootError.toString());
  }

  @Test
  public void testExplain() throws Exception {

    final LTRScoringModel model = createModelFromFiles("neuralnetworkmodel_explainable.json",
        "neuralnetworkmodel_features.json");

    final float[] featureValues = { 1.2f, 3.4f, 5.6f, 7.8f };

    final List<Explanation> explanations = new ArrayList<Explanation>();
    for (int ii=0; ii<featureValues.length; ++ii)
    {
      explanations.add(Explanation.match(featureValues[ii], ""));
    }

    final float finalScore = model.score(featureValues);
    final Explanation explanation = model.explain(null, 0, finalScore, explanations);
    assertEquals(finalScore+" = (name=neuralnetworkmodel_explainable"+
        ",featureValues=[constantOne=1.2,constantTwo=3.4,constantThree=5.6,constantFour=7.8]"+
        ",layers=[(matrix=2x4,activation=relu),(matrix=1x2,activation=identity)]"+
        ")\n",
        explanation.toString());
  }

  public static class CustomNeuralNetworkModel extends NeuralNetworkModel {

    public CustomNeuralNetworkModel(String name, List<Feature> features, List<Normalizer> norms,
        String featureStoreName, List<Feature> allFeatures, Map<String,Object> params) {
      super(name, features, norms, featureStoreName, allFeatures, params);
    }

    public class DefaultLayer extends org.apache.solr.ltr.model.NeuralNetworkModel.DefaultLayer {
      @Override
      public void setActivation(Object o) {
        super.setActivation(o);
        switch (this.activationStr) {
          case "answer":
            this.activation = new Activation() {
              @Override
              public float apply(float in) {
                return in * 42f;
              }
            };
            break;
          default:
            break;
        }
      }
    }

    @Override
    @SuppressWarnings({"unchecked"})
    protected Layer createLayer(Object o) {
      final DefaultLayer layer = new DefaultLayer();
      if (o != null) {
        SolrPluginUtils.invokeSetters(layer, ((Map<String,Object>) o).entrySet());
      }
      return layer;
    }

  }

  @Test
  public void testCustom() throws Exception {

    final LTRScoringModel model = createModelFromFiles("neuralnetworkmodel_custom.json",
        "neuralnetworkmodel_features.json");

    final float featureValue1 = 4f;
    final float featureValue2 = 2f;
    final float[] featureValues = { featureValue1, featureValue2 };

    final double expectedScore = (featureValue1+featureValue2) * 42f;
    float actualScore = model.score(featureValues);
    assertEquals(expectedScore, actualScore, 0.001);

    final List<Explanation> explanations = new ArrayList<Explanation>();
    for (int ii=0; ii<featureValues.length; ++ii)
    {
      explanations.add(Explanation.match(featureValues[ii], ""));
    }
    final Explanation explanation = model.explain(null, 0, actualScore, explanations);
    assertEquals(actualScore+" = (name=neuralnetworkmodel_custom"+
        ",featureValues=[constantFour=4.0,constantTwo=2.0]"+
        ",layers=[(matrix=1x2,activation=answer)]"+
        ")\n",
        explanation.toString());
  }

}
