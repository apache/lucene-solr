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
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.util.SolrPluginUtils;

/**
 * A scoring model that computes document scores using a neural network.
 * <p>
 * Supported <a href="https://en.wikipedia.org/wiki/Activation_function">activation functions</a> are:
 * <code>identity</code>, <code>relu</code>, <code>sigmoid</code>, <code>tanh</code>, <code>leakyrelu</code> and
 * contributions to support additional activation functions are welcome.
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
        "layers" : [
            {
                "matrix" : [ [ 1.0, 2.0, 3.0 ],
                             [ 4.0, 5.0, 6.0 ],
                             [ 7.0, 8.0, 9.0 ],
                             [ 10.0, 11.0, 12.0 ] ],
                "bias" : [ 13.0, 14.0, 15.0, 16.0 ],
                "activation" : "sigmoid"
            },
            {
                "matrix" : [ [ 17.0, 18.0, 19.0, 20.0 ],
                             [ 21.0, 22.0, 23.0, 24.0 ] ],
                "bias" : [ 25.0, 26.0 ],
                "activation" : "relu"
            },
            {
                "matrix" : [ [ 27.0, 28.0 ],
                             [ 29.0, 30.0 ] ],
                "bias" : [ 31.0, 32.0 ],
                "activation" : "leakyrelu"
            },
            {
                "matrix" : [ [ 33.0, 34.0 ],
                             [ 35.0, 36.0 ] ],
                "bias" : [ 37.0, 38.0 ],
                "activation" : "tanh"
            },
            {
                "matrix" : [ [ 39.0, 40.0 ] ],
                "bias" : [ 41.0 ],
                "activation" : "identity"
            }
        ]
    }
}</pre>
 * <p>
 * Training libraries:
 * <ul>
 * <li> <a href="https://keras.io">Keras</a> is a high-level neural networks API, written in Python.
 * A Keras and Solr implementation of RankNet can be found at <a href="https://github.com/airalcorn2/RankNet">https://github.com/airalcorn2/RankNet</a>.
 * </ul>
 * <p>
 * Background reading:
 * <ul>
 * <li> <a href="http://icml.cc/2015/wp-content/uploads/2015/06/icml_ranking.pdf">
 * C. Burges, T. Shaked, E. Renshaw, A. Lazier, M. Deeds, N. Hamilton, and G. Hullender. Learning to Rank Using Gradient Descent.
 * Proceedings of the 22nd International Conference on Machine Learning (ICML), ACM, 2005.</a>
 * </ul>
 */
@SuppressWarnings("unchecked")
public class NeuralNetworkModel extends LTRScoringModel {

  /**
   * layers is part of the LTRScoringModel params map
   * and therefore here it does not individually
   * influence the class hashCode, equals, etc.
   */
  private List<Layer> layers;

  protected interface Activation {
    // similar to UnaryOperator<Float>
    float apply(float in);
  }

  public interface Layer {
    public float[] calculateOutput(float[] inputVec);
    public int validate(int inputDim) throws ModelException;
    public String describe();
  }

  public class DefaultLayer implements Layer {
    private int layerID;
    private float[][] weightMatrix;
    private int matrixRows;
    private int matrixCols;
    private float[] biasVector;
    private int numUnits;
    protected String activationStr;
    protected Activation activation;

    public DefaultLayer() {
      layerID = layers.size();
    }

    public void setMatrix(Object matrixObj) {
      final List<List<Double>> matrix = (List<List<Double>>) matrixObj;
      this.matrixRows = matrix.size();
      this.matrixCols = matrix.get(0).size();
      this.weightMatrix = new float[this.matrixRows][this.matrixCols];

      for (int i = 0; i < this.matrixRows; i++) {
        for (int j = 0; j < this.matrixCols; j++) {
          this.weightMatrix[i][j] = matrix.get(i).get(j).floatValue();
        }
      }
    }

    public void setBias(Object biasObj) {
      final List<Double> vector = (List<Double>) biasObj;
      this.numUnits = vector.size();
      this.biasVector = new float[numUnits];

      for (int i = 0; i < this.numUnits; i++) {
        this.biasVector[i] = vector.get(i).floatValue();
      }
    }

    public void setActivation(Object activationStr) {
      this.activationStr = (String) activationStr;
      switch (this.activationStr) {
        case "relu":
          this.activation = new Activation() {
            @Override
            public float apply(float in) {
              return in < 0 ? 0 : in;
            }
          };
          break;
        case "leakyrelu":
          this.activation = new Activation() {
            @Override
            public float apply(float in) {
              return in < 0 ? 0.01f * in : in;
            }
          };
          break;
        case "tanh":
          this.activation = new Activation() {
            @Override
            public float apply(float in) {
              return (float)Math.tanh(in);
            }
          };
          break;
        case "sigmoid":
          this.activation = new Activation() {
            @Override
            public float apply(float in) {
              return (float) (1 / (1 + Math.exp(-in)));
            }
          };
          break;
        case "identity":
          this.activation = new Activation() {
            @Override
            public float apply(float in) {
              return in;
            }
          };
          break;
        default:
          this.activation = null;
          break;
      }
    }

    public float[] calculateOutput(float[] inputVec) {

      float[] outputVec = new float[this.matrixRows];

      for (int i = 0; i < this.matrixRows; i++) {
        float outputVal = this.biasVector[i];
        for (int j = 0; j < this.matrixCols; j++) {
          outputVal += this.weightMatrix[i][j] * inputVec[j];
        }
        outputVec[i] = this.activation.apply(outputVal);
      }

      return outputVec;
    }

    public int validate(int inputDim) throws ModelException {
      if (this.numUnits != this.matrixRows) {
        throw new ModelException("Dimension mismatch in model \""  + name +  "\". Layer " +
                                 Integer.toString(this.layerID) + " has " + Integer.toString(this.numUnits) +
                                 " bias weights but " + Integer.toString(this.matrixRows) + " weight matrix rows.");
      }
      if (this.activation == null) {
        throw new ModelException("Invalid activation function (\""+this.activationStr+"\") in layer "+Integer.toString(this.layerID)+" of model \"" + name + "\".");
      }
      if (inputDim != this.matrixCols) {
        if (this.layerID == 0) {
          throw new ModelException("Dimension mismatch in model \"" + name + "\". The input has " +
                                   Integer.toString(inputDim) + " features, but the weight matrix for layer 0 has " +
                                   Integer.toString(this.matrixCols) + " columns.");
        } else {
          throw new ModelException("Dimension mismatch in model \"" + name + "\". The weight matrix for layer " +
                                   Integer.toString(this.layerID - 1) + " has " + Integer.toString(inputDim) + " rows, but the " +
                                   "weight matrix for layer " + Integer.toString(this.layerID) + " has " +
                                   Integer.toString(this.matrixCols) + " columns.");
        }
      }
      return this.matrixRows;
    }

    public String describe() {
      final StringBuilder sb = new StringBuilder();
      sb
      .append("(matrix=").append(Integer.toString(this.matrixRows)).append('x').append(Integer.toString(this.matrixCols))
      .append(",activation=").append(this.activationStr).append(")");
      return sb.toString();
    }
  }

  protected Layer createLayer(Object o) {
    final DefaultLayer layer = new DefaultLayer();
    if (o != null) {
      SolrPluginUtils.invokeSetters(layer, ((Map<String,Object>) o).entrySet());
    }
    return layer;
  }

  public void setLayers(Object layers) {
    this.layers = new ArrayList<Layer>();
    for (final Object o : (List<Object>) layers) {
      final Layer layer = createLayer(o);
      this.layers.add(layer);
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

    int inputDim = features.size();

    for (Layer layer : layers) {
      inputDim = layer.validate(inputDim);
    }

    if (inputDim != 1) {
      throw new ModelException("The output matrix for model \"" + name + "\" has " + Integer.toString(inputDim) +
                               " rows, but should only have one.");
    }
  }

  @Override
  public float score(float[] inputFeatures) {

    float[] outputVec = inputFeatures;

    for (Layer layer : layers) {
      outputVec = layer.calculateOutput(outputVec);
    }

    return outputVec[0];
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc,
                             float finalScore, List<Explanation> featureExplanations) {

    final StringBuilder modelDescription = new StringBuilder();

    modelDescription.append("(name=").append(getName());
    modelDescription.append(",featureValues=[");

    for (int i = 0; i < featureExplanations.size(); i++) {
      Explanation featureExplain = featureExplanations.get(i);
      if (i > 0) {
        modelDescription.append(',');
      }
      final String key = features.get(i).getName();
      modelDescription.append(key).append('=').append(featureExplain.getValue());
    }

    modelDescription.append("],layers=[");

    for (int i = 0; i < layers.size(); i++) {
      if (i > 0) modelDescription.append(',');
      modelDescription.append(layers.get(i).describe());
    }
    modelDescription.append("])");

    return Explanation.match(finalScore, modelDescription.toString());
  }

}
