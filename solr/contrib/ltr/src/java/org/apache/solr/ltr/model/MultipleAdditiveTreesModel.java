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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.util.SolrPluginUtils;

/**
 * A scoring model that computes scores based on the summation of multiple weighted trees.
 * Example models are LambdaMART and Gradient Boosted Regression Trees (GBRT) .
 * <p>
 * Example configuration:
<pre>{
   "class" : "org.apache.solr.ltr.model.MultipleAdditiveTreesModel",
   "name" : "multipleadditivetreesmodel",
   "features":[
       { "name" : "userTextTitleMatch"},
       { "name" : "originalScore"}
   ],
   "params" : {
       "trees" : [
           {
               "weight" : "1",
               "root": {
                   "feature" : "userTextTitleMatch",
                   "threshold" : "0.5",
                   "left" : {
                       "value" : "-100"
                   },
                   "right" : {
                       "feature" : "originalScore",
                       "threshold" : "10.0",
                       "left" : {
                           "value" : "50"
                       },
                       "right" : {
                           "value" : "75"
                       }
                   }
               }
           },
           {
               "weight" : "2",
               "root" : {
                   "value" : "-10"
               }
           }
       ]
   }
}</pre>
 * <p>
 * Training libraries:
 * <ul>
 * <li> <a href="http://sourceforge.net/p/lemur/wiki/RankLib/">RankLib</a>
 * </ul>
 * <p>
 * Background reading:
 * <ul>
 * <li> <a href="http://research.microsoft.com/pubs/132652/MSR-TR-2010-82.pdf">
 * Christopher J.C. Burges. From RankNet to LambdaRank to LambdaMART: An Overview.
 * Microsoft Research Technical Report MSR-TR-2010-82.</a>
 * </ul>
 * <ul>
 * <li> <a href="https://papers.nips.cc/paper/3305-a-general-boosting-method-and-its-application-to-learning-ranking-functions-for-web-search.pdf">
 * Z. Zheng, H. Zha, T. Zhang, O. Chapelle, K. Chen, and G. Sun. A General Boosting Method and its Application to Learning Ranking Functions for Web Search.
 * Advances in Neural Information Processing Systems (NIPS), 2007.</a>
 * </ul>
 */
public class MultipleAdditiveTreesModel extends LTRScoringModel {

  /**
   * fname2index is filled from constructor arguments
   * (that are already part of the base class hashCode)
   * and therefore here it does not individually
   * influence the class hashCode, equals, etc.
   */
  private final HashMap<String,Integer> fname2index;
  /**
   * trees is part of the LTRScoringModel params map
   * and therefore here it does not individually
   * influence the class hashCode, equals, etc.
   */
  private List<RegressionTree> trees;

  private RegressionTree createRegressionTree(Map<String,Object> map) {
    final RegressionTree rt = new RegressionTree();
    if (map != null) {
      SolrPluginUtils.invokeSetters(rt, map.entrySet());
    }
    return rt;
  }

  private RegressionTreeNode createRegressionTreeNode(Map<String,Object> map) {
    final RegressionTreeNode rtn = new RegressionTreeNode();
    if (map != null) {
      SolrPluginUtils.invokeSetters(rtn, map.entrySet());
    }
    return rtn;
  }

  public class RegressionTreeNode {
    private static final float NODE_SPLIT_SLACK = 1E-6f;

    private float value = 0f;
    private String feature;
    private int featureIndex = -1;
    private Float threshold;
    private RegressionTreeNode left;
    private RegressionTreeNode right;

    public void setValue(float value) {
      this.value = value;
    }

    public void setValue(String value) {
      this.value = Float.parseFloat(value);
    }

    public void setFeature(String feature) {
      this.feature = feature;
      final Integer idx = fname2index.get(this.feature);
      // this happens if the tree specifies a feature that does not exist
      // this could be due to lambdaSmart building off of pre-existing trees
      // that use a feature that is no longer output during feature extraction
      featureIndex = (idx == null) ? -1 : idx;
    }

    public void setThreshold(float threshold) {
      this.threshold = threshold + NODE_SPLIT_SLACK;
    }

    public void setThreshold(String threshold) {
      this.threshold = Float.parseFloat(threshold) + NODE_SPLIT_SLACK;
    }

    @SuppressWarnings({"unchecked"})
    public void setLeft(Object left) {
      this.left = createRegressionTreeNode((Map<String,Object>) left);
    }

    @SuppressWarnings({"unchecked"})
    public void setRight(Object right) {
      this.right = createRegressionTreeNode((Map<String,Object>) right);
    }

    public boolean isLeaf() {
      return feature == null;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      if (isLeaf()) {
        sb.append(value);
      } else {
        sb.append("(feature=").append(feature);
        sb.append(",threshold=").append(threshold.floatValue()-NODE_SPLIT_SLACK);
        sb.append(",left=").append(left);
        sb.append(",right=").append(right);
        sb.append(')');
      }
      return sb.toString();
    }

    public RegressionTreeNode() {
    }

  }

  public class RegressionTree {

    private Float weight;
    private RegressionTreeNode root;

    public void setWeight(float weight) {
      this.weight = weight;
    }

    public void setWeight(String weight) {
      this.weight = Float.valueOf(weight);
    }

    @SuppressWarnings({"unchecked"})
    public void setRoot(Object root) {
      this.root = createRegressionTreeNode((Map<String,Object>)root);
    }

    public float score(float[] featureVector) {
      return weight.floatValue() * scoreNode(featureVector, root);
    }

    public String explain(float[] featureVector) {
      return explainNode(featureVector, root);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("(weight=").append(weight);
      sb.append(",root=").append(root);
      sb.append(")");
      return sb.toString();
    }

    public RegressionTree() {
    }

    public void validate() throws ModelException {
      if (weight == null) {
        throw new ModelException("MultipleAdditiveTreesModel tree doesn't contain a weight");
      }
      if (root == null) {
        throw new ModelException("MultipleAdditiveTreesModel tree doesn't contain a tree");
      } else {
        validateNode(root);
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  public void setTrees(Object trees) {
    this.trees = new ArrayList<RegressionTree>();
    for (final Object o : (List<Object>) trees) {
      final RegressionTree rt = createRegressionTree((Map<String,Object>) o);
      this.trees.add(rt);
    }
  }

  public MultipleAdditiveTreesModel(String name, List<Feature> features,
      List<Normalizer> norms,
      String featureStoreName, List<Feature> allFeatures,
      Map<String,Object> params) {
    super(name, features, norms, featureStoreName, allFeatures, params);

    fname2index = new HashMap<String,Integer>();
    for (int i = 0; i < features.size(); ++i) {
      final String key = features.get(i).getName();
      fname2index.put(key, i);
    }
  }

  @Override
  protected void validate() throws ModelException {
    super.validate();
    if (trees == null) {
      throw new ModelException("no trees declared for model "+name);
    }
    for (RegressionTree tree : trees) {
      tree.validate();
    }
  }

  @Override
  public float score(float[] modelFeatureValuesNormalized) {
    float score = 0;
    for (final RegressionTree t : trees) {
      score += t.score(modelFeatureValuesNormalized);
    }
    return score;
  }

  private static float scoreNode(float[] featureVector, RegressionTreeNode regressionTreeNode) {
    while (true) {
      if (regressionTreeNode.isLeaf()) {
        return regressionTreeNode.value;
      }
      // unsupported feature (tree is looking for a feature that does not exist)
      if ((regressionTreeNode.featureIndex < 0) || (regressionTreeNode.featureIndex >= featureVector.length)) {
        return 0f;
      }

      if (featureVector[regressionTreeNode.featureIndex] <= regressionTreeNode.threshold) {
        regressionTreeNode = regressionTreeNode.left;
      } else {
        regressionTreeNode = regressionTreeNode.right;
      }
    }
  }

  private static void validateNode(RegressionTreeNode regressionTreeNode) throws ModelException {
    
    // Create an empty stack and push root to it
    Stack<RegressionTreeNode> stack = new Stack<RegressionTreeNode>();
    stack.push(regressionTreeNode);

    while (stack.empty() == false) {
      RegressionTreeNode topStackNode = stack.pop();

      if (topStackNode.isLeaf()) {
        if (topStackNode.left != null || topStackNode.right != null) {
          throw new ModelException("MultipleAdditiveTreesModel tree node is leaf with left=" + topStackNode.left + " and right=" + topStackNode.right);
        }
        continue;
      }
      if (null == topStackNode.threshold) {
        throw new ModelException("MultipleAdditiveTreesModel tree node is missing threshold");
      }
      if (null == topStackNode.left) {
        throw new ModelException("MultipleAdditiveTreesModel tree node is missing left");
      } else {
        stack.push(topStackNode.left);
      }
      if (null == topStackNode.right) {
        throw new ModelException("MultipleAdditiveTreesModel tree node is missing right");
      } else {
        stack.push(topStackNode.right);
      }
    }
  }

  private static String explainNode(float[] featureVector, RegressionTreeNode regressionTreeNode) {
    final StringBuilder returnValueBuilder = new StringBuilder();
    while (true) {
      if (regressionTreeNode.isLeaf()) {
        returnValueBuilder.append("val: " + regressionTreeNode.value);
        return returnValueBuilder.toString();
      }

      // unsupported feature (tree is looking for a feature that does not exist)
      if ((regressionTreeNode.featureIndex < 0) || (regressionTreeNode.featureIndex >= featureVector.length)) {
        returnValueBuilder.append("'" + regressionTreeNode.feature + "' does not exist in FV, Return Zero");
        return returnValueBuilder.toString();
      }

      // could store extra information about how much training data supported
      // each branch and report
      // that here

      if (featureVector[regressionTreeNode.featureIndex] <= regressionTreeNode.threshold) {
        returnValueBuilder.append("'" + regressionTreeNode.feature + "':" + featureVector[regressionTreeNode.featureIndex] + " <= "
                + regressionTreeNode.threshold + ", Go Left | ");
        regressionTreeNode = regressionTreeNode.left;
      } else {
        returnValueBuilder.append("'" + regressionTreeNode.feature + "':" + featureVector[regressionTreeNode.featureIndex] + " > "
                + regressionTreeNode.threshold + ", Go Right | ");
        regressionTreeNode = regressionTreeNode.right;
      }
    }

  }


  // /////////////////////////////////////////
  // produces a string that looks like:
  // 40.0 = multipleadditivetreesmodel [ org.apache.solr.ltr.model.MultipleAdditiveTreesModel ]
  // model applied to
  // features, sum of:
  // 50.0 = tree 0 | 'matchedTitle':1.0 > 0.500001, Go Right |
  // 'this_feature_doesnt_exist' does not
  // exist in FV, Go Left | val: 50.0
  // -10.0 = tree 1 | val: -10.0
  @Override
  public Explanation explain(LeafReaderContext context, int doc,
      float finalScore, List<Explanation> featureExplanations) {
    final float[] fv = new float[featureExplanations.size()];
    int index = 0;
    for (final Explanation featureExplain : featureExplanations) {
      fv[index] = featureExplain.getValue().floatValue();
      index++;
    }

    final List<Explanation> details = new ArrayList<>();
    index = 0;

    for (final RegressionTree t : trees) {
      final float score = t.score(fv);
      final Explanation p = Explanation.match(score, "tree " + index + " | "
          + t.explain(fv));
      details.add(p);
      index++;
    }

    return Explanation.match(finalScore, toString()
        + " model applied to features, sum of:", details);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append("(name=").append(getName());
    sb.append(",trees=[");
    for (int ii = 0; ii < trees.size(); ++ii) {
      if (ii>0) {
        sb.append(',');
      }
      sb.append(trees.get(ii));
    }
    sb.append("])");
    return sb.toString();
  }

}
