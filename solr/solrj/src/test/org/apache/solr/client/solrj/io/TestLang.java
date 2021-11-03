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
package org.apache.solr.client.solrj.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorDay;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorDayOfQuarter;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorDayOfYear;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorEpoch;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorHour;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorMinute;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorMonth;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorQuarter;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorSecond;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorWeek;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorYear;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

@Slow
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class TestLang extends SolrTestCase {

  private static final String[] allFunctions = {
      "search", "facet", "facet2D", "update", "delete", "topic", "commit", "random", "knnSearch", "merge",
      "unique", "top", "group", "reduce", "parallel", "rollup", "stats", "innerJoin",
      "leftOuterJoin", "hashJoin", "outerHashJoin", "intersect", "complement", "sort",
      "train", "features", "daemon", "shortestPath", "gatherNodes", "nodes",
      "select", "shortestPath", "gatherNodes", "nodes", "scoreNodes", "model", "fetch",
      "executor", "null", "priority", "significantTerms", "cartesianProduct",
      "shuffle", "calc", "eval", "echo", "cell", "list", "let", "get", "timeseries", "tuple", "sql", "min",
      "max", "avg", "sum", "count", "replace", "concat", "group", "distinct", "having",
      "val", "anova", "array", "col", "conv", "copyOfRange", "copyOf", "cov", "corr", "describe", "distance", "empiricalDistribution",
      "finddelay", "hist", "length", "movingAvg", "standardize", "percentile", "predict", "rank", "regress", "rev", "scale",
      "sequence", "addAll", "append", "plot", "normalDistribution", "uniformDistribution", "sample", "kolmogorovSmirnov",
      "ks", "asc", "cumulativeProbability", "ebeAdd", "ebeSubtract", "ebeMultiply", "ebeDivide",
      "dotProduct", "cosineSimilarity", "freqTable", "uniformIntegerDistribution", "binomialDistribution",
      "poissonDistribution", "enumeratedDistribution", "probability", "sumDifference", "meanDifference",
      "primes", "factorial", "movingMedian", "binomialCoefficient", "expMovingAvg", "monteCarlo", "constantDistribution",
      "weibullDistribution", "mean", "mode", "logNormalDistribution", "zipFDistribution", "gammaDistribution",
      "betaDistribution", "polyfit", "harmonicFit", "harmfit", "loess", "matrix", "transpose", "unitize",
      "triangularDistribution", "precision", "minMaxScale", "markovChain", "grandSum",
      "scalarAdd", "scalarSubtract", "scalarMultiply", "scalarDivide", "sumRows",
      "sumColumns", "diff", "corrPValues", "normalizeSum", "geometricDistribution", "olsRegress",
      "derivative", "spline", "ttest", "pairedTtest", "multiVariateNormalDistribution", "integral",
      "density", "mannWhitney", "sumSq", "akima", "lerp", "chiSquareDataSet", "gtestDataSet",
      "termVectors", "getColumnLabels", "getRowLabels", "getAttribute", "kmeans", "getCentroids",
      "getCluster", "topFeatures", "featureSelect", "rowAt", "colAt", "setColumnLabels",
      "setRowLabels", "knn", "getAttributes", "indexOf", "columnCount", "rowCount", "fuzzyKmeans",
      "getMembershipMatrix", "multiKmeans", "l2norm", "l1norm", "linfnorm", "matrixMult",
      "bicubicSpline", "and", "eor", "eq", "gt", "gteq", "lt", "lteq", "not", "or", TemporalEvaluatorYear.FUNCTION_NAME,
       TemporalEvaluatorMonth.FUNCTION_NAME, TemporalEvaluatorDay.FUNCTION_NAME, TemporalEvaluatorDayOfYear.FUNCTION_NAME,
       TemporalEvaluatorHour.FUNCTION_NAME, TemporalEvaluatorMinute.FUNCTION_NAME, TemporalEvaluatorSecond.FUNCTION_NAME,
       TemporalEvaluatorEpoch.FUNCTION_NAME, TemporalEvaluatorWeek.FUNCTION_NAME, TemporalEvaluatorQuarter.FUNCTION_NAME,
       TemporalEvaluatorDayOfQuarter.FUNCTION_NAME, "abs", "add", "div", "mult", "sub", "log", "pow",
      "mod", "ceil", "floor", "sin", "asin", "sinh", "cos", "acos", "cosh", "tan", "atan", "tanh", "round", "sqrt",
      "cbrt", "coalesce", "uuid", "if", "convert", "valueAt", "memset", "fft", "ifft", "euclidean","manhattan",
      "earthMovers", "canberra", "chebyshev", "ones", "zeros", "setValue", "getValue", "knnRegress", "gaussfit",
      "outliers", "stream", "getCache", "putCache", "listCache", "removeCache", "zscores", "latlonVectors",
      "convexHull", "getVertices", "getBaryCenter", "getArea", "getBoundarySize","oscillate",
      "getAmplitude", "getPhase", "getAngularFrequency", "enclosingDisk", "getCenter", "getRadius",
      "getSupportPoints", "pairSort", "log10", "plist", "recip", "pivot", "ltrim", "rtrim", "export",
      "zplot", "natural", "repeat", "movingMAD", "hashRollup", "noop", "var", "stddev", "recNum", "isNull",
      "notNull", "matches", "projectToBorder", "double", "long", "parseCSV", "parseTSV", "dateTime",
       "split", "upper", "trim", "lower", "trunc", "cosine", "dbscan", "per", "std", "drill", "input", "countDist"};

  @Test
  public void testLang() {
    List<String> functions = new ArrayList<>();
    for(String f : allFunctions) {
      functions.add(f);
    }
    StreamFactory factory = new StreamFactory();
    Lang.register(factory);
    Map<String, Supplier<Class<? extends Expressible>>> registeredFunctions = factory.getFunctionNames();

    //Check that each function that is expected is registered.
    for(String func : functions) {
      assertTrue("Testing function:"+func, registeredFunctions.containsKey(func));
    }

    //Check that each function that is registered is expected.
    Set<String> keys = registeredFunctions.keySet();
    for(String key : keys) {
      assertTrue("Testing key:"+key, functions.contains(key));
    }
  }
}
