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

import java.util.HashMap;
import java.util.Map;

public class ClassificationEvaluation {
  private long truePositive;
  private long falsePositive;
  private long trueNegative;
  private long falseNegative;

  public void count(int actual, int predicted) {
    if (predicted == 1) {
      if (actual == 1) truePositive++;
      else falsePositive++;
    } else {
      if (actual == 0) trueNegative++;
      else falseNegative++;
    }
  }

  @SuppressWarnings({"unchecked"})
  public void putToMap(@SuppressWarnings({"rawtypes"})Map map) {
    map.put("truePositive_i",truePositive);
    map.put("trueNegative_i",trueNegative);
    map.put("falsePositive_i",falsePositive);
    map.put("falseNegative_i",falseNegative);
  }

  @SuppressWarnings({"rawtypes"})
  public Map toMap() {
    HashMap map = new HashMap();
    putToMap(map);
    return map;
  }

  public static ClassificationEvaluation create(@SuppressWarnings({"rawtypes"})Map map) {
    ClassificationEvaluation evaluation = new ClassificationEvaluation();
    evaluation.addEvaluation(map);
    return evaluation;
  }

  public void addEvaluation(@SuppressWarnings({"rawtypes"})Map map) {
    this.truePositive += (long) map.get("truePositive_i");
    this.trueNegative += (long) map.get("trueNegative_i");
    this.falsePositive += (long) map.get("falsePositive_i");
    this.falseNegative += (long) map.get("falseNegative_i");
  }

  public double getPrecision() {
    if (truePositive + falsePositive == 0) return 0;
    return (double) truePositive / (truePositive + falsePositive);
  }

  public double getRecall() {
    if (truePositive + falseNegative == 0) return 0;
    return (double) truePositive / (truePositive + falseNegative);
  }

  public double getF1() {
    double precision = getPrecision();
    double recall = getRecall();
    if (precision + recall == 0) return 0;
    return 2 * (precision * recall) / (precision + recall);
  }

  public double getAccuracy() {
    return (double) (truePositive + trueNegative) / (truePositive + trueNegative + falseNegative + falsePositive);
  }
}
