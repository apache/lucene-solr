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
package org.apache.solr.ltr;

/**
 * A feature logger that logs in csv format.
 */
public class CSVFeatureLogger extends FeatureLogger {
  public static final char DEFAULT_KEY_VALUE_SEPARATOR = '=';
  public static final char DEFAULT_FEATURE_SEPARATOR = ',';
  private final char keyValueSep;
  private final char featureSep;

  public CSVFeatureLogger(String fvCacheName, FeatureFormat f) {
    super(fvCacheName, f);
    this.keyValueSep = DEFAULT_KEY_VALUE_SEPARATOR;
    this.featureSep = DEFAULT_FEATURE_SEPARATOR;
  }

  public CSVFeatureLogger(String fvCacheName, FeatureFormat f, char keyValueSep, char featureSep) {
    super(fvCacheName, f);
    this.keyValueSep = keyValueSep;
    this.featureSep = featureSep;
  }

  @Override
  public String makeFeatureVector(LTRScoringQuery.FeatureInfo[] featuresInfo) {
    // Allocate the buffer to a size based on the number of features instead of the 
    // default 16.  You need space for the name, value, and two separators per feature, 
    // but not all the features are expected to fire, so this is just a naive estimate. 
    StringBuilder sb = new StringBuilder(featuresInfo.length * 3);
    boolean isDense = featureFormat.equals(FeatureFormat.DENSE);
    for (LTRScoringQuery.FeatureInfo featInfo:featuresInfo) {
      if (featInfo.isUsed() || isDense){
        sb.append(featInfo.getName())
        .append(keyValueSep)
        .append(featInfo.getValue())
        .append(featureSep);
      }
    }

    final String features = (sb.length() > 0 ? 
        sb.substring(0, sb.length() - 1) : "");

    return features;
  }

}