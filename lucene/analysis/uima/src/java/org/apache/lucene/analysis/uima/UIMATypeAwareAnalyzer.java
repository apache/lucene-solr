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
package org.apache.lucene.analysis.uima;


import org.apache.lucene.analysis.Analyzer;

import java.util.Map;

/**
 * {@link Analyzer} which uses the {@link UIMATypeAwareAnnotationsTokenizer} for the tokenization phase
 */
public final class UIMATypeAwareAnalyzer extends Analyzer {
  private final String descriptorPath;
  private final String tokenType;
  private final String featurePath;
  private final Map<String, Object> configurationParameters;

  public UIMATypeAwareAnalyzer(String descriptorPath, String tokenType, String featurePath, Map<String, Object> configurationParameters) {
    this.descriptorPath = descriptorPath;
    this.tokenType = tokenType;
    this.featurePath = featurePath;
    this.configurationParameters = configurationParameters;
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    return new TokenStreamComponents(new UIMATypeAwareAnnotationsTokenizer(descriptorPath, tokenType, featurePath, configurationParameters));
  }
}
