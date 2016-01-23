package org.apache.lucene.analysis.uima;

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

import org.apache.lucene.analysis.Analyzer;

import java.io.Reader;
import java.util.Map;

/**
 * An {@link Analyzer} which use the {@link UIMAAnnotationsTokenizer} for creating tokens
 */
public final class UIMABaseAnalyzer extends Analyzer {

  private final String descriptorPath;
  private final String tokenType;
  private final Map<String, Object> configurationParameters;

  public UIMABaseAnalyzer(String descriptorPath, String tokenType, Map<String, Object> configurationParameters) {
    this.descriptorPath = descriptorPath;
    this.tokenType = tokenType;
    this.configurationParameters = configurationParameters;
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    return new TokenStreamComponents(new UIMAAnnotationsTokenizer(descriptorPath, tokenType, configurationParameters));
  }

}
