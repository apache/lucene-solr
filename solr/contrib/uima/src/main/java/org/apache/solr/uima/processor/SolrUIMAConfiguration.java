package org.apache.solr.uima.processor;

/**
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

import java.util.Map;

/**
 * Configuration holding all the configurable parameters for calling UIMA inside Solr
 * 
 * @version $Id$
 */
public class SolrUIMAConfiguration {

  private String[] fieldsToAnalyze;

  private boolean fieldsMerging;

  private Map<String, Map<String, String>> typesFeaturesFieldsMapping;

  private String aePath;

  private Map<String, Object> runtimeParameters;

  public SolrUIMAConfiguration(String aePath, String[] fieldsToAnalyze, boolean fieldsMerging,
          Map<String, Map<String, String>> typesFeaturesFieldsMapping,
          Map<String, Object> runtimeParameters) {
    this.aePath = aePath;
    this.fieldsToAnalyze = fieldsToAnalyze;
    this.fieldsMerging = fieldsMerging;
    this.runtimeParameters = runtimeParameters;
    this.typesFeaturesFieldsMapping = typesFeaturesFieldsMapping;
  }

  public String[] getFieldsToAnalyze() {
    return fieldsToAnalyze;
  }

  public boolean isFieldsMerging() {
    return fieldsMerging;
  }

  public Map<String, Map<String, String>> getTypesFeaturesFieldsMapping() {
    return typesFeaturesFieldsMapping;
  }

  public String getAePath() {
    return aePath;
  }

  public Map<String, Object> getRuntimeParameters() {
    return runtimeParameters;
  }

}
