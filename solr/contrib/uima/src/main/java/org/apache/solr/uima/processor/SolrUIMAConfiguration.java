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

  private Map<String, Map<String, MapField>> typesFeaturesFieldsMapping;

  private String aePath;

  private Map<String, Object> runtimeParameters;

  public SolrUIMAConfiguration(String aePath, String[] fieldsToAnalyze, boolean fieldsMerging,
          Map<String, Map<String, MapField>> typesFeaturesFieldsMapping,
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

  public Map<String, Map<String, MapField>> getTypesFeaturesFieldsMapping() {
    return typesFeaturesFieldsMapping;
  }

  public String getAePath() {
    return aePath;
  }

  public Map<String, Object> getRuntimeParameters() {
    return runtimeParameters;
  }
  
  static final class MapField {
    
    private String fieldName, fieldNameFeature;
    private boolean prefix; // valid if dynamicField == true
                            // false: *_s, true: s_*
    
    MapField(String fieldName, String fieldNameFeature){
      this.fieldName = fieldName;
      this.fieldNameFeature = fieldNameFeature;
      if(fieldNameFeature != null){
        if(fieldName.startsWith("*")){
          prefix = false;
          this.fieldName = fieldName.substring(1);
        }
        else if(fieldName.endsWith("*")){
          prefix = true;
          this.fieldName = fieldName.substring(0, fieldName.length() - 1);
        }
        else
          throw new RuntimeException("static field name cannot be used for dynamicField");
      }
    }
    
    String getFieldNameFeature(){
      return fieldNameFeature;
    }
    
    String getFieldName(String featureValue){
      if(fieldNameFeature != null){
        return prefix ? fieldName + featureValue : featureValue + fieldName;
      }
      return fieldName;
    }
  }
}
