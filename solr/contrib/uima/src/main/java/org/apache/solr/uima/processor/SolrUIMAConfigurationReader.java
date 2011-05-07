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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.NamedList;

/**
 * Read configuration for Solr-UIMA integration
 * 
 * @version $Id$
 * 
 */
public class SolrUIMAConfigurationReader {

  private NamedList<Object> args;

  public SolrUIMAConfigurationReader(NamedList<Object> args) {
    this.args = args;
  }

  public SolrUIMAConfiguration readSolrUIMAConfiguration() {
    return new SolrUIMAConfiguration(readAEPath(), readFieldsToAnalyze(), readFieldsMerging(),
            readTypesFeaturesFieldsMapping(), readAEOverridingParameters());
  }

  private String readAEPath() {
    return (String) args.get("analysisEngine");
  }

  @SuppressWarnings("rawtypes")
  private NamedList getAnalyzeFields() {
    return (NamedList) args.get("analyzeFields");
  }

  @SuppressWarnings("unchecked")
  private String[] readFieldsToAnalyze() {
    List<String> fields = (List<String>) getAnalyzeFields().get("fields");
    return fields.toArray(new String[fields.size()]);
  }

  private boolean readFieldsMerging() {
    return (Boolean) getAnalyzeFields().get("merge");
  }

  @SuppressWarnings("rawtypes")
  private Map<String, Map<String, String>> readTypesFeaturesFieldsMapping() {
    Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();

    NamedList fieldMappings = (NamedList) args.get("fieldMappings");
    /* iterate over UIMA types */
    for (int i = 0; i < fieldMappings.size(); i++) {
      NamedList type = (NamedList) fieldMappings.get("type", i);
      String typeName = (String)type.get("name");

      Map<String, String> subMap = new HashMap<String, String>();
      /* iterate over mapping definitions */
      for(int j = 0; j < type.size() - 1; j++){
        NamedList mapping = (NamedList) type.get("mapping", j + 1);
        String featureName = (String) mapping.get("feature");
        String mappedFieldName = (String) mapping.get("field");
        subMap.put(featureName, mappedFieldName);
      }
      map.put(typeName, subMap);
    }
    return map;
  }

  @SuppressWarnings("rawtypes")
  private Map<String, Object> readAEOverridingParameters() {
    Map<String, Object> runtimeParameters = new HashMap<String, Object>();
    NamedList runtimeParams = (NamedList) args.get("runtimeParameters");
    for (int i = 0; i < runtimeParams.size(); i++) {
      String name = runtimeParams.getName(i);
      Object value = runtimeParams.getVal(i);
      runtimeParameters.put(name, value);
    }
    return runtimeParameters;
  }

}
