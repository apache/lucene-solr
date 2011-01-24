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
import java.util.Map;

import org.apache.solr.core.SolrConfig;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Read configuration for Solr-UIMA integration
 * 
 * @version $Id$
 * 
 */
public class SolrUIMAConfigurationReader {

  private static final String AE_RUNTIME_PARAMETERS_NODE_PATH = "/config/uimaConfig/runtimeParameters";

  private static final String FIELD_MAPPING_NODE_PATH = "/config/uimaConfig/fieldMapping";

  private static final String ANALYZE_FIELDS_NODE_PATH = "/config/uimaConfig/analyzeFields";

  private static final String ANALYSIS_ENGINE_NODE_PATH = "/config/uimaConfig/analysisEngine";

  private SolrConfig solrConfig;

  public SolrUIMAConfigurationReader(SolrConfig solrConfig) {
    this.solrConfig = solrConfig;
  }

  public SolrUIMAConfiguration readSolrUIMAConfiguration() {
    return new SolrUIMAConfiguration(readAEPath(), readFieldsToAnalyze(), readFieldsMerging(),
            readTypesFeaturesFieldsMapping(), readAEOverridingParameters());
  }

  private String readAEPath() {
    return solrConfig.getNode(ANALYSIS_ENGINE_NODE_PATH, true).getTextContent();
  }

  private String[] readFieldsToAnalyze() {
    Node analyzeFieldsNode = solrConfig.getNode(ANALYZE_FIELDS_NODE_PATH, true);
    return analyzeFieldsNode.getTextContent().split(",");
  }

  private boolean readFieldsMerging() {
    Node analyzeFieldsNode = solrConfig.getNode(ANALYZE_FIELDS_NODE_PATH, true);
    Node mergeNode = analyzeFieldsNode.getAttributes().getNamedItem("merge");
    return Boolean.valueOf(mergeNode.getNodeValue());
  }

  private Map<String, Map<String, String>> readTypesFeaturesFieldsMapping() {
    Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();

    Node fieldMappingNode = solrConfig.getNode(FIELD_MAPPING_NODE_PATH, true);
    /* iterate over UIMA types */
    if (fieldMappingNode.hasChildNodes()) {
      NodeList typeNodes = fieldMappingNode.getChildNodes();
      for (int i = 0; i < typeNodes.getLength(); i++) {
        /* <type> node */
        Node typeNode = typeNodes.item(i);
        if (typeNode.getNodeType() != Node.TEXT_NODE) {
          Node typeNameAttribute = typeNode.getAttributes().getNamedItem("name");
          /* get a UIMA typename */
          String typeName = typeNameAttribute.getNodeValue();
          /* create entry for UIMA type */
          map.put(typeName, new HashMap<String, String>());
          if (typeNode.hasChildNodes()) {
            /* iterate over features */
            NodeList featuresNodeList = typeNode.getChildNodes();
            for (int j = 0; j < featuresNodeList.getLength(); j++) {
              Node mappingNode = featuresNodeList.item(j);
              if (mappingNode.getNodeType() != Node.TEXT_NODE) {
                /* get field name */
                Node fieldNameNode = mappingNode.getAttributes().getNamedItem("field");
                String mappedFieldName = fieldNameNode.getNodeValue();
                /* get feature name */
                Node featureNameNode = mappingNode.getAttributes().getNamedItem("feature");
                String featureName = featureNameNode.getNodeValue();
                /* map the feature to the field for the specified type */
                map.get(typeName).put(featureName, mappedFieldName);
              }
            }
          }
        }
      }
    }
    return map;
  }

  private Map<String, String> readAEOverridingParameters() {
    Map<String, String> runtimeParameters = new HashMap<String, String>();
    Node uimaConfigNode = solrConfig.getNode(AE_RUNTIME_PARAMETERS_NODE_PATH, true);

    if (uimaConfigNode.hasChildNodes()) {
      NodeList overridingNodes = uimaConfigNode.getChildNodes();
      for (int i = 0; i < overridingNodes.getLength(); i++) {
        Node overridingNode = overridingNodes.item(i);
        if (overridingNode.getNodeType() != Node.TEXT_NODE) {
          runtimeParameters.put(overridingNode.getNodeName(), overridingNode.getTextContent());
        }
      }
    }

    return runtimeParameters;
  }

}
