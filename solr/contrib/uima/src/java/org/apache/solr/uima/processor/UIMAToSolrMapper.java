package org.apache.solr.uima.processor;

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

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.uima.processor.SolrUIMAConfiguration.MapField;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.Type;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;

/**
 * Map UIMA types and features over fields of a Solr document
 * 
 *
 */
public class UIMAToSolrMapper {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrInputDocument document;

  private final JCas cas;

  public UIMAToSolrMapper(SolrInputDocument document, JCas cas) {
    this.document = document;
    this.cas = cas;
  }

  /**
   * map features of a certain UIMA type to corresponding Solr fields based on the mapping
   *
   * @param typeName             name of UIMA type to map
   */
  void map(String typeName, Map<String, MapField> featureFieldsmapping) throws FieldMappingException {
    try {
      Type type = cas.getTypeSystem().getType(typeName);
      for (FSIterator<FeatureStructure> iterator = cas.getFSIndexRepository().getAllIndexedFS(type); iterator
          .hasNext(); ) {
        FeatureStructure fs = iterator.next();
        for (String featureName : featureFieldsmapping.keySet()) {
          MapField mapField = featureFieldsmapping.get(featureName);
          String fieldNameFeature = mapField.getFieldNameFeature();
          String fieldNameFeatureValue = fieldNameFeature == null ? null :
              fs.getFeatureValueAsString(type.getFeatureByBaseName(fieldNameFeature));
          String fieldName = mapField.getFieldName(fieldNameFeatureValue);
          if (log.isInfoEnabled()) {
            log.info("mapping {}@{} to {}", new Object[]{typeName, featureName, fieldName});
          }
          String featureValue;
          if (fs instanceof Annotation && "coveredText".equals(featureName)) {
            featureValue = ((Annotation) fs).getCoveredText();
          } else {
            featureValue = fs.getFeatureValueAsString(type.getFeatureByBaseName(featureName));
          }
          if (log.isDebugEnabled()) {
            log.debug("writing {} in {}", new Object[]{featureValue, fieldName});
          }
          document.addField(fieldName, featureValue, 1.0f);
        }
      }
    } catch (Exception e) {
      throw new FieldMappingException(e);
    }
  }

}
