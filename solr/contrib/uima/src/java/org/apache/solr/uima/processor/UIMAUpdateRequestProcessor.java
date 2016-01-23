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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.uima.processor.SolrUIMAConfiguration.MapField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.JCasPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Update document(s) to be indexed with UIMA extracted information
 * 
 */
public class UIMAUpdateRequestProcessor extends UpdateRequestProcessor {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private SolrUIMAConfiguration solrUIMAConfiguration;
  
  private AnalysisEngine ae;
  
  private JCasPool pool;
  
  public UIMAUpdateRequestProcessor(UpdateRequestProcessor next,
      String coreName, SolrUIMAConfiguration config, AnalysisEngine ae,
      JCasPool pool) {
    super(next);
    this.ae = ae;
    this.pool = pool;
    solrUIMAConfiguration = config;
  }
  
  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    String text = null;
    try {
      /* get Solr document */
      SolrInputDocument solrInputDocument = cmd.getSolrInputDocument();

      /* get the fields to analyze */
      String[] texts = getTextsToAnalyze(solrInputDocument);
      for (String currentText : texts) {
        text = currentText;
        if (text != null && text.length() > 0) {
          /* create a JCas which contain the text to analyze */
          JCas jcas = pool.getJCas(0);
          try {
            /* process the text value */
            processText(text, jcas);

            UIMAToSolrMapper uimaToSolrMapper = new UIMAToSolrMapper(
                solrInputDocument, jcas);
            /* get field mapping from config */
            Map<String,Map<String,MapField>> typesAndFeaturesFieldsMap = solrUIMAConfiguration
                .getTypesFeaturesFieldsMapping();
            /* map type features on fields */
            for (Entry<String,Map<String,MapField>> entry : typesAndFeaturesFieldsMap
                .entrySet()) {
              uimaToSolrMapper.map(entry.getKey(), entry.getValue());
            }
          } finally {
            pool.releaseJCas(jcas);
          }
        }
      }
    } catch (Exception e) {
      String logField = solrUIMAConfiguration.getLogField();
      if (logField == null) {
        SchemaField uniqueKeyField = cmd.getReq().getSchema()
            .getUniqueKeyField();
        if (uniqueKeyField != null) {
          logField = uniqueKeyField.getName();
        }
      }
      String optionalFieldInfo = logField == null ? "." : ". " + logField + "=" + cmd.getSolrInputDocument().
          getField(logField).getValue() + ", ";
      int len;
      String debugString;
      if (text != null && text.length() > 0) {
        len = Math.min(text.length(), 100);
        debugString = " text=\"" + text.substring(0, len) + "...\"";
      } else {
        debugString = " null text";
      }
      if (solrUIMAConfiguration.isIgnoreErrors()) {
        log.warn(
            "skip the text processing due to {}",
            new StringBuilder().append(e.getLocalizedMessage())
                .append(optionalFieldInfo).append(debugString));
      } else {
        throw new SolrException(ErrorCode.SERVER_ERROR, "processing error " + e.getLocalizedMessage() +
            optionalFieldInfo + debugString, e);
      }
    }
    super.processAdd(cmd);
  }
  
  /*
   * get the texts to analyze from the corresponding fields
   */
  private String[] getTextsToAnalyze(SolrInputDocument solrInputDocument) {
    String[] fieldsToAnalyze = solrUIMAConfiguration.getFieldsToAnalyze();
    boolean merge = solrUIMAConfiguration.isFieldsMerging();
    String[] textVals;
    if (merge) {
      StringBuilder unifiedText = new StringBuilder("");
      for (String aFieldsToAnalyze : fieldsToAnalyze) {
        if (solrInputDocument.getFieldValues(aFieldsToAnalyze) != null) {
          Object[] Values = solrInputDocument.getFieldValues(aFieldsToAnalyze).toArray();
          for (Object Value : Values) {
            if (unifiedText.length() > 0) {
              unifiedText.append(' ');
            }
            unifiedText.append(Value.toString());
          }
        }
      }
      textVals = new String[1];
      textVals[0] = unifiedText.toString();
    } else {
      textVals = new String[fieldsToAnalyze.length];
      for (int i = 0; i < fieldsToAnalyze.length; i++) {
        if (solrInputDocument.getFieldValues(fieldsToAnalyze[i]) != null) {
          Object[] Values = solrInputDocument.getFieldValues(fieldsToAnalyze[i]).toArray();
          for (Object Value : Values) {
            textVals[i] += Value.toString();
          }
        }
      }
    }
    return textVals;
  }
  
  /*
   * process a field value executing UIMA on the JCas containing it as document
   * text
   */
  private void processText(String textFieldValue, JCas jcas)
      throws ResourceInitializationException, AnalysisEngineProcessException {
    if (log.isDebugEnabled()) {
      log.debug("Analyzing text");
    }

    jcas.setDocumentText(textFieldValue);

    /* perform analysis on text field */
    ae.process(jcas);
    if (log.isDebugEnabled()) {
      log.debug("Text processing completed");
    }
  }

  /**
   * @return the configuration object for this request processor
   */
  public SolrUIMAConfiguration getConfiguration()
  {
    return solrUIMAConfiguration;
  }
}
