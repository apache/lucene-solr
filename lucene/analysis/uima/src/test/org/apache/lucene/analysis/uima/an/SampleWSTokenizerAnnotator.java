package org.apache.lucene.analysis.uima.an;

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

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.JCasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

/**
 * Dummy implementation of a UIMA based whitespace tokenizer
 */
public class SampleWSTokenizerAnnotator extends JCasAnnotator_ImplBase {

  private final static String TOKEN_TYPE = "org.apache.lucene.uima.ts.TokenAnnotation";
  private final static String SENTENCE_TYPE = "org.apache.lucene.uima.ts.SentenceAnnotation";
  private String lineEnd;
  private static final String WHITESPACE = " ";

  @Override
  public void initialize(UimaContext aContext) throws ResourceInitializationException {
    super.initialize(aContext);
    lineEnd = String.valueOf(aContext.getConfigParameterValue("line-end"));
  }

  @Override
  public void process(JCas jCas) throws AnalysisEngineProcessException {
    Type sentenceType = jCas.getCas().getTypeSystem().getType(SENTENCE_TYPE);
    Type tokenType = jCas.getCas().getTypeSystem().getType(TOKEN_TYPE);
    int i = 0;
    for (String sentenceString : jCas.getDocumentText().split(lineEnd)) {
      // add the sentence
      AnnotationFS sentenceAnnotation = jCas.getCas().createAnnotation(sentenceType, i, sentenceString.length());
      jCas.addFsToIndexes(sentenceAnnotation);
      i += sentenceString.length();
    }

    // get tokens
    int j = 0;
    for (String tokenString : jCas.getDocumentText().split(WHITESPACE)) {
      int tokenLength = tokenString.length();
      AnnotationFS tokenAnnotation = jCas.getCas().createAnnotation(tokenType, j, j + tokenLength);
      jCas.addFsToIndexes(tokenAnnotation);
      j += tokenLength;
    }
  }

}
