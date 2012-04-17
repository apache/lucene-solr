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

import org.apache.uima.analysis_component.JCasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;

/**
 * Dummy implementation of a PoS tagger to add part of speech as token types
 */
public class SamplePoSTagger extends JCasAnnotator_ImplBase {

  private static final String NUM = "NUM";
  private static final String WORD = "WORD";
  private static final String TYPE_NAME = "org.apache.lucene.uima.ts.TokenAnnotation";
  private static final String FEATURE_NAME = "pos";

  @Override
  public void process(JCas jcas) throws AnalysisEngineProcessException {
    Type type = jcas.getCas().getTypeSystem().getType(TYPE_NAME);
    Feature posFeature = type.getFeatureByBaseName(FEATURE_NAME);

    for (Annotation annotation : jcas.getAnnotationIndex(type)) {
      String text = annotation.getCoveredText();
      String pos = extractPoS(text);
      annotation.setStringValue(posFeature, pos);
    }
  }

  private String extractPoS(String text) {
    try {
      Double.valueOf(text);
      return NUM;
    } catch (Exception e) {
      return WORD;
    }
  }
}
