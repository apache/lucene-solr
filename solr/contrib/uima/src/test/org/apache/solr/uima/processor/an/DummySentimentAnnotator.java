package org.apache.solr.uima.processor.an;

import java.util.Arrays;

import org.apache.solr.uima.ts.SentimentAnnotation;
import org.apache.uima.TokenAnnotation;
import org.apache.uima.analysis_component.JCasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;

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

public class DummySentimentAnnotator extends JCasAnnotator_ImplBase{

  private static final String[] positiveAdj = {"happy","cool","nice"};

  private static final String[] negativeAdj = {"bad","sad","ugly"};

  @Override
  public void process(JCas jcas) throws AnalysisEngineProcessException {
    for (Annotation annotation : jcas.getAnnotationIndex(TokenAnnotation.type)) {
      String tokenPOS = ((TokenAnnotation) annotation).getPosTag();
      if ("jj".equals(tokenPOS)) {
        if (Arrays.asList(positiveAdj).contains(annotation.getCoveredText())) {
          SentimentAnnotation sentimentAnnotation = createSentimentAnnotation(jcas, annotation);
          sentimentAnnotation.setMood("positive");
          sentimentAnnotation.addToIndexes();
        }
        else if (Arrays.asList(negativeAdj).contains(annotation.getCoveredText())) {
          SentimentAnnotation sentimentAnnotation = createSentimentAnnotation(jcas, annotation);
          sentimentAnnotation.setMood("negative");
          sentimentAnnotation.addToIndexes();
        }
      }
    }
  }

  private SentimentAnnotation createSentimentAnnotation(JCas jcas, Annotation annotation) {
    SentimentAnnotation sentimentAnnotation = new SentimentAnnotation(jcas);
    sentimentAnnotation.setBegin(annotation.getBegin());
    sentimentAnnotation.setEnd(annotation.getEnd());
    return sentimentAnnotation;
  }

}
