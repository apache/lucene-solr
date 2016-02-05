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
package org.apache.lucene.analysis.uima.an;


import org.apache.uima.TokenAnnotation;
import org.apache.uima.analysis_component.JCasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;

/**
 * Dummy implementation of an entity annotator to tag tokens as certain types of entities
 */
public class SampleEntityAnnotator extends JCasAnnotator_ImplBase {

  private static final String NP = "np";
  private static final String NPS = "nps";
  private static final String TYPE_NAME = "org.apache.lucene.analysis.uima.ts.EntityAnnotation";
  private static final String ENTITY_FEATURE = "entity";
  private static final String NAME_FEATURE = "entity";

  @Override
  public void process(JCas jcas) throws AnalysisEngineProcessException {
    Type type = jcas.getCas().getTypeSystem().getType(TYPE_NAME);
    Feature entityFeature = type.getFeatureByBaseName(ENTITY_FEATURE);
    Feature nameFeature = type.getFeatureByBaseName(NAME_FEATURE);

    for (Annotation annotation : jcas.getAnnotationIndex(TokenAnnotation.type)) {
      String tokenPOS = ((TokenAnnotation) annotation).getPosTag();

      if (NP.equals(tokenPOS) || NPS.equals(tokenPOS)) {
        AnnotationFS entityAnnotation = jcas.getCas().createAnnotation(type, annotation.getBegin(), annotation.getEnd());

        entityAnnotation.setStringValue(entityFeature, annotation.getCoveredText());

        String name = "OTHER"; // "OTHER" makes no sense. In practice, "PERSON", "COUNTRY", "E-MAIL", etc.
        if (annotation.getCoveredText().equals("Apache"))
          name = "ORGANIZATION";
        entityAnnotation.setStringValue(nameFeature, name);

        jcas.addFsToIndexes(entityAnnotation);
      }
    }
  }

}
