package org.apache.solr.uima.processor.an;

import org.apache.solr.uima.ts.EntityAnnotation;
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

public class DummyEntityAnnotator extends JCasAnnotator_ImplBase{

  @Override
  public void process(JCas jcas) throws AnalysisEngineProcessException {
    for (Annotation annotation : jcas.getAnnotationIndex(TokenAnnotation.type)) {
      String tokenPOS = ((TokenAnnotation) annotation).getPosTag();
      if ("np".equals(tokenPOS) || "nps".equals(tokenPOS)) {
        EntityAnnotation entityAnnotation = new EntityAnnotation(jcas);
        entityAnnotation.setBegin(annotation.getBegin());
        entityAnnotation.setEnd(annotation.getEnd());
        String entityString = annotation.getCoveredText();
        entityAnnotation.setEntity(entityString);
        String name = "OTHER"; // "OTHER" makes no sense. In practice, "PERSON", "COUNTRY", "E-MAIL", etc.
        if(entityString.equals("Apache"))
          name = "ORGANIZATION";
        entityAnnotation.setName(name);
        entityAnnotation.addToIndexes();
      }
    }
  }

}
