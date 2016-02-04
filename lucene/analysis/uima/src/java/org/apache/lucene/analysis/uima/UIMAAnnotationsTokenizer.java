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
package org.apache.lucene.analysis.uima;


import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.AttributeFactory;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.resource.ResourceInitializationException;

import java.io.IOException;
import java.util.Map;

/**
 * a {@link Tokenizer} which creates tokens from UIMA Annotations
 */
public final class UIMAAnnotationsTokenizer extends BaseUIMATokenizer {

  private final CharTermAttribute termAttr;

  private final OffsetAttribute offsetAttr;

  private final String tokenTypeString;

  private int finalOffset = 0;

  public UIMAAnnotationsTokenizer(String descriptorPath, String tokenType, Map<String, Object> configurationParameters) {
    this(descriptorPath, tokenType, configurationParameters, DEFAULT_TOKEN_ATTRIBUTE_FACTORY);
  }

  public UIMAAnnotationsTokenizer(String descriptorPath, String tokenType, Map<String, Object> configurationParameters, 
                                  AttributeFactory factory) {
    super(factory, descriptorPath, configurationParameters);
    this.tokenTypeString = tokenType;
    this.termAttr = addAttribute(CharTermAttribute.class);
    this.offsetAttr = addAttribute(OffsetAttribute.class);
  }

  @Override
  protected void initializeIterator() throws IOException {
    try {
      analyzeInput();
    } catch (AnalysisEngineProcessException | ResourceInitializationException e) {
      throw new IOException(e);
    }
    finalOffset = correctOffset(cas.getDocumentText().length());
    Type tokenType = cas.getTypeSystem().getType(tokenTypeString);
    iterator = cas.getAnnotationIndex(tokenType).iterator();
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (iterator == null) {
      initializeIterator();
    }
    if (iterator.hasNext()) {
      clearAttributes();
      AnnotationFS next = iterator.next();
      termAttr.append(next.getCoveredText());
      offsetAttr.setOffset(correctOffset(next.getBegin()), correctOffset(next.getEnd()));
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void end() throws IOException {
    super.end();
    offsetAttr.setOffset(finalOffset, finalOffset);
  }
}
