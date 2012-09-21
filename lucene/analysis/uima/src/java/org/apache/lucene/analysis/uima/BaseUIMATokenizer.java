package org.apache.lucene.analysis.uima;

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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.uima.ae.AEProviderFactory;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.resource.ResourceInitializationException;

import java.io.IOException;
import java.io.Reader;

/**
 * Abstract base implementation of a {@link Tokenizer} which is able to analyze the given input with a
 * UIMA {@link AnalysisEngine}
 */
public abstract class BaseUIMATokenizer extends Tokenizer {

  protected FSIterator<AnnotationFS> iterator;
  protected final AnalysisEngine ae;
  protected final CAS cas;

  protected BaseUIMATokenizer(Reader reader, String descriptorPath) {
    super(reader);
    try {
      ae = AEProviderFactory.getInstance().getAEProvider(descriptorPath).getAE();
      cas = ae.newCAS();
    } catch (ResourceInitializationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * analyzes the tokenizer input using the given analysis engine
   * <p/>
   * {@link #cas} will be filled with  extracted metadata (UIMA annotations, feature structures)
   *
   * @throws AnalysisEngineProcessException
   * @throws IOException
   */
  protected void analyzeInput() throws AnalysisEngineProcessException, IOException {
    cas.reset();
    cas.setDocumentText(toString(input));
    ae.process(cas);
  }

  /**
   * initialize the FSIterator which is used to build tokens at each incrementToken() method call
   *
   * @throws IOException
   */
  protected abstract void initializeIterator() throws IOException;

  private String toString(Reader reader) throws IOException {
    StringBuilder stringBuilder = new StringBuilder();
    int ch;
    while ((ch = reader.read()) > -1) {
      stringBuilder.append((char) ch);
    }
    return stringBuilder.toString();
  }

  @Override
  public void reset() throws IOException {
    iterator = null;
  }

  @Override
  public void end() throws IOException {
    iterator = null;
  }


}
