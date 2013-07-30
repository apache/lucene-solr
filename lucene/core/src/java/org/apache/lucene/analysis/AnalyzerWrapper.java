package org.apache.lucene.analysis;

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

import java.io.Reader;

/**
 * Extension to {@link Analyzer} suitable for Analyzers which wrap
 * other Analyzers.
 * <p/>
 * {@link #getWrappedAnalyzer(String)} allows the Analyzer
 * to wrap multiple Analyzers which are selected on a per field basis.
 * <p/>
 * {@link #wrapComponents(String, Analyzer.TokenStreamComponents)} allows the
 * TokenStreamComponents of the wrapped Analyzer to then be wrapped
 * (such as adding a new {@link TokenFilter} to form new TokenStreamComponents.
 */
public abstract class AnalyzerWrapper extends Analyzer {

  /**
   * Creates a new AnalyzerWrapper.  Since the {@link Analyzer.ReuseStrategy} of
   * the wrapped Analyzers are unknown, {@link Analyzer.PerFieldReuseStrategy} is assumed
   */
  protected AnalyzerWrapper() {
    super(new PerFieldReuseStrategy());
  }

  /**
   * Retrieves the wrapped Analyzer appropriate for analyzing the field with
   * the given name
   *
   * @param fieldName Name of the field which is to be analyzed
   * @return Analyzer for the field with the given name.  Assumed to be non-null
   */
  protected abstract Analyzer getWrappedAnalyzer(String fieldName);

  /**
   * Wraps / alters the given TokenStreamComponents, taken from the wrapped
   * Analyzer, to form new components. It is through this method that new
   * TokenFilters can be added by AnalyzerWrappers. By default, the given
   * components are returned.
   * 
   * @param fieldName
   *          Name of the field which is to be analyzed
   * @param components
   *          TokenStreamComponents taken from the wrapped Analyzer
   * @return Wrapped / altered TokenStreamComponents.
   */
  protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
    return components;
  }

  /**
   * Wraps / alters the given Reader. Through this method AnalyzerWrappers can
   * implement {@link #initReader(String, Reader)}. By default, the given reader
   * is returned.
   * 
   * @param fieldName
   *          name of the field which is to be analyzed
   * @param reader
   *          the reader to wrap
   * @return the wrapped reader
   */
  protected Reader wrapReader(String fieldName, Reader reader) {
    return reader;
  }
  
  @Override
  protected final TokenStreamComponents createComponents(String fieldName, Reader aReader) {
    return wrapComponents(fieldName, getWrappedAnalyzer(fieldName).createComponents(fieldName, aReader));
  }

  @Override
  public final int getPositionIncrementGap(String fieldName) {
    return getWrappedAnalyzer(fieldName).getPositionIncrementGap(fieldName);
  }

  @Override
  public final int getOffsetGap(String fieldName) {
    return getWrappedAnalyzer(fieldName).getOffsetGap(fieldName);
  }

  @Override
  public final Reader initReader(String fieldName, Reader reader) {
    return getWrappedAnalyzer(fieldName).initReader(fieldName, wrapReader(fieldName, reader));
  }
}
