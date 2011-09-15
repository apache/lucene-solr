package org.apache.lucene.analysis;

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

import org.apache.lucene.document.Fieldable;

import java.io.Reader;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

/**
 * This analyzer is used to facilitate scenarios where different
 * fields require different analysis techniques.  Use the Map
 * argument in {@link #PerFieldAnalyzerWrapper(Analyzer, java.util.Map)}
 * to add non-default analyzers for fields.
 * 
 * <p>Example usage:
 * 
 * <pre>
 *   Map analyzerPerField = new HashMap();
 *   analyzerPerField.put("firstname", new KeywordAnalyzer());
 *   analyzerPerField.put("lastname", new KeywordAnalyzer());
 *
 *   PerFieldAnalyzerWrapper aWrapper =
 *      new PerFieldAnalyzerWrapper(new StandardAnalyzer(), analyzerPerField);
 * </pre>
 * 
 * <p>In this example, StandardAnalyzer will be used for all fields except "firstname"
 * and "lastname", for which KeywordAnalyzer will be used.
 * 
 * <p>A PerFieldAnalyzerWrapper can be used like any other analyzer, for both indexing
 * and query parsing.
 */
public final class PerFieldAnalyzerWrapper extends Analyzer {
  private final Analyzer defaultAnalyzer;
  private final Map<String,Analyzer> analyzerMap = new HashMap<String,Analyzer>();


  /**
   * Constructs with default analyzer.
   *
   * @param defaultAnalyzer Any fields not specifically
   * defined to use a different analyzer will use the one provided here.
   */
  public PerFieldAnalyzerWrapper(Analyzer defaultAnalyzer) {
    this(defaultAnalyzer, null);
  }
  
  /**
   * Constructs with default analyzer and a map of analyzers to use for 
   * specific fields.
   *
   * @param defaultAnalyzer Any fields not specifically
   * defined to use a different analyzer will use the one provided here.
   * @param fieldAnalyzers a Map (String field name to the Analyzer) to be 
   * used for those fields 
   */
  public PerFieldAnalyzerWrapper(Analyzer defaultAnalyzer, 
      Map<String,Analyzer> fieldAnalyzers) {
    this.defaultAnalyzer = defaultAnalyzer;
    if (fieldAnalyzers != null) {
      analyzerMap.putAll(fieldAnalyzers);
    }
  }
  

  /**
   * Defines an analyzer to use for the specified field.
   *
   * @param fieldName field name requiring a non-default analyzer
   * @param analyzer non-default analyzer to use for field
   * @deprecated Changing the Analyzer for a field after instantiation prevents
   *             reusability.  Analyzers for fields should be set during construction.
   */
  @Deprecated
  public void addAnalyzer(String fieldName, Analyzer analyzer) {
    analyzerMap.put(fieldName, analyzer);
  }

  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    Analyzer analyzer = analyzerMap.get(fieldName);
    if (analyzer == null) {
      analyzer = defaultAnalyzer;
    }

    return analyzer.tokenStream(fieldName, reader);
  }
  
  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    Analyzer analyzer = analyzerMap.get(fieldName);
    if (analyzer == null)
      analyzer = defaultAnalyzer;

    return analyzer.reusableTokenStream(fieldName, reader);
  }
  
  /** Return the positionIncrementGap from the analyzer assigned to fieldName */
  @Override
  public int getPositionIncrementGap(String fieldName) {
    Analyzer analyzer = analyzerMap.get(fieldName);
    if (analyzer == null)
      analyzer = defaultAnalyzer;
    return analyzer.getPositionIncrementGap(fieldName);
  }

  /** Return the offsetGap from the analyzer assigned to field */
  @Override
  public int getOffsetGap(Fieldable field) {
    Analyzer analyzer = analyzerMap.get(field.name());
    if (analyzer == null)
      analyzer = defaultAnalyzer;
    return analyzer.getOffsetGap(field);
  }
  
  @Override
  public String toString() {
    return "PerFieldAnalyzerWrapper(" + analyzerMap + ", default=" + defaultAnalyzer + ")";
  }
}
