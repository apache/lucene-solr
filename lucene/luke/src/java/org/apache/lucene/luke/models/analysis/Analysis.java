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

package org.apache.lucene.luke.models.analysis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.luke.models.LukeException;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A dedicated interface for Luke's Analysis tab.
 */
public interface Analysis {

  /**
   * Holder for a token.
   */
  class Token {
    private final String term;
    private final List<TokenAttribute> attributes;

    Token(@Nonnull String term, @Nonnull List<TokenAttribute> attributes) {
      this.term = term;
      this.attributes = attributes;
    }

    /**
     * Returns the string representation of this token.
     */
    public String getTerm() {
      return term;
    }

    /**
     * Returns attributes of this token.
     */
    public List<TokenAttribute> getAttributes() {
      return ImmutableList.copyOf(attributes);
    }
  }

  /**
   * Holder for a token attribute.
   */
  class TokenAttribute {
    private final String attClass;
    private final Map<String, String> attValues;

    TokenAttribute(@Nonnull String attClass, @Nonnull Map<String, String> attValues) {
      this.attClass = attClass;
      this.attValues = attValues;
    }

    /**
     * Returns attribute class name.
     */
    public String getAttClass() {
      return attClass;
    }

    /**
     * Returns value of this attribute.
     */
    public Map<String, String> getAttValues() {
      return ImmutableMap.copyOf(attValues);
    }
  }

  /**
   * Returns built-in {@link Analyzer}s.
   */
  Collection<Class<? extends Analyzer>> getPresetAnalyzerTypes();

  /**
   * Returns available {@link CharFilterFactory}s.
   */
  Collection<Class<? extends CharFilterFactory>> getAvailableCharFilterFactories();

  /**
   * Returns available {@link TokenizerFactory}s.
   */
  Collection<Class<? extends TokenizerFactory>> getAvailableTokenizerFactories();

  /**
   * Returns available {@link TokenFilterFactory}s.
   */
  Collection<Class<? extends TokenFilterFactory>> getAvailableTokenFilterFactories();

  /**
   * Creates new Analyzer instance for the specified class name.
   *
   * @param analyzerType - instantiable class name of an Analyzer
   * @return new Analyzer instance
   * @throws LukeException - if failed to create new Analyzer instance
   */
  Analyzer createAnalyzerFromClassName(String analyzerType);

  /**
   * Creates new custom Analyzer instance with the given configurations.
   *
   * @param config - custom analyzer configurations
   * @return new Analyzer instance
   * @throws LukeException - if failed to create new Analyzer instance
   */
  Analyzer buildCustomAnalyzer(CustomAnalyzerConfig config);

  /**
   * Analyzes given text with the current Analyzer.
   *
   * @param text - text string to analyze
   * @return the list of token
   * @throws LukeException - if an internal error occurs when analyzing text
   */
  List<Token> analyze(String text);

  /**
   * Returns current analyzer.
   * @throws LukeException - if current analyzer not set
   */
  Analyzer currentAnalyzer();

  /**
   * Adds external jar files to classpath and loads custom {@link CharFilterFactory}s, {@link TokenizerFactory}s, or {@link TokenFilterFactory}s.
   *
   * @param jarFiles - list of paths to jar file
   * @throws LukeException - if an internal error occurs when loading jars
   */
  void addExternalJars(List<String> jarFiles);

}
