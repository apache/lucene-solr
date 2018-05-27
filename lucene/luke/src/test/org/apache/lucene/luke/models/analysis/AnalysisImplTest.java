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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.luke.models.LukeException;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AnalysisImplTest extends AnalysisTestBase {

  @Test
  public void testGetPresetAnalyzerTypes() throws Exception {
    AnalysisImpl analysis = new AnalysisImpl();
    Collection<Class<? extends Analyzer>> analyerTypes = analysis.getPresetAnalyzerTypes();
    assertNotNull(analyerTypes);
    for (Class<? extends Analyzer> clazz : analyerTypes) {
      clazz.newInstance();
    }
  }

  @Test
  public void testGetAvailableCharFilterFactories() {
    AnalysisImpl analysis = new AnalysisImpl();
    Collection<Class<? extends CharFilterFactory>> charFilterFactories = analysis.getAvailableCharFilterFactories();
    assertNotNull(charFilterFactories);
  }

  @Test
  public void testGetAvailableTokenizerFactories() {
    AnalysisImpl analysis = new AnalysisImpl();
    Collection<Class<? extends TokenizerFactory>> tokenizerFactories = analysis.getAvailableTokenizerFactories();
    assertNotNull(tokenizerFactories);
  }

  @Test
  public void testGetAvailableTokenFilterFactories() {
    AnalysisImpl analysis = new AnalysisImpl();
    Collection<Class<? extends TokenFilterFactory>> tokenFilterFactories = analysis.getAvailableTokenFilterFactories();
    assertNotNull(tokenFilterFactories);
  }

  @Test
  public void testAnalyze_preset() throws Exception {
    AnalysisImpl analysis = new AnalysisImpl();
    String analyzerType = "org.apache.lucene.analysis.standard.StandardAnalyzer";
    Analyzer analyzer = analysis.createAnalyzerFromClassName(analyzerType);
    assertEquals(analyzerType, analyzer.getClass().getName());

    String text = "It is a truth universally acknowledged, that a single man in possession of a good fortune, must be in want of a wife.";
    List<Analysis.Token> tokens = analysis.analyze(text);
    assertNotNull(tokens);
    printTokens(tokens);
  }

  @Test
  public void testAnalyze_custom() throws Exception {
    AnalysisImpl analysis = new AnalysisImpl();
    CustomAnalyzerConfig.Builder builder = new CustomAnalyzerConfig.Builder(
        "org.apache.lucene.analysis.core.KeywordTokenizerFactory",
        ImmutableMap.of("maxTokenLen", "128"))
        .addTokenFilterConfig("org.apache.lucene.analysis.core.LowerCaseFilterFactory", Collections.emptyMap());
    CustomAnalyzer analyzer = (CustomAnalyzer) analysis.buildCustomAnalyzer(builder.build());
    assertEquals("org.apache.lucene.analysis.custom.CustomAnalyzer", analyzer.getClass().getName());
    assertEquals("org.apache.lucene.analysis.core.KeywordTokenizerFactory", analyzer.getTokenizerFactory().getClass().getName());
    assertEquals("org.apache.lucene.analysis.core.LowerCaseFilterFactory", analyzer.getTokenFilterFactories().get(0).getClass().getName());

    String text = "Apache Lucene";
    List<Analysis.Token> tokens = analysis.analyze(text);
    assertNotNull(tokens);
    printTokens(tokens);
  }

  @Test
  public void testAnalyzer_custom_with_confdir() throws Exception {
    Path confDir = createTempDir("conf");
    Path stopFile = Files.createFile(Paths.get(confDir.toString(), "stop.txt"));
    Files.write(stopFile, "of\nthe\nby\nfor\n".getBytes());

    AnalysisImpl analysis = new AnalysisImpl();
    CustomAnalyzerConfig.Builder builder = new CustomAnalyzerConfig.Builder(
        "org.apache.lucene.analysis.core.WhitespaceTokenizerFactory",
        ImmutableMap.of("maxTokenLen", "128"))
        .configDir(confDir.toString())
        .addTokenFilterConfig("org.apache.lucene.analysis.core.LowerCaseFilterFactory", Collections.emptyMap())
        .addTokenFilterConfig("org.apache.lucene.analysis.core.StopFilterFactory",
            ImmutableMap.of("ignoreCase", "true", "words", "stop.txt", "format", "wordset"));
    CustomAnalyzer analyzer = (CustomAnalyzer) analysis.buildCustomAnalyzer(builder.build());
    assertEquals("org.apache.lucene.analysis.custom.CustomAnalyzer", analyzer.getClass().getName());
    assertEquals("org.apache.lucene.analysis.core.WhitespaceTokenizerFactory", analyzer.getTokenizerFactory().getClass().getName());
    assertEquals("org.apache.lucene.analysis.core.LowerCaseFilterFactory", analyzer.getTokenFilterFactories().get(0).getClass().getName());
    assertEquals("org.apache.lucene.analysis.core.StopFilterFactory", analyzer.getTokenFilterFactories().get(1).getClass().getName());

    String text = "Government of the People, by the People, for the People";
    List<Analysis.Token> tokens = analysis.analyze(text);
    assertNotNull(tokens);
    printTokens(tokens);
  }

  @Test(expected = LukeException.class)
  public void testAnalyze_not_set() throws Exception {
    AnalysisImpl analysis = new AnalysisImpl();
    String text = "This test must fail.";
    analysis.analyze(text);
  }

  private void printTokens(List<Analysis.Token> tokens) {
    for (Analysis.Token token : tokens) {
      System.out.println("---- Token(term=" + token.getTerm() + ") ----");
      for (Analysis.TokenAttribute att : token.getAttributes()) {
        System.out.println(att.getAttClass());
        for (Map.Entry<String, String> entry : att.getAttValues().entrySet()) {
          System.out.println(String.format("  %s=%s", entry.getKey(), entry.getValue()));
        }
      }
    }
  }

}
