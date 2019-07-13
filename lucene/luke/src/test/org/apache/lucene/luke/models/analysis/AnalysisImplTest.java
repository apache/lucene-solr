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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class AnalysisImplTest extends LuceneTestCase {

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
  public void testGetAvailableCharFilters() {
    AnalysisImpl analysis = new AnalysisImpl();
    Collection<String> charFilters = analysis.getAvailableCharFilters();
    assertNotNull(charFilters);
  }

  @Test
  public void testGetAvailableTokenizers() {
    AnalysisImpl analysis = new AnalysisImpl();
    Collection<String> tokenizers = analysis.getAvailableTokenizers();
    assertNotNull(tokenizers);
  }

  @Test
  public void testGetAvailableTokenFilters() {
    AnalysisImpl analysis = new AnalysisImpl();
    Collection<String> tokenFilters = analysis.getAvailableTokenFilters();
    assertNotNull(tokenFilters);
  }

  @Test
  public void testAnalyze_preset() {
    AnalysisImpl analysis = new AnalysisImpl();
    String analyzerType = "org.apache.lucene.analysis.standard.StandardAnalyzer";
    Analyzer analyzer = analysis.createAnalyzerFromClassName(analyzerType);
    assertEquals(analyzerType, analyzer.getClass().getName());

    String text = "It is a truth universally acknowledged, that a single man in possession of a good fortune, must be in want of a wife.";
    List<Analysis.Token> tokens = analysis.analyze(text);
    assertNotNull(tokens);
  }

  @Test
  public void testAnalyze_custom() {
    AnalysisImpl analysis = new AnalysisImpl();
    Map<String, String> tkParams = new HashMap<>();
    tkParams.put("maxTokenLen", "128");
    CustomAnalyzerConfig.Builder builder = new CustomAnalyzerConfig.Builder(
        "keyword", tkParams)
        .addTokenFilterConfig("lowercase", Collections.emptyMap());
    CustomAnalyzer analyzer = (CustomAnalyzer) analysis.buildCustomAnalyzer(builder.build());
    assertEquals("org.apache.lucene.analysis.custom.CustomAnalyzer", analyzer.getClass().getName());
    assertEquals("org.apache.lucene.analysis.core.KeywordTokenizerFactory", analyzer.getTokenizerFactory().getClass().getName());
    assertEquals("org.apache.lucene.analysis.core.LowerCaseFilterFactory", analyzer.getTokenFilterFactories().get(0).getClass().getName());

    String text = "Apache Lucene";
    List<Analysis.Token> tokens = analysis.analyze(text);
    assertNotNull(tokens);
  }

  @Test
  public void testAnalyzer_custom_with_confdir() throws Exception {
    Path confDir = createTempDir("conf");
    Path stopFile = Files.createFile(Paths.get(confDir.toString(), "stop.txt"));
    Files.write(stopFile, "of\nthe\nby\nfor\n".getBytes(StandardCharsets.UTF_8));

    AnalysisImpl analysis = new AnalysisImpl();
    Map<String, String> tkParams = new HashMap<>();
    tkParams.put("maxTokenLen", "128");
    Map<String, String> tfParams = new HashMap<>();
    tfParams.put("ignoreCase", "true");
    tfParams.put("words", "stop.txt");
    tfParams.put("format", "wordset");
    CustomAnalyzerConfig.Builder builder = new CustomAnalyzerConfig.Builder(
        "whitespace", tkParams)
        .configDir(confDir.toString())
        .addTokenFilterConfig("lowercase", Collections.emptyMap())
        .addTokenFilterConfig("stop", tfParams);
    CustomAnalyzer analyzer = (CustomAnalyzer) analysis.buildCustomAnalyzer(builder.build());
    assertEquals("org.apache.lucene.analysis.custom.CustomAnalyzer", analyzer.getClass().getName());
    assertEquals("org.apache.lucene.analysis.core.WhitespaceTokenizerFactory", analyzer.getTokenizerFactory().getClass().getName());
    assertEquals("org.apache.lucene.analysis.core.LowerCaseFilterFactory", analyzer.getTokenFilterFactories().get(0).getClass().getName());
    assertEquals("org.apache.lucene.analysis.core.StopFilterFactory", analyzer.getTokenFilterFactories().get(1).getClass().getName());

    String text = "Government of the People, by the People, for the People";
    List<Analysis.Token> tokens = analysis.analyze(text);
    assertNotNull(tokens);
  }

  @Test(expected = LukeException.class)
  public void testAnalyze_not_set() {
    AnalysisImpl analysis = new AnalysisImpl();
    String text = "This test must fail.";
    analysis.analyze(text);
  }

  @Test(expected = LukeException.class)
  public void testAnalyzeStepByStep_preset() {
    AnalysisImpl analysis = new AnalysisImpl();
    String analyzerType = "org.apache.lucene.analysis.standard.StandardAnalyzer";
    Analyzer analyzer = analysis.createAnalyzerFromClassName(analyzerType);
    assertEquals(analyzerType, analyzer.getClass().getName());

    String text = "This test must fail.";
    analysis.analyzeStepByStep(text);
  }

  @Test
  public void testAnalyzeStepByStep_custom() {
    AnalysisImpl analysis = new AnalysisImpl();
    Map<String, String> tkParams = new HashMap<>();
    tkParams.put("maxTokenLen", "128");
    CustomAnalyzerConfig.Builder builder = new CustomAnalyzerConfig.Builder("keyword", tkParams)
        .addTokenFilterConfig("lowercase", Collections.emptyMap())
        .addCharFilterConfig("htmlstrip", Collections.emptyMap());
    CustomAnalyzer analyzer = (CustomAnalyzer) analysis.buildCustomAnalyzer(builder.build());
    assertEquals("org.apache.lucene.analysis.custom.CustomAnalyzer", analyzer.getClass().getName());
    assertEquals("org.apache.lucene.analysis.charfilter.HTMLStripCharFilterFactory",
        analyzer.getCharFilterFactories().get(0).getClass().getName());
    assertEquals("org.apache.lucene.analysis.core.KeywordTokenizerFactory",
        analyzer.getTokenizerFactory().getClass().getName());
    assertEquals("org.apache.lucene.analysis.core.LowerCaseFilterFactory",
        analyzer.getTokenFilterFactories().get(0).getClass().getName());

    String text = "Apache Lucene";
    Analysis.StepByStepResult result = analysis.analyzeStepByStep(text);
    assertNotNull(result);
    assertNotNull(result.getCharfilteredTexts());
    assertEquals(1,result.getCharfilteredTexts().size());
    assertEquals("htmlStrip", result.getCharfilteredTexts().get(0).getName());

    assertNotNull(result.getNamedTokens());
    assertEquals(2, result.getNamedTokens().size());
    //FIXME check each namedTokensList
    assertEquals("keyword", result.getNamedTokens().get(0).getName());
    assertEquals("lowercase", result.getNamedTokens().get(1).getName());
  }
}
