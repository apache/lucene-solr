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

import javax.annotation.Nonnull;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.util.AttributeImpl;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

public final class AnalysisImpl implements Analysis {

  private final List<Class<? extends Analyzer>> presetAnalyzerTypes;

  private Analyzer analyzer;

  public AnalysisImpl() {
    presetAnalyzerTypes = new ArrayList<>();
    for (Class<? extends Analyzer> clazz : getInstantiableSubTypesBuiltIn(Analyzer.class)) {
      try {
        // add to presets if no args constructor is available
        clazz.getConstructor();
        presetAnalyzerTypes.add(clazz);
      } catch (NoSuchMethodException e) {
      }
    }
    // standard analyzer cannot be found, so manually add it...
    presetAnalyzerTypes.add(StandardAnalyzer.class);
    presetAnalyzerTypes.sort(Comparator.comparing(Class::getName));
  }

  @Override
  public void addExternalJars(List<String> jarFiles) {
    List<URL> urls = new ArrayList<>();

    for (String jarFile : jarFiles) {
      Path path = FileSystems.getDefault().getPath(jarFile);
      if (!Files.exists(path) || !jarFile.endsWith(".jar")) {
        throw new LukeException(String.format(Locale.ENGLISH, "Invalid jar file path: %s", jarFile));
      }
      try {
        URL url = path.toUri().toURL();
        urls.add(url);
      } catch (IOException e) {
        throw new LukeException(e.getMessage(), e);
      }
    }

    // reload available tokenizers, charfilters, and tokenfilters
    URLClassLoader classLoader = new URLClassLoader(
        urls.toArray(new URL[0]), ClassLoader.getSystemClassLoader());
    CharFilterFactory.reloadCharFilters(classLoader);
    TokenizerFactory.reloadTokenizers(classLoader);
    TokenFilterFactory.reloadTokenFilters(classLoader);
  }

  @Override
  public Collection<Class<? extends Analyzer>> getPresetAnalyzerTypes() {
    return ImmutableList.copyOf(presetAnalyzerTypes);
  }

  @Override
  public Collection<String> getAvailableCharFilters() {
    return CharFilterFactory.availableCharFilters().stream().sorted().collect(Collectors.toList());
  }

  @Override
  public Collection<String> getAvailableTokenizers() {
    return TokenizerFactory.availableTokenizers().stream().sorted().collect(Collectors.toList());
  }

  @Override
  public Collection<String> getAvailableTokenFilters() {
    return TokenFilterFactory.availableTokenFilters().stream().sorted().collect(Collectors.toList());
  }

  private <T> List<Class<? extends T>> getInstantiableSubTypesBuiltIn(Class<T> superType) {
    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .setUrls(ClasspathHelper.forPackage("org.apache.lucene"))
        .setScanners(new SubTypesScanner())
        .filterInputsBy(new FilterBuilder().include("org\\.apache\\.lucene\\.analysis.*")));
    return reflections.getSubTypesOf(superType).stream()
        .filter(type -> !Modifier.isAbstract(type.getModifiers()))
        .filter(type -> !type.getSimpleName().startsWith("Mock"))
        .collect(Collectors.toList());
  }

  @Override
  public List<Token> analyze(@Nonnull String text) {
    if (analyzer == null) {
      throw new LukeException("Analyzer is not set.");
    }

    try {
      List<Token> result = new ArrayList<>();

      TokenStream stream = analyzer.tokenStream("", text);
      stream.reset();

      CharTermAttribute charAtt = stream.getAttribute(CharTermAttribute.class);

      // iterate tokens
      while (stream.incrementToken()) {
        List<TokenAttribute> attributes = new ArrayList<>();
        Iterator<AttributeImpl> itr = stream.getAttributeImplsIterator();

        while (itr.hasNext()) {
          AttributeImpl att = itr.next();
          Map<String, String> attValues = new LinkedHashMap<>();
          att.reflectWith((attClass, key, value) -> {
            if (value != null)
              attValues.put(key, value.toString());
          });
          attributes.add(new TokenAttribute(att.getClass().getSimpleName(), attValues));
        }

        result.add(new Token(charAtt.toString(), attributes));
      }
      stream.close();

      return result;
    } catch (IOException e) {
      throw new LukeException(e.getMessage(), e);
    }
  }

  @Override
  public Analyzer createAnalyzerFromClassName(@Nonnull String analyzerType) {
    try {
      Class<? extends Analyzer> clazz = Class.forName(analyzerType).asSubclass(Analyzer.class);
      this.analyzer = clazz.newInstance();
      return analyzer;
    } catch (ReflectiveOperationException e) {
      throw new LukeException(String.format(Locale.ENGLISH, "Failed to instantiate class: %s", analyzerType), e);
    }
  }

  @Override
  public Analyzer buildCustomAnalyzer(@Nonnull CustomAnalyzerConfig config) {
    try {
      // create builder
      CustomAnalyzer.Builder builder = config.getConfigDir()
          .map(path -> CustomAnalyzer.builder(FileSystems.getDefault().getPath(path)))
          .orElse(CustomAnalyzer.builder());

      // set tokenizer
      builder.withTokenizer(config.getTokenizerConfig().getName(), config.getTokenizerConfig().getParams());

      // add char filters
      for (CustomAnalyzerConfig.ComponentConfig cfConf : config.getCharFilterConfigs()) {
        builder.addCharFilter(cfConf.getName(), cfConf.getParams());
      }

      // add token filters
      for (CustomAnalyzerConfig.ComponentConfig tfConf : config.getTokenFilterConfigs()) {
        builder.addTokenFilter(tfConf.getName(), tfConf.getParams());
      }

      // build analyzer
      this.analyzer = builder.build();
      return analyzer;
    } catch (Exception e) {
      throw new LukeException("Failed to build custom analyzer.", e);
    }
  }

  @Override
  public Analyzer currentAnalyzer() {
    if (analyzer == null) {
      throw new LukeException("Analyzer is not set.");
    }
    return analyzer;
  }

}
