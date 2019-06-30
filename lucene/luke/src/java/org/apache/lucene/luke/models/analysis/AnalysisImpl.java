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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.util.reflection.ClassScanner;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.IOUtils;

/** Default implementation of {@link AnalysisImpl} */
public final class AnalysisImpl implements Analysis {

  private List<Class<? extends Analyzer>> presetAnalyzerTypes;

  private Analyzer analyzer;

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
        urls.toArray(new URL[0]), this.getClass().getClassLoader());
    CharFilterFactory.reloadCharFilters(classLoader);
    TokenizerFactory.reloadTokenizers(classLoader);
    TokenFilterFactory.reloadTokenFilters(classLoader);
  }

  @Override
  public Collection<Class<? extends Analyzer>> getPresetAnalyzerTypes() {
    if (Objects.isNull(presetAnalyzerTypes)) {
      List<Class<? extends Analyzer>> types = new ArrayList<>();
      for (Class<? extends Analyzer> clazz : getInstantiableSubTypesBuiltIn(Analyzer.class)) {
        try {
          // add to presets if no args constructor is available
          clazz.getConstructor();
          types.add(clazz);
        } catch (NoSuchMethodException e) {
        }
      }
      presetAnalyzerTypes = List.copyOf(types);
    }
    return presetAnalyzerTypes;
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
    ClassScanner scanner = new ClassScanner("org.apache.lucene.analysis", getClass().getClassLoader());
    Set<Class<? extends T>> types = scanner.scanSubTypes(superType);
    return types.stream()
        .filter(type -> !Modifier.isAbstract(type.getModifiers()))
        .filter(type -> !type.getSimpleName().startsWith("Mock"))
        .sorted(Comparator.comparing(Class::getName))
        .collect(Collectors.toList());
  }

  @Override
  public List<Token> analyze(String text) {
    Objects.requireNonNull(text);

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
        List<TokenAttribute> attributes = copyAttributes(stream, charAtt);
        result.add(new Token(charAtt.toString(), attributes));
      }
      stream.close();

      return result;
    } catch (IOException e) {
      throw new LukeException(e.getMessage(), e);
    }
  }

  private List<TokenAttribute> copyAttributes(TokenStream tokenStream, CharTermAttribute charAtt) {
    List<TokenAttribute> attributes = new ArrayList<>();
    Iterator<AttributeImpl> itr = tokenStream.getAttributeImplsIterator();
    while(itr.hasNext()) {
      AttributeImpl att = itr.next();
      Map<String, String> attValues = new LinkedHashMap<>();
      att.reflectWith((attClass, key, value) -> {
        if (value != null)
          attValues.put(key, value.toString());
      });
      attributes.add(new TokenAttribute(att.getClass().getSimpleName(), attValues));
    }
    return attributes;
  }

  @Override
  public Analyzer createAnalyzerFromClassName(String analyzerType) {
    Objects.requireNonNull(analyzerType);

    try {
      Class<? extends Analyzer> clazz = Class.forName(analyzerType).asSubclass(Analyzer.class);
      this.analyzer = clazz.getConstructor().newInstance();
      return analyzer;
    } catch (ReflectiveOperationException e) {
      throw new LukeException(String.format(Locale.ENGLISH, "Failed to instantiate class: %s", analyzerType), e);
    }
  }

  @Override
  public Analyzer buildCustomAnalyzer(CustomAnalyzerConfig config) {
    Objects.requireNonNull(config);
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

  @Override
  public StepByStepResult analyzeStepByStep(String text){
    Objects.requireNonNull(text);
    if (analyzer == null) {
      throw new LukeException("Analyzer is not set.");
    }

    if (!(analyzer instanceof CustomAnalyzer)) {
      throw new LukeException("Analyzer is not CustomAnalyzer.");
    }

    List<NamedTokens> namedTokens = new ArrayList<>();
    List<CharfilteredText> charfilteredTexts = new ArrayList<>();

    try {
      CustomAnalyzer customAnalyzer = (CustomAnalyzer)analyzer;
      final List<CharFilterFactory> charFilterFactories = customAnalyzer.getCharFilterFactories();
      Reader reader = new StringReader(text);
      String charFilteredSource = text;
      if (charFilterFactories.size() > 0) {
        Reader cs = reader;
        for (CharFilterFactory charFilterFactory : charFilterFactories) {
          cs = charFilterFactory.create(reader);
          Reader readerForWriteOut = new StringReader(charFilteredSource);
          readerForWriteOut = charFilterFactory.create(readerForWriteOut);
          charFilteredSource = writeCharStream(readerForWriteOut);
          charfilteredTexts.add(new CharfilteredText(CharFilterFactory.findSPIName(charFilterFactory.getClass()), charFilteredSource));
        }
        reader = cs;
      }

      final TokenizerFactory tokenizerFactory = customAnalyzer.getTokenizerFactory();
      final List<TokenFilterFactory> tokenFilterFactories = customAnalyzer.getTokenFilterFactories();

      TokenStream tokenStream = tokenizerFactory.create();
      ((Tokenizer)tokenStream).setReader(reader);
      List<Token> tokens = new ArrayList<>();
      List<AttributeSource> attributeSources = analyzeTokenStream(tokenStream, tokens);
      namedTokens.add(new NamedTokens(TokenizerFactory.findSPIName(tokenizerFactory.getClass()), tokens));

      ListBasedTokenStream listBasedTokenStream = new ListBasedTokenStream(tokenStream, attributeSources);
      for (TokenFilterFactory tokenFilterFactory : tokenFilterFactories) {
        tokenStream = tokenFilterFactory.create(listBasedTokenStream);
        tokens = new ArrayList<>();
        attributeSources = analyzeTokenStream(tokenStream, tokens);
        namedTokens.add(new NamedTokens(TokenFilterFactory.findSPIName(tokenFilterFactory.getClass()), tokens));
        try {
          listBasedTokenStream.close();
        } catch (IOException e) {
          // do nothing;
        }
        listBasedTokenStream = new ListBasedTokenStream(listBasedTokenStream, attributeSources);
      }
      try {
        listBasedTokenStream.close();
      } catch (IOException e) {
        // do nothing.
      } finally {
        reader.close();
      }
      return new StepByStepResult(charfilteredTexts, namedTokens);
    } catch (Exception e) {
      throw new LukeException(e.getMessage(), e);
    }
  }

  /**
   * Analyzes the given TokenStream, collecting the Tokens it produces.
   *
   * @param tokenStream TokenStream to analyze
   *
   * @return List of tokens produced from the TokenStream
   */
  private List<AttributeSource> analyzeTokenStream(TokenStream tokenStream, List<Token> result) {
    final List<AttributeSource> tokens = new ArrayList<>();
    try {
      tokenStream.reset();
      CharTermAttribute charAtt = tokenStream.getAttribute(CharTermAttribute.class);
      while (tokenStream.incrementToken()) {
        tokens.add(tokenStream.cloneAttributes());
        List<TokenAttribute> attributes = copyAttributes(tokenStream, charAtt);
        result.add(new Token(charAtt.toString(), attributes));
      }
      tokenStream.end();
    } catch (IOException ioe) {
      throw new RuntimeException("Error occurred while iterating over TokenStream", ioe);
    } finally {
      IOUtils.closeWhileHandlingException(tokenStream);
    }
    return tokens;
  }

  /**
   * TokenStream that iterates over a list of pre-existing Tokens
   * see org.apache.solr.handler.AnalysisRequestHandlerBase#ListBasedTokenStream
   */
  protected final static class ListBasedTokenStream extends TokenStream {
    private final List<AttributeSource> tokens;
    private Iterator<AttributeSource> tokenIterator;

    /**
     * Creates a new ListBasedTokenStream which uses the given tokens as its token source.
     *
     * @param attributeSource source of the attribute factory and attribute impls
     * @param tokens Source of tokens to be used
     */
    ListBasedTokenStream(AttributeSource attributeSource, List<AttributeSource> tokens) {
      super(attributeSource.getAttributeFactory());
      this.tokens = tokens;
      // Make sure all the attributes of the source are here too
      addAttributes(attributeSource);
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      tokenIterator = tokens.iterator();
    }

    @Override
    public boolean incrementToken() {
      if (tokenIterator.hasNext()) {
        clearAttributes();
        AttributeSource next = tokenIterator.next();
        addAttributes(next);
        next.copyTo(this);
        return true;
      } else {
        return false;
      }
    }

    void addAttributes(AttributeSource attributeSource) {
      Iterator<AttributeImpl> atts = attributeSource.getAttributeImplsIterator();
      while (atts.hasNext()) {
        addAttributeImpl(atts.next()); // adds both impl & interfaces
      }
    }
  }

  private static String writeCharStream(Reader input ){
    final int BUFFER_SIZE = 1024;
    char[] buf = new char[BUFFER_SIZE];
    int len = 0;
    StringBuilder sb = new StringBuilder();
    do {
      try {
        len = input.read( buf, 0, BUFFER_SIZE );
      } catch (IOException e) {
        throw new RuntimeException("Error occurred while iterating over charfiltering", e);
      }
      if( len > 0 )
        sb.append(buf, 0, len);
    } while( len == BUFFER_SIZE );
    return sb.toString();
  }

}
