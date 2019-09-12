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
package org.apache.lucene.analysis.custom;


import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.miscellaneous.ConditionalTokenFilter;
import org.apache.lucene.analysis.miscellaneous.ConditionalTokenFilterFactory;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.AbstractAnalysisFactory;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;
import org.apache.lucene.analysis.util.FilesystemResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.Version;

import static org.apache.lucene.analysis.util.AnalysisSPILoader.newFactoryClassInstance;

/**
 * A general-purpose Analyzer that can be created with a builder-style API.
 * Under the hood it uses the factory classes {@link TokenizerFactory},
 * {@link TokenFilterFactory}, and {@link CharFilterFactory}.
 * <p>You can create an instance of this Analyzer using the builder by passing the SPI names (as defined by {@link java.util.ServiceLoader} interface) to it:
 * <pre class="prettyprint">
 * Analyzer ana = CustomAnalyzer.builder(Paths.get(&quot;/path/to/config/dir&quot;))
 *   .withTokenizer(StandardTokenizerFactory.NAME)
 *   .addTokenFilter(LowerCaseFilterFactory.NAME)
 *   .addTokenFilter(StopFilterFactory.NAME, &quot;ignoreCase&quot;, &quot;false&quot;, &quot;words&quot;, &quot;stopwords.txt&quot;, &quot;format&quot;, &quot;wordset&quot;)
 *   .build();
 * </pre>
 * The parameters passed to components are also used by Apache Solr and are documented
 * on their corresponding factory classes. Refer to documentation of subclasses
 * of {@link TokenizerFactory}, {@link TokenFilterFactory}, and {@link CharFilterFactory}.
 * <p>This is the same as the above:
 * <pre class="prettyprint">
 * Analyzer ana = CustomAnalyzer.builder(Paths.get(&quot;/path/to/config/dir&quot;))
 *   .withTokenizer(&quot;standard&quot;)
 *   .addTokenFilter(&quot;lowercase&quot;)
 *   .addTokenFilter(&quot;stop&quot;, &quot;ignoreCase&quot;, &quot;false&quot;, &quot;words&quot;, &quot;stopwords.txt&quot;, &quot;format&quot;, &quot;wordset&quot;)
 *   .build();
 * </pre>
 * <p>The list of names to be used for components can be looked up through:
 * {@link TokenizerFactory#availableTokenizers()}, {@link TokenFilterFactory#availableTokenFilters()},
 * and {@link CharFilterFactory#availableCharFilters()}.
 * <p>You can create conditional branches in the analyzer by using {@link Builder#when(String, String...)} and
 * {@link Builder#whenTerm(Predicate)}:
 * <pre class="prettyprint">
 * Analyzer ana = CustomAnalyzer.builder()
 *    .withTokenizer(&quot;standard&quot;)
 *    .addTokenFilter(&quot;lowercase&quot;)
 *    .whenTerm(t -&gt; t.length() &gt; 10)
 *      .addTokenFilter(&quot;reversestring&quot;)
 *    .endwhen()
 *    .build();
 * </pre>
 *
 * @since 5.0.0
 */
public final class CustomAnalyzer extends Analyzer {
  
  /**
   * Returns a builder for custom analyzers that loads all resources from
   * Lucene's classloader. All path names given must be absolute with package prefixes. 
   */
  public static Builder builder() {
    return builder(new ClasspathResourceLoader(CustomAnalyzer.class.getClassLoader()));
  }
  
  /** 
   * Returns a builder for custom analyzers that loads all resources from the given
   * file system base directory. Place, e.g., stop word files there.
   * Files that are not in the given directory are loaded from Lucene's classloader.
   */
  public static Builder builder(Path configDir) {
    return builder(new FilesystemResourceLoader(configDir, CustomAnalyzer.class.getClassLoader()));
  }
  
  /** Returns a builder for custom analyzers that loads all resources using the given {@link ResourceLoader}. */
  public static Builder builder(ResourceLoader loader) {
    return new Builder(loader);
  }
  
  private final CharFilterFactory[] charFilters;
  private final TokenizerFactory tokenizer;
  private final TokenFilterFactory[] tokenFilters;
  private final Integer posIncGap, offsetGap;

  CustomAnalyzer(Version defaultMatchVersion, CharFilterFactory[] charFilters, TokenizerFactory tokenizer, TokenFilterFactory[] tokenFilters, Integer posIncGap, Integer offsetGap) {
    this.charFilters = charFilters;
    this.tokenizer = tokenizer;
    this.tokenFilters = tokenFilters;
    this.posIncGap = posIncGap;
    this.offsetGap = offsetGap;
    if (defaultMatchVersion != null) {
      setVersion(defaultMatchVersion);
    }
  }
  
  @Override
  protected Reader initReader(String fieldName, Reader reader) {
    for (final CharFilterFactory charFilter : charFilters) {
      reader = charFilter.create(reader);
    }
    return reader;
  }

  @Override
  protected Reader initReaderForNormalization(String fieldName, Reader reader) {
    for (CharFilterFactory charFilter : charFilters) {
      reader = charFilter.normalize(reader);
    }
    return reader;
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    final Tokenizer tk = tokenizer.create(attributeFactory(fieldName));
    TokenStream ts = tk;
    for (final TokenFilterFactory filter : tokenFilters) {
      ts = filter.create(ts);
    }
    return new TokenStreamComponents(tk, ts);
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    TokenStream result = in;
    for (TokenFilterFactory filter : tokenFilters) {
      result = filter.normalize(result);
    }
    return result;
  }

  @Override
  public int getPositionIncrementGap(String fieldName) {
    // use default from Analyzer base class if null
    return (posIncGap == null) ? super.getPositionIncrementGap(fieldName) : posIncGap.intValue();
  }
  
  @Override
  public int getOffsetGap(String fieldName) {
    // use default from Analyzer base class if null
    return (offsetGap == null) ? super.getOffsetGap(fieldName) : offsetGap.intValue();
  }
  
  /** Returns the list of char filters that are used in this analyzer. */
  public List<CharFilterFactory> getCharFilterFactories() {
    return Collections.unmodifiableList(Arrays.asList(charFilters));
  }
  
  /** Returns the tokenizer that is used in this analyzer. */
  public TokenizerFactory getTokenizerFactory() {
    return tokenizer;
  }
  
  /** Returns the list of token filters that are used in this analyzer. */
  public List<TokenFilterFactory> getTokenFilterFactories() {
    return Collections.unmodifiableList(Arrays.asList(tokenFilters));
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName()).append('(');
    for (final CharFilterFactory filter : charFilters) {
      sb.append(filter).append(',');
    }
    sb.append(tokenizer);
    for (final TokenFilterFactory filter : tokenFilters) {
      sb.append(',').append(filter);
    }
    return sb.append(')').toString();
  }

  /** Builder for {@link CustomAnalyzer}.
   * @see CustomAnalyzer#builder()
   * @see CustomAnalyzer#builder(Path)
   * @see CustomAnalyzer#builder(ResourceLoader)
   */
  public static final class Builder {
    private final ResourceLoader loader;
    private final SetOnce<Version> defaultMatchVersion = new SetOnce<>();
    private final List<CharFilterFactory> charFilters = new ArrayList<>();
    private final SetOnce<TokenizerFactory> tokenizer = new SetOnce<>();
    private final List<TokenFilterFactory> tokenFilters = new ArrayList<>();
    private final SetOnce<Integer> posIncGap = new SetOnce<>();
    private final SetOnce<Integer> offsetGap = new SetOnce<>();
    
    private boolean componentsAdded = false;
    
    Builder(ResourceLoader loader) {
      this.loader = loader;
    }
    
    /** This match version is passed as default to all tokenizers or filters. It is used unless you
     * pass the parameter {code luceneMatchVersion} explicitly. It defaults to undefined, so the
     * underlying factory will (in most cases) use {@link Version#LATEST}. */
    public Builder withDefaultMatchVersion(Version version) {
      Objects.requireNonNull(version, "version may not be null");
      if (componentsAdded) {
        throw new IllegalStateException("You may only set the default match version before adding tokenizers, "+
            "token filters, or char filters.");
      }
      this.defaultMatchVersion.set(version);
      return this;
    }
    
    /** Sets the position increment gap of the analyzer.
     * The default is defined in the analyzer base class.
     * @see Analyzer#getPositionIncrementGap(String)
     */
    public Builder withPositionIncrementGap(int posIncGap) {
      if (posIncGap < 0) {
        throw new IllegalArgumentException("posIncGap must be >= 0");
      }
      this.posIncGap.set(posIncGap);
      return this;
    }
    
    /** Sets the offset gap of the analyzer. The default is defined
     * in the analyzer base class.
     * @see Analyzer#getOffsetGap(String)
     */
    public Builder withOffsetGap(int offsetGap) {
      if (offsetGap < 0) {
        throw new IllegalArgumentException("offsetGap must be >= 0");
      }
      this.offsetGap.set(offsetGap);
      return this;
    }
    
    /** Uses the given tokenizer.
     * @param factory class that is used to create the tokenizer.
     * @param params a list of factory string params as key/value pairs.
     *  The number of parameters must be an even number, as they are pairs.
     */
    public Builder withTokenizer(Class<? extends TokenizerFactory> factory, String... params) throws IOException {
      return withTokenizer(factory, paramsToMap(params));
    }
    
    /** Uses the given tokenizer.
     * @param factory class that is used to create the tokenizer.
     * @param params the map of parameters to be passed to factory. The map must be modifiable.
     */
    public Builder withTokenizer(Class<? extends TokenizerFactory> factory, Map<String,String> params) throws IOException {
      Objects.requireNonNull(factory, "Tokenizer factory may not be null");
      tokenizer.set(applyResourceLoader(newFactoryClassInstance(factory, applyDefaultParams(params))));
      componentsAdded = true;
      return this;
    }
    
    /** Uses the given tokenizer.
     * @param name is used to look up the factory with {@link TokenizerFactory#forName(String, Map)}.
     *  The list of possible names can be looked up with {@link TokenizerFactory#availableTokenizers()}.
     * @param params a list of factory string params as key/value pairs.
     *  The number of parameters must be an even number, as they are pairs.
     */
    public Builder withTokenizer(String name, String... params) throws IOException {
      return withTokenizer(name, paramsToMap(params));
    }
    
    /** Uses the given tokenizer.
     * @param name is used to look up the factory with {@link TokenizerFactory#forName(String, Map)}.
     *  The list of possible names can be looked up with {@link TokenizerFactory#availableTokenizers()}.
     * @param params the map of parameters to be passed to factory. The map must be modifiable.
     */
    public Builder withTokenizer(String name, Map<String,String> params) throws IOException {
      Objects.requireNonNull(name, "Tokenizer name may not be null");
      tokenizer.set(applyResourceLoader(TokenizerFactory.forName(name, applyDefaultParams(params))));
      componentsAdded = true;
      return this;
    }
    
    /** Adds the given token filter.
     * @param factory class that is used to create the token filter.
     * @param params a list of factory string params as key/value pairs.
     *  The number of parameters must be an even number, as they are pairs.
     */
    public Builder addTokenFilter(Class<? extends TokenFilterFactory> factory, String... params) throws IOException {
      return addTokenFilter(factory, paramsToMap(params));
    }
    
    /** Adds the given token filter.
     * @param factory class that is used to create the token filter.
     * @param params the map of parameters to be passed to factory. The map must be modifiable.
     */
    public Builder addTokenFilter(Class<? extends TokenFilterFactory> factory, Map<String,String> params) throws IOException {
      Objects.requireNonNull(factory, "TokenFilter name may not be null");
      tokenFilters.add(applyResourceLoader(newFactoryClassInstance(factory, applyDefaultParams(params))));
      componentsAdded = true;
      return this;
    }
    
    /** Adds the given token filter.
     * @param name is used to look up the factory with {@link TokenFilterFactory#forName(String, Map)}.
     *  The list of possible names can be looked up with {@link TokenFilterFactory#availableTokenFilters()}.
     * @param params a list of factory string params as key/value pairs.
     *  The number of parameters must be an even number, as they are pairs.
     */
    public Builder addTokenFilter(String name, String... params) throws IOException {
      return addTokenFilter(name, paramsToMap(params));
    }
    
    /** Adds the given token filter.
     * @param name is used to look up the factory with {@link TokenFilterFactory#forName(String, Map)}.
     *  The list of possible names can be looked up with {@link TokenFilterFactory#availableTokenFilters()}.
     * @param params the map of parameters to be passed to factory. The map must be modifiable.
     */
    public Builder addTokenFilter(String name, Map<String,String> params) throws IOException {
      Objects.requireNonNull(name, "TokenFilter name may not be null");
      tokenFilters.add(applyResourceLoader(TokenFilterFactory.forName(name, applyDefaultParams(params))));
      componentsAdded = true;
      return this;
    }

    private Builder addTokenFilter(TokenFilterFactory factory) {
      Objects.requireNonNull(factory, "TokenFilterFactory may not be null");
      tokenFilters.add(factory);
      componentsAdded = true;
      return this;
    }
    
    /** Adds the given char filter.
     * @param factory class that is used to create the char filter.
     * @param params a list of factory string params as key/value pairs.
     *  The number of parameters must be an even number, as they are pairs.
     */
    public Builder addCharFilter(Class<? extends CharFilterFactory> factory, String... params) throws IOException {
      return addCharFilter(factory, paramsToMap(params));
    }
    
    /** Adds the given char filter.
     * @param factory class that is used to create the char filter.
     * @param params the map of parameters to be passed to factory. The map must be modifiable.
     */
    public Builder addCharFilter(Class<? extends CharFilterFactory> factory, Map<String,String> params) throws IOException {
      Objects.requireNonNull(factory, "CharFilter name may not be null");
      charFilters.add(applyResourceLoader(newFactoryClassInstance(factory, applyDefaultParams(params))));
      componentsAdded = true;
      return this;
    }
    
    /** Adds the given char filter.
     * @param name is used to look up the factory with {@link CharFilterFactory#forName(String, Map)}.
     *  The list of possible names can be looked up with {@link CharFilterFactory#availableCharFilters()}.
     * @param params a list of factory string params as key/value pairs.
     *  The number of parameters must be an even number, as they are pairs.
     */
    public Builder addCharFilter(String name, String... params) throws IOException {
      return addCharFilter(name, paramsToMap(params));
    }
    
    /** Adds the given char filter.
     * @param name is used to look up the factory with {@link CharFilterFactory#forName(String, Map)}.
     *  The list of possible names can be looked up with {@link CharFilterFactory#availableCharFilters()}.
     * @param params the map of parameters to be passed to factory. The map must be modifiable.
     */
    public Builder addCharFilter(String name, Map<String,String> params) throws IOException {
      Objects.requireNonNull(name, "CharFilter name may not be null");
      charFilters.add(applyResourceLoader(CharFilterFactory.forName(name, applyDefaultParams(params))));
      componentsAdded = true;
      return this;
    }

    /**
     * Add a {@link ConditionalTokenFilterFactory} to the analysis chain
     *
     * TokenFilters added by subsequent calls to {@link ConditionBuilder#addTokenFilter(String, String...)}
     * and related functions will only be used if the current token matches the condition.  Consumers
     * must call {@link ConditionBuilder#endwhen()} to return to the normal tokenfilter
     * chain once conditional filters have been added
     *
     * @param name    is used to look up the factory with {@link TokenFilterFactory#forName(String, Map)}
     * @param params  the parameters to be passed to the factory
     */
    public ConditionBuilder when(String name, String... params) throws IOException {
      return when(name, paramsToMap(params));
    }

    /**
     * Add a {@link ConditionalTokenFilterFactory} to the analysis chain
     *
     * TokenFilters added by subsequent calls to {@link ConditionBuilder#addTokenFilter(String, String...)}
     * and related functions will only be used if the current token matches the condition.  Consumers
     * must call {@link ConditionBuilder#endwhen()} to return to the normal tokenfilter
     * chain once conditional filters have been added
     *
     * @param name    is used to look up the factory with {@link TokenFilterFactory#forName(String, Map)}
     * @param params  the parameters to be passed to the factory.  The map must be modifiable
     */
    @SuppressWarnings("unchecked")
    public ConditionBuilder when(String name, Map<String, String> params) throws IOException {
      Class<? extends TokenFilterFactory> clazz = TokenFilterFactory.lookupClass(name);
      if (ConditionalTokenFilterFactory.class.isAssignableFrom(clazz) == false) {
        throw new IllegalArgumentException("TokenFilterFactory " + name + " is not a ConditionalTokenFilterFactory");
      }
      return when((Class<? extends ConditionalTokenFilterFactory>) clazz, params);
    }

    /**
     * Add a {@link ConditionalTokenFilterFactory} to the analysis chain
     *
     * TokenFilters added by subsequent calls to {@link ConditionBuilder#addTokenFilter(String, String...)}
     * and related functions will only be used if the current token matches the condition.  Consumers
     * must call {@link ConditionBuilder#endwhen()} to return to the normal tokenfilter
     * chain once conditional filters have been added
     *
     * @param factory class that is used to create the ConditionalTokenFilter
     * @param params  the parameters to be passed to the factory
     */
    public ConditionBuilder when(Class<? extends ConditionalTokenFilterFactory> factory, String... params) throws IOException {
      return when(factory, paramsToMap(params));
    }

    /**
     * Add a {@link ConditionalTokenFilterFactory} to the analysis chain
     *
     * TokenFilters added by subsequent calls to {@link ConditionBuilder#addTokenFilter(String, String...)}
     * and related functions will only be used if the current token matches the condition.  Consumers
     * must call {@link ConditionBuilder#endwhen()} to return to the normal tokenfilter
     * chain once conditional filters have been added
     *
     * @param factory class that is used to create the ConditionalTokenFilter
     * @param params  the parameters to be passed to the factory.  The map must be modifiable
     */
    public ConditionBuilder when(Class<? extends ConditionalTokenFilterFactory> factory, Map<String, String> params) throws IOException {
      return when(newFactoryClassInstance(factory, applyDefaultParams(params)));
    }

    /**
     * Add a {@link ConditionalTokenFilterFactory} to the analysis chain
     *
     * TokenFilters added by subsequent calls to {@link ConditionBuilder#addTokenFilter(String, String...)}
     * and related functions will only be used if the current token matches the condition.  Consumers
     * must call {@link ConditionBuilder#endwhen()} to return to the normal tokenfilter
     * chain once conditional filters have been added
     */
    public ConditionBuilder when(ConditionalTokenFilterFactory factory) {
      return new ConditionBuilder(factory, this);
    }

    /**
     * Apply subsequent token filters if the current token's term matches a predicate
     *
     * This is the equivalent of:
     * <pre>
     *   when(new ConditionalTokenFilterFactory(Collections.emptyMap()) {
     *      {@code @}Override
     *      protected ConditionalTokenFilter create(TokenStream input, Function&lt;TokenStream, TokenStream&gt; inner) {
     *        return new ConditionalTokenFilter(input, inner) {
     *          CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
     *          {@code @}Override
     *          protected boolean shouldFilter() {
     *            return predicate.test(termAtt);
     *          }
     *        };
     *      }
     *   });
     * </pre>
     */
    public ConditionBuilder whenTerm(Predicate<CharSequence> predicate) {
      return new ConditionBuilder(new ConditionalTokenFilterFactory(Collections.emptyMap()) {
        @Override
        protected ConditionalTokenFilter create(TokenStream input, Function<TokenStream, TokenStream> inner) {
          return new ConditionalTokenFilter(input, inner) {
            CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
            @Override
            protected boolean shouldFilter() {
              return predicate.test(termAtt);
            }
          };
        }
      }, this);
    }
    
    /** Builds the analyzer. */
    public CustomAnalyzer build() {
      if (tokenizer.get() == null) {
        throw new IllegalStateException("You have to set at least a tokenizer.");
      }
      return new CustomAnalyzer(
        defaultMatchVersion.get(),
        charFilters.toArray(new CharFilterFactory[charFilters.size()]),
        tokenizer.get(), 
        tokenFilters.toArray(new TokenFilterFactory[tokenFilters.size()]),
        posIncGap.get(),
        offsetGap.get()
      );
    }
    
    private Map<String,String> applyDefaultParams(Map<String,String> map) {
      if (defaultMatchVersion.get() != null && !map.containsKey(AbstractAnalysisFactory.LUCENE_MATCH_VERSION_PARAM)) {
        map.put(AbstractAnalysisFactory.LUCENE_MATCH_VERSION_PARAM, defaultMatchVersion.get().toString());
      }
      return map;
    }
    
    private Map<String, String> paramsToMap(String... params) {
      if (params.length % 2 != 0) {
        throw new IllegalArgumentException("Key-value pairs expected, so the number of params must be even.");
      }
      final Map<String, String> map = new HashMap<>();
      for (int i = 0; i < params.length; i += 2) {
        Objects.requireNonNull(params[i], "Key of param may not be null.");
        map.put(params[i], params[i + 1]);
      }
      return map;
    }
    
    <T> T applyResourceLoader(T factory) throws IOException {
      if (factory instanceof ResourceLoaderAware) {
        ((ResourceLoaderAware) factory).inform(loader);
      }
      return factory;
    }
  }

  /**
   * Factory class for a {@link ConditionalTokenFilter}
   */
  public static class ConditionBuilder {

    private final List<TokenFilterFactory> innerFilters = new ArrayList<>();
    private final ConditionalTokenFilterFactory factory;
    private final Builder parent;

    private ConditionBuilder(ConditionalTokenFilterFactory factory, Builder parent) {
      this.factory = factory;
      this.parent = parent;
    }

    /** Adds the given token filter.
     * @param name is used to look up the factory with {@link TokenFilterFactory#forName(String, Map)}.
     *  The list of possible names can be looked up with {@link TokenFilterFactory#availableTokenFilters()}.
     * @param params the map of parameters to be passed to factory. The map must be modifiable.
     */
    public ConditionBuilder addTokenFilter(String name, Map<String, String> params) throws IOException {
      innerFilters.add(TokenFilterFactory.forName(name, parent.applyDefaultParams(params)));
      return this;
    }

    /** Adds the given token filter.
     * @param name is used to look up the factory with {@link TokenFilterFactory#forName(String, Map)}.
     *  The list of possible names can be looked up with {@link TokenFilterFactory#availableTokenFilters()}.
     * @param params the map of parameters to be passed to factory. The map must be modifiable.
     */
    public ConditionBuilder addTokenFilter(String name, String... params) throws IOException {
      return addTokenFilter(name, parent.paramsToMap(params));
    }

    /** Adds the given token filter.
     * @param factory class that is used to create the token filter.
     * @param params the map of parameters to be passed to factory. The map must be modifiable.
     */
    public ConditionBuilder addTokenFilter(Class<? extends TokenFilterFactory> factory, Map<String, String> params) throws IOException {
      innerFilters.add(newFactoryClassInstance(factory, parent.applyDefaultParams(params)));
      return this;
    }

    /** Adds the given token filter.
     * @param factory class that is used to create the token filter.
     * @param params the map of parameters to be passed to factory. The map must be modifiable.
     */
    public ConditionBuilder addTokenFilter(Class<? extends TokenFilterFactory> factory, String... params) throws IOException {
      return addTokenFilter(factory, parent.paramsToMap(params));
    }

    /**
     * Close the branch and return to the main analysis chain
     */
    public Builder endwhen() throws IOException {
      factory.setInnerFilters(innerFilters);
      parent.applyResourceLoader(factory);
      parent.addTokenFilter(factory);
      return parent;
    }
  }

}
