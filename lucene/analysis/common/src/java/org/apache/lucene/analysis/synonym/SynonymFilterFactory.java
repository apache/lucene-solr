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
package org.apache.lucene.analysis.synonym;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenizerFactory;
import org.apache.lucene.analysis.core.FlattenGraphFilterFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;

/**
 * Factory for {@link SynonymFilter}.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_synonym" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.SynonymFilterFactory" synonyms="synonyms.txt"
 *             format="solr" ignoreCase="false" expand="true"
 *             tokenizerFactory="solr.WhitespaceTokenizerFactory"
 *             [optional tokenizer factory parameters]/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * <p>An optional param name prefix of "tokenizerFactory." may be used for any init params that the
 * SynonymFilterFactory needs to pass to the specified TokenizerFactory. If the TokenizerFactory
 * expects an init parameters with the same name as an init param used by the SynonymFilterFactory,
 * the prefix is mandatory.
 *
 * <p>The optional {@code format} parameter controls how the synonyms will be parsed: It supports
 * the short names of {@code solr} for {@link SolrSynonymParser} and {@code wordnet} for and {@link
 * WordnetSynonymParser}, or your own {@code SynonymMap.Parser} class name. The default is {@code
 * solr}. A custom {@link SynonymMap.Parser} is expected to have a constructor taking:
 *
 * <ul>
 *   <li><code>boolean dedup</code> - true if duplicates should be ignored, false otherwise
 *   <li><code>boolean expand</code> - true if conflation groups should be expanded, false if they
 *       are one-directional
 *   <li><code>{@link Analyzer} analyzer</code> - an analyzer used for each raw synonym
 * </ul>
 *
 * @see SolrSynonymParser SolrSynonymParser: default format
 * @deprecated Use {@link SynonymGraphFilterFactory} instead, but be sure to also use {@link
 *     FlattenGraphFilterFactory} at index time (not at search time) as well.
 * @since 3.1
 * @lucene.spi {@value #NAME}
 */
@Deprecated
public class SynonymFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {

  /** SPI name */
  public static final String NAME = "synonym";

  private final boolean ignoreCase;
  private final String tokenizerFactory;
  private final String synonyms;
  private final String format;
  private final boolean expand;
  private final String analyzerName;
  private final Map<String, String> tokArgs = new HashMap<>();

  private SynonymMap map;

  public SynonymFilterFactory(Map<String, String> args) {
    super(args);
    ignoreCase = getBoolean(args, "ignoreCase", false);
    synonyms = require(args, "synonyms");
    format = get(args, "format");
    expand = getBoolean(args, "expand", true);

    analyzerName = get(args, "analyzer");
    tokenizerFactory = get(args, "tokenizerFactory");
    if (analyzerName != null && tokenizerFactory != null) {
      throw new IllegalArgumentException(
          "Analyzer and TokenizerFactory can't be specified both: "
              + analyzerName
              + " and "
              + tokenizerFactory);
    }

    if (tokenizerFactory != null) {
      tokArgs.put("luceneMatchVersion", getLuceneMatchVersion().toString());
      for (Iterator<String> itr = args.keySet().iterator(); itr.hasNext(); ) {
        String key = itr.next();
        tokArgs.put(key.replaceAll("^tokenizerFactory\\.", ""), args.get(key));
        itr.remove();
      }
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public SynonymFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public TokenStream create(TokenStream input) {
    // if the fst is null, it means there's actually no synonyms... just return the original stream
    // as there is nothing to do here.
    return map.fst == null ? input : new SynonymFilter(input, map, ignoreCase);
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    final TokenizerFactory factory =
        tokenizerFactory == null ? null : loadTokenizerFactory(loader, tokenizerFactory);
    Analyzer analyzer;

    if (analyzerName != null) {
      analyzer = loadAnalyzer(loader, analyzerName);
    } else {
      analyzer =
          new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
              Tokenizer tokenizer = factory == null ? new WhitespaceTokenizer() : factory.create();
              TokenStream stream = ignoreCase ? new LowerCaseFilter(tokenizer) : tokenizer;
              return new TokenStreamComponents(tokenizer, stream);
            }
          };
    }

    try (Analyzer a = analyzer) {
      String formatClass = format;
      if (format == null || format.equals("solr")) {
        formatClass = SolrSynonymParser.class.getName();
      } else if (format.equals("wordnet")) {
        formatClass = WordnetSynonymParser.class.getName();
      }
      // TODO: expose dedup as a parameter?
      map = loadSynonyms(loader, formatClass, true, a);
    } catch (ParseException e) {
      throw new IOException("Error parsing synonyms file:", e);
    }
  }

  /** Load synonyms with the given {@link SynonymMap.Parser} class. */
  protected SynonymMap loadSynonyms(
      ResourceLoader loader, String cname, boolean dedup, Analyzer analyzer)
      throws IOException, ParseException {
    CharsetDecoder decoder =
        StandardCharsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);

    SynonymMap.Parser parser;
    Class<? extends SynonymMap.Parser> clazz = loader.findClass(cname, SynonymMap.Parser.class);
    try {
      parser =
          clazz
              .getConstructor(boolean.class, boolean.class, Analyzer.class)
              .newInstance(dedup, expand, analyzer);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    List<String> files = splitFileNames(synonyms);
    for (String file : files) {
      decoder.reset();
      try (final Reader isr = new InputStreamReader(loader.openResource(file), decoder)) {
        parser.parse(isr);
      }
    }
    return parser.build();
  }

  // (there are no tests for this functionality)
  private TokenizerFactory loadTokenizerFactory(ResourceLoader loader, String cname)
      throws IOException {
    Class<? extends TokenizerFactory> clazz = loader.findClass(cname, TokenizerFactory.class);
    try {
      TokenizerFactory tokFactory = clazz.getConstructor(Map.class).newInstance(tokArgs);
      if (tokFactory instanceof ResourceLoaderAware) {
        ((ResourceLoaderAware) tokFactory).inform(loader);
      }
      return tokFactory;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Analyzer loadAnalyzer(ResourceLoader loader, String cname) throws IOException {
    Class<? extends Analyzer> clazz = loader.findClass(cname, Analyzer.class);
    try {
      Analyzer analyzer = clazz.getConstructor().newInstance();
      if (analyzer instanceof ResourceLoaderAware) {
        ((ResourceLoaderAware) analyzer).inform(loader);
      }
      return analyzer;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
