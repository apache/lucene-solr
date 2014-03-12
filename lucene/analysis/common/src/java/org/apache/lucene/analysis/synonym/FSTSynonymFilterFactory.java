package org.apache.lucene.analysis.synonym;

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

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.WordnetSynonymParser;
import org.apache.lucene.analysis.util.*;
import org.apache.lucene.util.Version;

/**
 * @deprecated (3.4) use {@link SynonymFilterFactory} instead. this is only a backwards compatibility
 *                   mechanism that will be removed in Lucene 5.0
 */
// NOTE: rename this to "SynonymFilterFactory" and nuke that delegator in Lucene 5.0!
@Deprecated
final class FSTSynonymFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  private final boolean ignoreCase;
  private final String tokenizerFactory;
  private final String synonyms;
  private final String format;
  private final boolean expand;
  private final Map<String, String> tokArgs = new HashMap<>();

  private SynonymMap map;
  
  public FSTSynonymFilterFactory(Map<String,String> args) {
    super(args);
    ignoreCase = getBoolean(args, "ignoreCase", false);
    synonyms = require(args, "synonyms");
    format = get(args, "format");
    expand = getBoolean(args, "expand", true);

    tokenizerFactory = get(args, "tokenizerFactory");
    if (tokenizerFactory != null) {
      assureMatchVersion();
      tokArgs.put("luceneMatchVersion", getLuceneMatchVersion().toString());
      for (Iterator<String> itr = args.keySet().iterator(); itr.hasNext();) {
        String key = itr.next();
        tokArgs.put(key.replaceAll("^tokenizerFactory\\.",""), args.get(key));
        itr.remove();
      }
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }
  
  @Override
  public TokenStream create(TokenStream input) {
    // if the fst is null, it means there's actually no synonyms... just return the original stream
    // as there is nothing to do here.
    return map.fst == null ? input : new SynonymFilter(input, map, ignoreCase);
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    final TokenizerFactory factory = tokenizerFactory == null ? null : loadTokenizerFactory(loader, tokenizerFactory);
    
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = factory == null ? new WhitespaceTokenizer(Version.LUCENE_CURRENT, reader) : factory.create(reader);
        TokenStream stream = ignoreCase ? new LowerCaseFilter(Version.LUCENE_CURRENT, tokenizer) : tokenizer;
        return new TokenStreamComponents(tokenizer, stream);
      }
    };

    try {
      String formatClass = format;
      if (format == null || format.equals("solr")) {
        formatClass = SolrSynonymParser.class.getName();
      } else if (format.equals("wordnet")) {
        formatClass = WordnetSynonymParser.class.getName();
      }
      // TODO: expose dedup as a parameter?
      map = loadSynonyms(loader, formatClass, true, analyzer);
    } catch (ParseException e) {
      throw new IOException("Error parsing synonyms file:", e);
    }
  }

  /**
   * Load synonyms with the given {@link SynonymMap.Parser} class.
   */
  private SynonymMap loadSynonyms(ResourceLoader loader, String cname, boolean dedup, Analyzer analyzer) throws IOException, ParseException {
    CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);

    SynonymMap.Parser parser;
    Class<? extends SynonymMap.Parser> clazz = loader.findClass(cname, SynonymMap.Parser.class);
    try {
      parser = clazz.getConstructor(boolean.class, boolean.class, Analyzer.class).newInstance(dedup, expand, analyzer);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    File synonymFile = new File(synonyms);
    if (synonymFile.exists()) {
      decoder.reset();
      parser.parse(new InputStreamReader(loader.openResource(synonyms), decoder));
    } else {
      List<String> files = splitFileNames(synonyms);
      for (String file : files) {
        decoder.reset();
        parser.parse(new InputStreamReader(loader.openResource(file), decoder));
      }
    }
    return parser.build();
  }
  
  // (there are no tests for this functionality)
  private TokenizerFactory loadTokenizerFactory(ResourceLoader loader, String cname) throws IOException {
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
}
