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
package org.apache.solr.spelling.suggest;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.HighFrequencyDictionary;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.suggest.FileDictionary;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.analyzing.AnalyzingSuggester;
import org.apache.lucene.search.suggest.fst.WFSTCompletionLookup;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.spelling.SolrSpellChecker;
import org.apache.solr.spelling.SpellingOptions;
import org.apache.solr.spelling.SpellingResult;
import org.apache.solr.spelling.Token;
import org.apache.solr.spelling.suggest.fst.FSTLookupFactory;
import org.apache.solr.spelling.suggest.jaspell.JaspellLookupFactory;
import org.apache.solr.spelling.suggest.tst.TSTLookupFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Suggester extends SolrSpellChecker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  /** Location of the source data - either a path to a file, or null for the
   * current IndexReader.
   */
  public static final String LOCATION = "sourceLocation";
  /** Fully-qualified class of the {@link Lookup} implementation. */
  public static final String LOOKUP_IMPL = "lookupImpl";
  /**
   * Minimum frequency of terms to consider when building the dictionary.
   */
  public static final String THRESHOLD_TOKEN_FREQUENCY = "threshold";
  /**
   * Name of the location where to persist the dictionary. If this location
   * is relative then the data will be stored under the core's dataDir. If this
   * is null the storing will be disabled.
   */
  public static final String STORE_DIR = "storeDir";
  
  protected String sourceLocation;
  protected File storeDir;
  protected float threshold;
  protected Dictionary dictionary;
  protected IndexReader reader;
  protected Lookup lookup;
  protected String lookupImpl;
  protected SolrCore core;

  private LookupFactory factory;
  
  @Override
  public String init(@SuppressWarnings({"rawtypes"})NamedList config, SolrCore core) {
    log.info("init: {}", config);
    String name = super.init(config, core);
    threshold = config.get(THRESHOLD_TOKEN_FREQUENCY) == null ? 0.0f
            : (Float)config.get(THRESHOLD_TOKEN_FREQUENCY);
    sourceLocation = (String) config.get(LOCATION);
    lookupImpl = (String)config.get(LOOKUP_IMPL);

    // support the old classnames without -Factory for config file backwards compatibility.
    if (lookupImpl == null || "org.apache.solr.spelling.suggest.jaspell.JaspellLookup".equals(lookupImpl)) {
      lookupImpl = JaspellLookupFactory.class.getName();
    } else if ("org.apache.solr.spelling.suggest.tst.TSTLookup".equals(lookupImpl)) {
      lookupImpl = TSTLookupFactory.class.getName();
    } else if ("org.apache.solr.spelling.suggest.fst.FSTLookup".equals(lookupImpl)) {
      lookupImpl = FSTLookupFactory.class.getName();
    }

    factory = core.getResourceLoader().newInstance(lookupImpl, LookupFactory.class);
    
    lookup = factory.create(config, core);
    core.addCloseHook(new CloseHook() {
      @Override
      public void preClose(SolrCore core) {
        if (lookup != null && lookup instanceof Closeable) {
          try {
            ((Closeable) lookup).close();
          } catch (IOException e) {
            log.warn("Could not close the suggester lookup.", e);
          }
        }
      }
      
      @Override
      public void postClose(SolrCore core) {}
    });
    
    String store = (String)config.get(STORE_DIR);
    if (store != null) {
      storeDir = new File(store);
      if (!storeDir.isAbsolute()) {
        storeDir = new File(core.getDataDir() + File.separator + storeDir);
      }
      if (!storeDir.exists()) {
        storeDir.mkdirs();
      } else {
        // attempt reload of the stored lookup
        try {
          lookup.load(new FileInputStream(new File(storeDir, factory.storeFileName())));
        } catch (IOException e) {
          log.warn("Loading stored lookup data failed", e);
        }
      }
    }
    
    return name;
  }
  
  @Override
  public void build(SolrCore core, SolrIndexSearcher searcher) throws IOException {
    log.info("build()");
    if (sourceLocation == null) {
      reader = searcher.getIndexReader();
      dictionary = new HighFrequencyDictionary(reader, field, threshold);
    } else {
      try {
        dictionary = new FileDictionary(new InputStreamReader(
                core.getResourceLoader().openResource(sourceLocation), StandardCharsets.UTF_8));
      } catch (UnsupportedEncodingException e) {
        // should not happen
        log.error("should not happen", e);
      }
    }

    lookup.build(dictionary);
    if (storeDir != null) {
      File target = new File(storeDir, factory.storeFileName());
      if(!lookup.store(new FileOutputStream(target))) {
        if (sourceLocation == null) {
          assert reader != null && field != null;
          log.error("Store Lookup build from index on field: {} failed reader has: {} docs", field, reader.maxDoc());
        } else {
          log.error("Store Lookup build from sourceloaction: {} failed", sourceLocation);
        }
      } else {
        if (log.isInfoEnabled()) {
          log.info("Stored suggest data to: {}", target.getAbsolutePath());
        }
      }
    }
  }

  @Override
  public void reload(SolrCore core, SolrIndexSearcher searcher) throws IOException {
    log.info("reload()");
    if (dictionary == null && storeDir != null) {
      // this may be a firstSearcher event, try loading it
      FileInputStream is = new FileInputStream(new File(storeDir, factory.storeFileName()));
      try {
        if (lookup.load(is)) {
          return;  // loaded ok
        }
      } finally {
        IOUtils.closeWhileHandlingException(is);
      }
      log.debug("load failed, need to build Lookup again");
    }
    // loading was unsuccessful - build it again
    build(core, searcher);
  }

  static SpellingResult EMPTY_RESULT = new SpellingResult();

  @Override
  public SpellingResult getSuggestions(SpellingOptions options) throws IOException {
    log.debug("getSuggestions: {}", options.tokens);
    if (lookup == null) {
      log.info("Lookup is null - invoke spellchecker.build first");
      return EMPTY_RESULT;
    }
    SpellingResult res = new SpellingResult();
    CharsRef scratch = new CharsRef();
    for (Token t : options.tokens) {
      scratch.chars = t.buffer();
      scratch.offset = 0;
      scratch.length = t.length();
      boolean onlyMorePopular = (options.suggestMode == SuggestMode.SUGGEST_MORE_POPULAR) &&
        !(lookup instanceof WFSTCompletionLookup) &&
        !(lookup instanceof AnalyzingSuggester);
      List<LookupResult> suggestions = lookup.lookup(scratch, onlyMorePopular, options.count);
      if (suggestions == null) {
        continue;
      }
      if (options.suggestMode != SuggestMode.SUGGEST_MORE_POPULAR) {
        Collections.sort(suggestions);
      }
      for (LookupResult lr : suggestions) {
        res.add(t, lr.key.toString(), (int)lr.value);
      }
    }
    return res;
  }
}
