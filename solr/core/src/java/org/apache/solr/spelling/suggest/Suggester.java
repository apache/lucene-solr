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

package org.apache.solr.spelling.suggest;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.HighFrequencyDictionary;
import org.apache.lucene.search.suggest.FileDictionary;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.Lookup.LookupResult;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.spelling.SolrSpellChecker;
import org.apache.solr.spelling.SpellingOptions;
import org.apache.solr.spelling.SpellingResult;
import org.apache.solr.spelling.suggest.fst.FSTLookupFactory;
import org.apache.solr.spelling.suggest.jaspell.JaspellLookupFactory;
import org.apache.solr.spelling.suggest.tst.TSTLookupFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Suggester extends SolrSpellChecker {
  private static final Logger LOG = LoggerFactory.getLogger(Suggester.class);
  
  /** Location of the source data - either a path to a file, or null for the
   * current IndexReader.
   */
  public static final String LOCATION = "sourceLocation";
  /** Field to use as the source of terms if using IndexReader. */
  public static final String FIELD = "field";
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
  protected String field;
  protected float threshold;
  protected Dictionary dictionary;
  protected IndexReader reader;
  protected Lookup lookup;
  protected String lookupImpl;
  protected SolrCore core;
  
  @Override
  public String init(NamedList config, SolrCore core) {
    LOG.info("init: " + config);
    String name = super.init(config, core);
    threshold = config.get(THRESHOLD_TOKEN_FREQUENCY) == null ? 0.0f
            : (Float)config.get(THRESHOLD_TOKEN_FREQUENCY);
    sourceLocation = (String) config.get(LOCATION);
    field = (String)config.get(FIELD);
    lookupImpl = (String)config.get(LOOKUP_IMPL);

    // support the old classnames without -Factory for config file backwards compatibility.
    if (lookupImpl == null || "org.apache.solr.spelling.suggest.jaspell.JaspellLookup".equals(lookupImpl)) {
      lookupImpl = JaspellLookupFactory.class.getName();
    } else if ("org.apache.solr.spelling.suggest.tst.TSTLookup".equals(lookupImpl)) {
      lookupImpl = TSTLookupFactory.class.getName();
    } else if ("org.apache.solr.spelling.suggest.fst.FSTLookup".equals(lookupImpl)) {
      lookupImpl = FSTLookupFactory.class.getName();
    }

    LookupFactory factory = (LookupFactory) core.getResourceLoader().newInstance(lookupImpl);
    lookup = factory.create(config, core);
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
          lookup.load(storeDir);
        } catch (IOException e) {
          LOG.warn("Loading stored lookup data failed", e);
        }
      }
    }
    return name;
  }
  
  @Override
  public void build(SolrCore core, SolrIndexSearcher searcher) {
    LOG.info("build()");
    if (sourceLocation == null) {
      reader = searcher.getIndexReader();
      dictionary = new HighFrequencyDictionary(reader, field, threshold);
    } else {
      try {
        dictionary = new FileDictionary(new InputStreamReader(
                core.getResourceLoader().openResource(sourceLocation), "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        // should not happen
        LOG.error("should not happen", e);
      }
    }
    try {
      lookup.build(dictionary);
      if (storeDir != null) {
        lookup.store(storeDir);
      }
    } catch (Exception e) {
      LOG.error("Error while building or storing Suggester data", e);
    }
  }

  @Override
  public void reload(SolrCore core, SolrIndexSearcher searcher) throws IOException {
    LOG.info("reload()");
    if (dictionary == null && storeDir != null) {
      // this may be a firstSearcher event, try loading it
      if (lookup.load(storeDir)) {
        return;  // loaded ok
      }
      LOG.debug("load failed, need to build Lookup again");
    }
    // loading was unsuccessful - build it again
    build(core, searcher);
  }

  public void add(String query, int numHits) {
    LOG.info("add " + query + ", " + numHits);
    lookup.add(query, new Integer(numHits));
  }
  
  static SpellingResult EMPTY_RESULT = new SpellingResult();

  @Override
  public SpellingResult getSuggestions(SpellingOptions options) throws IOException {
    LOG.debug("getSuggestions: " + options.tokens);
    if (lookup == null) {
      LOG.info("Lookup is null - invoke spellchecker.build first");
      return EMPTY_RESULT;
    }
    SpellingResult res = new SpellingResult();
    for (Token t : options.tokens) {
      String term = new String(t.buffer(), 0, t.length());
      List<LookupResult> suggestions = lookup.lookup(term,
          options.onlyMorePopular, options.count);
      if (suggestions == null) {
        continue;
      }
      if (!options.onlyMorePopular) {
        Collections.sort(suggestions);
      }
      for (LookupResult lr : suggestions) {
        res.add(t, lr.key, ((Number)lr.value).intValue());
      }
    }
    return res;
  }
}
