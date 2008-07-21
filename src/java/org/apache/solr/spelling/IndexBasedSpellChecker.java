package org.apache.solr.spelling;
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

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.HighFrequencyDictionary;

import java.io.IOException;
import java.util.logging.Logger;


/**
 * <p>
 * A spell checker implementation which can load words from Solr as well as arbitary Lucene indices.
 * </p>
 * 
 * <p>
 * Refer to http://wiki.apache.org/solr/SpellCheckComponent for more details
 * </p>
 * 
 * @since solr 1.3
 **/
public class IndexBasedSpellChecker extends AbstractLuceneSpellChecker {
  private static final Logger log = Logger.getLogger(IndexBasedSpellChecker.class.getName());

  public static final String THRESHOLD_TOKEN_FREQUENCY = "thresholdTokenFrequency";

  protected float threshold;
  protected IndexReader reader;

  public String init(NamedList config, SolrResourceLoader loader) {
    super.init(config, loader);
    threshold = config.get(THRESHOLD_TOKEN_FREQUENCY) == null ? 0.0f
            : (Float) config.get(THRESHOLD_TOKEN_FREQUENCY);
    initSourceReader();
    return name;
  }

  private void initSourceReader() {
    if (sourceLocation != null) {
      try {
        FSDirectory luceneIndexDir = FSDirectory.getDirectory(sourceLocation);
        this.reader = IndexReader.open(luceneIndexDir);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void build(SolrCore core, SolrIndexSearcher searcher) {
    IndexReader reader = null;
    try {
      if (sourceLocation == null) {
        // Load from Solr's index
        reader = searcher.getReader();
      } else {
        // Load from Lucene index at given sourceLocation
        reader = this.reader;
      }


      loadLuceneDictionary(core.getSchema(), reader);
      spellChecker.clearIndex();
      spellChecker.indexDictionary(dictionary);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected IndexReader determineReader(IndexReader reader) {
    IndexReader result = null;
    if (sourceLocation != null) {
      result = this.reader;
    } else {
      result = reader;
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private void loadLuceneDictionary(IndexSchema schema, IndexReader reader) {
    // Create the dictionary
    dictionary = new HighFrequencyDictionary(reader, field,
            threshold);
    // Get the field's analyzer
    FieldType fieldType = schema.getFieldTypeNoEx(field);
    analyzer = fieldType == null ? new WhitespaceAnalyzer()
            : fieldType.getQueryAnalyzer();
  }

  @Override
  public void reload() throws IOException {
    super.reload();
    //reload the source
    initSourceReader();
  }
}
