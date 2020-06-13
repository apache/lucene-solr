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
package org.apache.solr.spelling;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.util.List;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.spell.HighFrequencyDictionary;
import org.apache.lucene.search.spell.PlainTextDictionary;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * <p>
 * A spell checker implementation that loads words from a text file (one word per line).
 * </p>
 *
 * @since solr 1.3
 **/
public class FileBasedSpellChecker extends AbstractLuceneSpellChecker {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String SOURCE_FILE_CHAR_ENCODING = "characterEncoding";

  private String characterEncoding;
  public static final String WORD_FIELD_NAME = "word";

  @Override
  public String init(@SuppressWarnings({"rawtypes"})NamedList config, SolrCore core) {
    super.init(config, core);
    characterEncoding = (String) config.get(SOURCE_FILE_CHAR_ENCODING);
    return name;
  }

  @Override
  public void build(SolrCore core, SolrIndexSearcher searcher) throws IOException {
    loadExternalFileDictionary(core, searcher);
    spellChecker.clearIndex();
    // TODO: you should be able to specify the IWC params?
    // TODO: if we enable this, codec gets angry since field won't exist in the schema
    // config.setCodec(core.getCodec());
    spellChecker.indexDictionary(dictionary, new IndexWriterConfig(null), false);
  }

  /**
   * Override to return null, since there is no reader associated with a file based index
   */
  @Override
  protected IndexReader determineReader(IndexReader reader) {
    return null;
  }

  private void loadExternalFileDictionary(SolrCore core, SolrIndexSearcher searcher) {
    try {
      IndexSchema schema = null == searcher ? core.getLatestSchema() : searcher.getSchema();
      // Get the field's analyzer
      if (fieldTypeName != null && schema.getFieldTypeNoEx(fieldTypeName) != null) {
        FieldType fieldType = schema.getFieldTypes().get(fieldTypeName);
        // Do index-time analysis using the given fieldType's analyzer
        RAMDirectory ramDir = new RAMDirectory();

        LogMergePolicy mp = new LogByteSizeMergePolicy();
        mp.setMergeFactor(300);

        IndexWriter writer = new IndexWriter(
            ramDir,
            new IndexWriterConfig(fieldType.getIndexAnalyzer()).
                setMaxBufferedDocs(150).
                setMergePolicy(mp).
                setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                // TODO: if we enable this, codec gets angry since field won't exist in the schema
                // .setCodec(core.getCodec())
        );

        List<String> lines = core.getResourceLoader().getLines(sourceLocation, characterEncoding);

        for (String s : lines) {
          Document d = new Document();
          d.add(new TextField(WORD_FIELD_NAME, s, Field.Store.NO));
          writer.addDocument(d);
        }
        writer.forceMerge(1);
        writer.close();

        dictionary = new HighFrequencyDictionary(DirectoryReader.open(ramDir),
                WORD_FIELD_NAME, 0.0f);
      } else {
        // check if character encoding is defined
        if (characterEncoding == null) {
          dictionary = new PlainTextDictionary(core.getResourceLoader().openResource(sourceLocation));
        } else {
          dictionary = new PlainTextDictionary(new InputStreamReader(core.getResourceLoader().openResource(sourceLocation), characterEncoding));
        }
      }


    } catch (IOException e) {
      log.error( "Unable to load spellings", e);
    }
  }

  public String getCharacterEncoding() {
    return characterEncoding;
  }
}
