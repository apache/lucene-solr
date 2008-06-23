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
package org.apache.solr.spelling;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spell.PlainTextDictionary;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.HighFrequencyDictionary;


/**
 * <p>
 * A spell checker implementation which can load words from a text 
 * file (one word per line).
 * </p>
 *
 * @since solr 1.3
 **/
public class FileBasedSpellChecker extends AbstractLuceneSpellChecker {

  private static final Logger log = Logger.getLogger(FileBasedSpellChecker.class.getName());

  public static final String FIELD_TYPE = "fieldType";

  public static final String SOURCE_FILE_CHAR_ENCODING = "characterEncoding";

  private String fieldTypeName;
  private String characterEncoding;
  public static final String WORD_FIELD_NAME = "word";

  public String init(NamedList config, SolrResourceLoader loader) {
    super.init(config, loader);
    fieldTypeName = (String) config.get(FIELD_TYPE);
    characterEncoding = (String) config.get(SOURCE_FILE_CHAR_ENCODING);
    return name;
  }

  public void build(SolrCore core) {
    try {
      loadExternalFileDictionary(core.getSchema(), core.getResourceLoader());
      spellChecker.clearIndex();
      spellChecker.indexDictionary(dictionary);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Override to return null, since there is no reader associated with a file based index
   */
  @Override
  protected IndexReader determineReader(IndexReader reader) {
    return null;
  }

  @SuppressWarnings("unchecked")
  private void loadExternalFileDictionary(IndexSchema schema, SolrResourceLoader loader) {
    IndexSearcher searcher = null;
    try {

      // Get the field's analyzer
      if (fieldTypeName != null
              && schema.getFieldTypeNoEx(fieldTypeName) != null) {
        FieldType fieldType = schema.getFieldTypes()
                .get(fieldTypeName);
        // Do index-time analysis using the given fieldType's analyzer
        RAMDirectory ramDir = new RAMDirectory();
        IndexWriter writer = new IndexWriter(ramDir, fieldType.getAnalyzer(),
                true, IndexWriter.MaxFieldLength.UNLIMITED);
        writer.setMergeFactor(300);
        writer.setMaxBufferedDocs(150);

        List<String> lines = loader.getLines(sourceLocation, characterEncoding);

        for (String s : lines) {
          Document d = new Document();
          d.add(new Field(WORD_FIELD_NAME, s, Field.Store.NO, Field.Index.TOKENIZED));
          writer.addDocument(d);
        }
        writer.optimize();
        writer.close();

        dictionary = new HighFrequencyDictionary(IndexReader.open(ramDir),
                WORD_FIELD_NAME, 0.0f);
        analyzer = fieldType.getQueryAnalyzer();
      } else {
        log.warning("No fieldType: " + fieldTypeName
                + " found for dictionary: " + name + ".  Using WhitespaceAnalzyer.");
        analyzer = new WhitespaceAnalyzer();

        // check if character encoding is defined
        if (characterEncoding == null) {
          dictionary = new PlainTextDictionary(loader.openResource(sourceLocation));
        } else {
          dictionary = new PlainTextDictionary(new InputStreamReader(loader.openResource(sourceLocation), characterEncoding));
        }
      }


    } catch (IOException e) {
      log.log(Level.SEVERE, "Unable to load spellings", e);
    } finally {
      try {
        if (searcher != null)
          searcher.close();
      } catch (IOException e) {
        // Ignore
      }
    }
  }
  
}
