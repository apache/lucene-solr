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
package org.apache.solr.handler.clustering.carrot2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.commongrams.CommonGramsFilterFactory;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.carrot2.core.LanguageCode;
import org.carrot2.core.attribute.Init;
import org.carrot2.core.attribute.Processing;
import org.carrot2.text.linguistic.DefaultLexicalDataFactory;
import org.carrot2.text.linguistic.ILexicalData;
import org.carrot2.text.linguistic.ILexicalDataFactory;
import org.carrot2.text.util.MutableCharArray;
import org.carrot2.util.attribute.Attribute;
import org.carrot2.util.attribute.Bindable;
import org.carrot2.util.attribute.Input;

/**
 * An implementation of Carrot2's {@link ILexicalDataFactory} that adds stop
 * words from a field's StopFilter to the default stop words used in Carrot2,
 * for all languages Carrot2 supports. Completely replacing Carrot2 stop words
 * with Solr's wouldn't make much sense because clustering needs more aggressive
 * stop words removal. In other words, if something is a stop word during
 * indexing, then it should also be a stop word during clustering, but not the
 * other way round.
 * 
 * @lucene.experimental
 */
@Bindable
public class SolrStopwordsCarrot2LexicalDataFactory implements ILexicalDataFactory {

  @Init
  @Input
  @Attribute(key = "solrCore")
  public SolrCore core;

  @Processing
  @Input
  @Attribute(key = "solrFieldNames")
  public Set<String> fieldNames;

  /**
   * A lazily-built cache of stop words per field.
   */
  private HashMap<String, List<CharArraySet>> solrStopWords = new HashMap<>();

  /**
   * Carrot2's default lexical resources to use in addition to Solr's stop
   * words.
   */
  public DefaultLexicalDataFactory carrot2LexicalDataFactory = new DefaultLexicalDataFactory();

  /**
   * Obtains stop words for a field from the associated
   * {@link StopFilterFactory}, if any.
   */
  private List<CharArraySet> getSolrStopWordsForField(String fieldName) {
    // No need to synchronize here, Carrot2 ensures that instances
    // of this class are not used by multiple threads at a time.
    synchronized (solrStopWords) {
      if (!solrStopWords.containsKey(fieldName)) {
        solrStopWords.put(fieldName, new ArrayList<>());

        IndexSchema schema = core.getLatestSchema();
        final Analyzer fieldAnalyzer = schema.getFieldType(fieldName).getIndexAnalyzer();
        if (fieldAnalyzer instanceof TokenizerChain) {
          final TokenFilterFactory[] filterFactories = 
              ((TokenizerChain) fieldAnalyzer).getTokenFilterFactories();
          for (TokenFilterFactory factory : filterFactories) {
            if (factory instanceof StopFilterFactory) {
              // StopFilterFactory holds the stop words in a CharArraySet
              CharArraySet stopWords = ((StopFilterFactory) factory).getStopWords();
              solrStopWords.get(fieldName).add(stopWords);
            }

            if (factory instanceof CommonGramsFilterFactory) {
              CharArraySet commonWords = ((CommonGramsFilterFactory) factory).getCommonWords();
              solrStopWords.get(fieldName).add(commonWords);
            }
          }
        }
      }
      return solrStopWords.get(fieldName);
    }
  }

  @Override
  public ILexicalData getLexicalData(LanguageCode languageCode) {
    final ILexicalData carrot2LexicalData = carrot2LexicalDataFactory
        .getLexicalData(languageCode);

    return new ILexicalData() {
      @Override
      public boolean isStopLabel(CharSequence word) {
        // Nothing in Solr maps to the concept of a stop label,
        // so return Carrot2's default here.
        return carrot2LexicalData.isStopLabel(word);
      }

      @Override
      public boolean isCommonWord(MutableCharArray word) {
        // Loop over the fields involved in clustering first
        for (String fieldName : fieldNames) {
          for (CharArraySet stopWords : getSolrStopWordsForField(fieldName)) {
            if (stopWords.contains(word)) {
              return true;
            }
          }
        }
        // Check default Carrot2 stop words too
        return carrot2LexicalData.isCommonWord(word);
      }
    };
  }
}
