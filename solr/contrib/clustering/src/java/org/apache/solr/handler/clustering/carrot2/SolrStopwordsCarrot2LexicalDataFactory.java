package org.apache.solr.handler.clustering.carrot2;

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

import java.util.Collection;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.solr.analysis.CommonGramsFilterFactory;
import org.apache.solr.analysis.StopFilterFactory;
import org.apache.solr.analysis.TokenFilterFactory;
import org.apache.solr.analysis.TokenizerChain;
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
import org.slf4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * An implementation of Carrot2's {@link ILexicalDataFactory} that adds stop
 * words from a field's StopFilter to the default stop words used in Carrot2,
 * for all languages Carrot2 supports. Completely replacing Carrot2 stop words
 * with Solr's wouldn't make much sense because clustering needs more aggressive
 * stop words removal. In other words, if something is a stop word during
 * indexing, then it should also be a stop word during clustering, but not the
 * other way round.
 */
@Bindable
public class SolrStopwordsCarrot2LexicalDataFactory implements
    ILexicalDataFactory {
  final static Logger logger = org.slf4j.LoggerFactory
      .getLogger(SolrStopwordsCarrot2LexicalDataFactory.class);

  @Init
  @Input
  @Attribute(key = "solrIndexSchema")
  private IndexSchema schema;

  @Processing
  @Input
  @Attribute(key = "solrFieldNames")
  private Set<String> fieldNames;

  /**
   * A lazily-built cache of stop words per field.
   */
  private Multimap<String, CharArraySet> solrStopWords = HashMultimap.create();

  /**
   * Carrot2's default lexical resources to use in addition to Solr's stop
   * words.
   */
  private DefaultLexicalDataFactory carrot2LexicalDataFactory = new DefaultLexicalDataFactory();

  /**
   * Obtains stop words for a field from the associated
   * {@link StopFilterFactory}, if any.
   */
  private Collection<CharArraySet> getSolrStopWordsForField(String fieldName) {
    // No need to synchronize here, Carrot2 ensures that instances
    // of this class are not used by multiple threads at a time.
    if (!solrStopWords.containsKey(fieldName)) {
      final Analyzer fieldAnalyzer = schema.getFieldType(fieldName)
          .getAnalyzer();
      if (fieldAnalyzer instanceof TokenizerChain) {
        final TokenFilterFactory[] filterFactories = ((TokenizerChain) fieldAnalyzer)
            .getTokenFilterFactories();
        for (TokenFilterFactory factory : filterFactories) {
          if (factory instanceof StopFilterFactory) {
            // StopFilterFactory holds the stop words in a CharArraySet, but
            // the getStopWords() method returns a Set<?>, so we need to cast.
            solrStopWords.put(fieldName,
                (CharArraySet) ((StopFilterFactory) factory).getStopWords());
          }

          if (factory instanceof CommonGramsFilterFactory) {
            solrStopWords.put(fieldName,
                (CharArraySet) ((CommonGramsFilterFactory) factory)
                    .getCommonWords());
          }
        }
      }
    }
    return solrStopWords.get(fieldName);
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
