package org.apache.solr.spelling;

import org.apache.lucene.search.spell.StringDistance;
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.spell.SuggestWordFrequencyComparator;
import org.apache.lucene.search.spell.SuggestWordQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.LevensteinDistance;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.SolrIndexSearcher;


/**
 * Abstract base class for all Lucene-based spell checking implementations.
 * 
 * <p>
 * Refer to <a href="http://wiki.apache.org/solr/SpellCheckComponent">SpellCheckComponent</a>
 * for more details.
 * </p>
 * 
 * @since solr 1.3
 */
public abstract class AbstractLuceneSpellChecker extends SolrSpellChecker {
  public static final Logger log = LoggerFactory.getLogger(AbstractLuceneSpellChecker.class);
  
  public static final String SPELLCHECKER_ARG_NAME = "spellchecker";
  public static final String LOCATION = "sourceLocation";
  public static final String INDEX_DIR = "spellcheckIndexDir";
  public static final String ACCURACY = "accuracy";
  public static final String STRING_DISTANCE = "distanceMeasure";
  public static final String COMPARATOR_CLASS = "comparatorClass";

  public static final String SCORE_COMP = "score";
  public static final String FREQ_COMP = "freq";

  protected org.apache.lucene.search.spell.SpellChecker spellChecker;

  protected String sourceLocation;
  /*
  * The Directory containing the Spell checking index
  * */
  protected Directory index;
  protected Dictionary dictionary;

  public static final int DEFAULT_SUGGESTION_COUNT = 5;
  protected String indexDir;
  protected float accuracy = 0.5f;
  public static final String FIELD = "field";

  protected StringDistance sd;

  @Override
  public String init(NamedList config, SolrCore core) {
    super.init(config, core);
    indexDir = (String) config.get(INDEX_DIR);
    String accuracy = (String) config.get(ACCURACY);
    //If indexDir is relative then create index inside core.getDataDir()
    if (indexDir != null)   {
      if (!new File(indexDir).isAbsolute()) {
        indexDir = core.getDataDir() + File.separator + indexDir;
      }
    }
    sourceLocation = (String) config.get(LOCATION);
    String compClass = (String) config.get(COMPARATOR_CLASS);
    Comparator<SuggestWord> comp = null;
    if (compClass != null){
      if (compClass.equalsIgnoreCase(SCORE_COMP)){
        comp = SuggestWordQueue.DEFAULT_COMPARATOR;
      } else if (compClass.equalsIgnoreCase(FREQ_COMP)){
        comp = new SuggestWordFrequencyComparator();
      } else{//must be a FQCN
        comp = (Comparator<SuggestWord>) core.getResourceLoader().newInstance(compClass);
      }
    } else {
      comp = SuggestWordQueue.DEFAULT_COMPARATOR;
    }
    String strDistanceName = (String)config.get(STRING_DISTANCE);
    if (strDistanceName != null) {
      sd = (StringDistance) core.getResourceLoader().newInstance(strDistanceName);
      //TODO: Figure out how to configure options.  Where's Spring when you need it?  Or at least BeanUtils...
    } else {
      sd = new LevensteinDistance();
    }
    try {
      initIndex();
      spellChecker = new SpellChecker(index, sd, comp);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (accuracy != null) {
      try {
        this.accuracy = Float.parseFloat(accuracy);
        spellChecker.setAccuracy(this.accuracy);
      } catch (NumberFormatException e) {
        throw new RuntimeException(
                "Unparseable accuracy given for dictionary: " + name, e);
      }
    }
    return name;
  }
  
  @Override
  public SpellingResult getSuggestions(SpellingOptions options) throws IOException {
  	SpellingResult result = new SpellingResult(options.tokens);
    IndexReader reader = determineReader(options.reader);
    Term term = field != null ? new Term(field, "") : null;
    float theAccuracy = (options.accuracy == Float.MIN_VALUE) ? spellChecker.getAccuracy() : options.accuracy;
    
    int count = Math.max(options.count, AbstractLuceneSpellChecker.DEFAULT_SUGGESTION_COUNT);
    SuggestMode mode = options.onlyMorePopular ? SuggestMode.SUGGEST_MORE_POPULAR : SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
    for (Token token : options.tokens) {
      String tokenText = new String(token.buffer(), 0, token.length());
      String[] suggestions = spellChecker.suggestSimilar(tokenText,
              count,
            field != null ? reader : null, //workaround LUCENE-1295
            field,
            mode, theAccuracy);
      if (suggestions.length == 1 && suggestions[0].equals(tokenText)) {
      	//These are spelled the same, continue on
        continue;
      }

      if (options.extendedResults == true && reader != null && field != null) {
        term = new Term(field, tokenText);
        result.addFrequency(token, reader.docFreq(term));
        int countLimit = Math.min(options.count, suggestions.length);
        if(countLimit>0)
        {
	        for (int i = 0; i < countLimit; i++) {
	          term = new Term(field, suggestions[i]);
	          result.add(token, suggestions[i], reader.docFreq(term));
	        }
        } else {
        	List<String> suggList = Collections.emptyList();
        	result.add(token, suggList);
        }
      } else {
        if (suggestions.length > 0) {
          List<String> suggList = Arrays.asList(suggestions);
          if (suggestions.length > options.count) {
            suggList = suggList.subList(0, options.count);
          }
          result.add(token, suggList);
        } else {
        	List<String> suggList = Collections.emptyList();
        	result.add(token, suggList);
        }
      }
    }
    return result;
  }

  protected IndexReader determineReader(IndexReader reader) {
    return reader;
  }

  @Override
  public void reload(SolrCore core, SolrIndexSearcher searcher) throws IOException {
    spellChecker.setSpellIndex(index);

  }

  /**
   * Initialize the {@link #index} variable based on the {@link #indexDir}.  Does not actually create the spelling index.
   *
   * @throws IOException
   */
  protected void initIndex() throws IOException {
    if (indexDir != null) {
      index = FSDirectory.open(new File(indexDir));
    } else {
      index = new RAMDirectory();
    }
  }

  /*
  * @return the Accuracy used for the Spellchecker
  * */
  @Override
  public float getAccuracy() {
    return accuracy;
  }

  /*
  * @return the Field used
  *
  * */
  public String getField() {
    return field;
  }

  /*
  *
  * @return the FieldType name.
  * */
  public String getFieldTypeName() {
    return fieldTypeName;
  }


  /*
  * @return the Index directory
  * */
  public String getIndexDir() {
    return indexDir;
  }

  /*
  * @return the location of the source
  * */
  public String getSourceLocation() {
    return sourceLocation;
  }

  @Override
  public StringDistance getStringDistance() {
    return sd;
  }

  public SpellChecker getSpellChecker() {
    return spellChecker;
  }
}
