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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.spell.SuggestWordFrequencyComparator;
import org.apache.lucene.search.spell.SuggestWordQueue;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
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
  @SuppressWarnings({"unchecked"})
  public String init(@SuppressWarnings({"rawtypes"})NamedList config, SolrCore core) {
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
        comp = (Comparator<SuggestWord>) core.getResourceLoader().newInstance(compClass, Comparator.class);
      }
    } else {
      comp = SuggestWordQueue.DEFAULT_COMPARATOR;
    }
    String strDistanceName = (String)config.get(STRING_DISTANCE);
    if (strDistanceName != null) {
      sd = core.getResourceLoader().newInstance(strDistanceName, StringDistance.class);
      //TODO: Figure out how to configure options.  Where's Spring when you need it?  Or at least BeanUtils...
    } else {
      sd = new LevenshteinDistance();
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
    for (Token token : options.tokens) {
      String tokenText = new String(token.buffer(), 0, token.length());
      term = new Term(field, tokenText);
      int docFreq = 0;
      if (reader != null) {
        docFreq = reader.docFreq(term);
      }
      String[] suggestions = spellChecker.suggestSimilar(tokenText,
          ((options.alternativeTermCount == 0 || docFreq == 0) ? count
              : options.alternativeTermCount), field != null ? reader : null, // workaround LUCENE-1295
          field, options.suggestMode, theAccuracy);
      if (suggestions.length == 1 && suggestions[0].equals(tokenText)
          && options.alternativeTermCount == 0) {
        // These are spelled the same, continue on
        continue;
      }
      // If considering alternatives to "correctly-spelled" terms, then add the
      // original as a viable suggestion.
      if (options.alternativeTermCount > 0 && docFreq > 0) {
        boolean foundOriginal = false;
        String[] suggestionsWithOrig = new String[suggestions.length + 1];
        for (int i = 0; i < suggestions.length; i++) {
          if (suggestions[i].equals(tokenText)) {
            foundOriginal = true;
            break;
          }
          suggestionsWithOrig[i + 1] = suggestions[i];
        }
        if (!foundOriginal) {
          suggestionsWithOrig[0] = tokenText;
          suggestions = suggestionsWithOrig;
        }
      }

      if (options.extendedResults == true && reader != null && field != null) {
        result.addFrequency(token, docFreq);
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
   * @throws IOException If there is a low-level I/O error.
   */
  protected void initIndex() throws IOException {
    if (indexDir != null) {
      // TODO: this is a workaround for SpellChecker repeatedly closing and opening a new IndexWriter while leaving readers open, which on
      // Windows causes problems because deleted files can't be opened.  It would be better for SpellChecker to hold a single IW instance,
      // and close it on close, but Solr never seems to close its spell checkers.  Wrapping as FilterDirectory prevents IndexWriter from
      // catching the pending deletions:
      index = new FilterDirectory(FSDirectory.open(new File(indexDir).toPath())) {
      };
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
