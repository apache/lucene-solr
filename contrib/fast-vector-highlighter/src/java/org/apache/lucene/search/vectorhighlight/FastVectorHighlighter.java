package org.apache.lucene.search.vectorhighlight;

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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

/**
 * Another highlighter implementation.
 *
 */
public class FastVectorHighlighter {

  public static final boolean DEFAULT_PHRASE_HIGHLIGHT = true;
  public static final boolean DEFAULT_FIELD_MATCH = true;
  private final boolean phraseHighlight;
  private final boolean fieldMatch;
  private final FragListBuilder fragListBuilder;
  private final FragmentsBuilder fragmentsBuilder;

  /**
   * the default constructor.
   */
  public FastVectorHighlighter(){
    this( DEFAULT_PHRASE_HIGHLIGHT, DEFAULT_FIELD_MATCH );
  }

  /**
   * a constructor. Using SimpleFragListBuilder and ScoreOrderFragmentsBuilder.
   * 
   * @param phraseHighlight true or false for phrase highlighting
   * @param fieldMatch true of false for field matching
   */
  public FastVectorHighlighter( boolean phraseHighlight, boolean fieldMatch ){
    this( phraseHighlight, fieldMatch, new SimpleFragListBuilder(), new ScoreOrderFragmentsBuilder() );
  }

  /**
   * a constructor. A FragListBuilder and a FragmentsBuilder can be specified (plugins).
   * 
   * @param phraseHighlight true of false for phrase highlighting
   * @param fieldMatch true of false for field matching
   * @param fragListBuilder an instance of FragListBuilder
   * @param fragmentsBuilder an instance of FragmentsBuilder
   */
  public FastVectorHighlighter( boolean phraseHighlight, boolean fieldMatch,
      FragListBuilder fragListBuilder, FragmentsBuilder fragmentsBuilder ){
    this.phraseHighlight = phraseHighlight;
    this.fieldMatch = fieldMatch;
    this.fragListBuilder = fragListBuilder;
    this.fragmentsBuilder = fragmentsBuilder;
  }

  /**
   * create a FieldQuery object.
   * 
   * @param query a query
   * @return the created FieldQuery object
   */
  public FieldQuery getFieldQuery( Query query ){
    return new FieldQuery( query, phraseHighlight, fieldMatch );
  }

  /**
   * return the best fragment.
   * 
   * @param fieldQuery FieldQuery object
   * @param reader IndexReader of the index
   * @param docId document id to be highlighted
   * @param fieldName field of the document to be highlighted
   * @param fragCharSize the length (number of chars) of a fragment
   * @return the best fragment (snippet) string
   * @throws IOException
   */
  public final String getBestFragment( final FieldQuery fieldQuery, IndexReader reader, int docId,
      String fieldName, int fragCharSize ) throws IOException {
    FieldFragList fieldFragList = getFieldFragList( fieldQuery, reader, docId, fieldName, fragCharSize );
    return fragmentsBuilder.createFragment( reader, docId, fieldName, fieldFragList );
  }

  /**
   * return the best fragments.
   * 
   * @param fieldQuery FieldQuery object
   * @param reader IndexReader of the index
   * @param docId document id to be highlighted
   * @param fieldName field of the document to be highlighted
   * @param fragCharSize the length (number of chars) of a fragment
   * @param maxNumFragments maximum number of fragments
   * @return created fragments or null when no fragments created.
   *         size of the array can be less than maxNumFragments
   * @throws IOException
   */
  public final String[] getBestFragments( final FieldQuery fieldQuery, IndexReader reader, int docId,
      String fieldName, int fragCharSize, int maxNumFragments ) throws IOException {
    FieldFragList fieldFragList = getFieldFragList( fieldQuery, reader, docId, fieldName, fragCharSize );
    return fragmentsBuilder.createFragments( reader, docId, fieldName, fieldFragList, maxNumFragments );
  }
  
  private FieldFragList getFieldFragList( final FieldQuery fieldQuery, IndexReader reader, int docId,
      String fieldName, int fragCharSize ) throws IOException {
    FieldTermStack fieldTermStack = new FieldTermStack( reader, docId, fieldName, fieldQuery );
    FieldPhraseList fieldPhraseList = new FieldPhraseList( fieldTermStack, fieldQuery );
    return fragListBuilder.createFieldFragList( fieldPhraseList, fragCharSize );
  }

  /**
   * return whether phraseHighlight or not.
   * 
   * @return whether phraseHighlight or not
   */
  public boolean isPhraseHighlight(){ return phraseHighlight; }

  /**
   * return whether fieldMatch or not.
   * 
   * @return whether fieldMatch or not
   */
  public boolean isFieldMatch(){ return fieldMatch; }
}
