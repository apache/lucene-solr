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
package org.apache.lucene.search.vectorhighlight;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Encoder;

/**
 * Another highlighter implementation.
 *
 */
public class FastVectorHighlighter {
  public static final boolean DEFAULT_PHRASE_HIGHLIGHT = true;
  public static final boolean DEFAULT_FIELD_MATCH = true;
  protected final boolean phraseHighlight;
  protected final boolean fieldMatch;
  private final FragListBuilder fragListBuilder;
  private final FragmentsBuilder fragmentsBuilder;
  private int phraseLimit = Integer.MAX_VALUE;

  /**
   * the default constructor.
   */
  public FastVectorHighlighter(){
    this( DEFAULT_PHRASE_HIGHLIGHT, DEFAULT_FIELD_MATCH );
  }

  /**
   * a constructor. Using {@link SimpleFragListBuilder} and {@link ScoreOrderFragmentsBuilder}.
   * 
   * @param phraseHighlight true or false for phrase highlighting
   * @param fieldMatch true of false for field matching
   */
  public FastVectorHighlighter( boolean phraseHighlight, boolean fieldMatch ){
    this( phraseHighlight, fieldMatch, new SimpleFragListBuilder(), new ScoreOrderFragmentsBuilder() );
  }

  /**
   * a constructor. A {@link FragListBuilder} and a {@link FragmentsBuilder} can be specified (plugins).
   * 
   * @param phraseHighlight true of false for phrase highlighting
   * @param fieldMatch true of false for field matching
   * @param fragListBuilder an instance of {@link FragListBuilder}
   * @param fragmentsBuilder an instance of {@link FragmentsBuilder}
   */
  public FastVectorHighlighter( boolean phraseHighlight, boolean fieldMatch,
      FragListBuilder fragListBuilder, FragmentsBuilder fragmentsBuilder ){
    this.phraseHighlight = phraseHighlight;
    this.fieldMatch = fieldMatch;
    this.fragListBuilder = fragListBuilder;
    this.fragmentsBuilder = fragmentsBuilder;
  }

  /**
   * create a {@link FieldQuery} object.
   * 
   * @param query a query
   * @return the created {@link FieldQuery} object
   */
  public FieldQuery getFieldQuery( Query query ) {
    // TODO: should we deprecate this? 
    // because if there is no reader, then we cannot rewrite MTQ.
    try {
      return getFieldQuery(query, null);
    } catch (IOException e) {
      // should never be thrown when reader is null
      throw new RuntimeException (e);
    }
  }
  
  /**
   * create a {@link FieldQuery} object.
   * 
   * @param query a query
   * @return the created {@link FieldQuery} object
   */
  public FieldQuery getFieldQuery( Query query, IndexReader reader ) throws IOException {
    return new FieldQuery( query, reader, phraseHighlight, fieldMatch );
  }

  /**
   * return the best fragment.
   * 
   * @param fieldQuery {@link FieldQuery} object
   * @param reader {@link IndexReader} of the index
   * @param docId document id to be highlighted
   * @param fieldName field of the document to be highlighted
   * @param fragCharSize the length (number of chars) of a fragment
   * @return the best fragment (snippet) string
   * @throws IOException If there is a low-level I/O error
   */
  public final String getBestFragment( final FieldQuery fieldQuery, IndexReader reader, int docId,
      String fieldName, int fragCharSize ) throws IOException {
    FieldFragList fieldFragList =
      getFieldFragList( fragListBuilder, fieldQuery, reader, docId, fieldName, fragCharSize );
    return fragmentsBuilder.createFragment( reader, docId, fieldName, fieldFragList );
  }

  /**
   * return the best fragments.
   * 
   * @param fieldQuery {@link FieldQuery} object
   * @param reader {@link IndexReader} of the index
   * @param docId document id to be highlighted
   * @param fieldName field of the document to be highlighted
   * @param fragCharSize the length (number of chars) of a fragment
   * @param maxNumFragments maximum number of fragments
   * @return created fragments or null when no fragments created.
   *         size of the array can be less than maxNumFragments
   * @throws IOException If there is a low-level I/O error
   */
  public final String[] getBestFragments( final FieldQuery fieldQuery, IndexReader reader, int docId,
      String fieldName, int fragCharSize, int maxNumFragments ) throws IOException {
    FieldFragList fieldFragList =
      getFieldFragList( fragListBuilder, fieldQuery, reader, docId, fieldName, fragCharSize );
    return fragmentsBuilder.createFragments( reader, docId, fieldName, fieldFragList, maxNumFragments );
  }

  /**
   * return the best fragment.
   * 
   * @param fieldQuery {@link FieldQuery} object
   * @param reader {@link IndexReader} of the index
   * @param docId document id to be highlighted
   * @param fieldName field of the document to be highlighted
   * @param fragCharSize the length (number of chars) of a fragment
   * @param fragListBuilder {@link FragListBuilder} object
   * @param fragmentsBuilder {@link FragmentsBuilder} object
   * @param preTags pre-tags to be used to highlight terms
   * @param postTags post-tags to be used to highlight terms
   * @param encoder an encoder that generates encoded text
   * @return the best fragment (snippet) string
   * @throws IOException If there is a low-level I/O error
   */
  public final String getBestFragment( final FieldQuery fieldQuery, IndexReader reader, int docId,
      String fieldName, int fragCharSize,
      FragListBuilder fragListBuilder, FragmentsBuilder fragmentsBuilder,
      String[] preTags, String[] postTags, Encoder encoder ) throws IOException {
    FieldFragList fieldFragList = getFieldFragList( fragListBuilder, fieldQuery, reader, docId, fieldName, fragCharSize );
    return fragmentsBuilder.createFragment( reader, docId, fieldName, fieldFragList, preTags, postTags, encoder );
  }

  /**
   * return the best fragments.
   * 
   * @param fieldQuery {@link FieldQuery} object
   * @param reader {@link IndexReader} of the index
   * @param docId document id to be highlighted
   * @param fieldName field of the document to be highlighted
   * @param fragCharSize the length (number of chars) of a fragment
   * @param maxNumFragments maximum number of fragments
   * @param fragListBuilder {@link FragListBuilder} object
   * @param fragmentsBuilder {@link FragmentsBuilder} object
   * @param preTags pre-tags to be used to highlight terms
   * @param postTags post-tags to be used to highlight terms
   * @param encoder an encoder that generates encoded text
   * @return created fragments or null when no fragments created.
   *         size of the array can be less than maxNumFragments
   * @throws IOException If there is a low-level I/O error
   */
  public final String[] getBestFragments( final FieldQuery fieldQuery, IndexReader reader, int docId,
      String fieldName, int fragCharSize, int maxNumFragments,
      FragListBuilder fragListBuilder, FragmentsBuilder fragmentsBuilder,
      String[] preTags, String[] postTags, Encoder encoder ) throws IOException {
    FieldFragList fieldFragList =
      getFieldFragList( fragListBuilder, fieldQuery, reader, docId, fieldName, fragCharSize );
    return fragmentsBuilder.createFragments( reader, docId, fieldName, fieldFragList, maxNumFragments,
        preTags, postTags, encoder );
  }

  /**
   * Return the best fragments.  Matches are scanned from matchedFields and turned into fragments against
   * storedField.  The highlighting may not make sense if matchedFields has matches with offsets that don't
   * correspond features in storedField.  It will outright throw a {@code StringIndexOutOfBoundsException}
   * if matchedFields produces offsets outside of storedField.  As such it is advisable that all
   * matchedFields share the same source as storedField or are at least a prefix of it.
   * 
   * @param fieldQuery {@link FieldQuery} object
   * @param reader {@link IndexReader} of the index
   * @param docId document id to be highlighted
   * @param storedField field of the document that stores the text
   * @param matchedFields fields of the document to scan for matches
   * @param fragCharSize the length (number of chars) of a fragment
   * @param maxNumFragments maximum number of fragments
   * @param fragListBuilder {@link FragListBuilder} object
   * @param fragmentsBuilder {@link FragmentsBuilder} object
   * @param preTags pre-tags to be used to highlight terms
   * @param postTags post-tags to be used to highlight terms
   * @param encoder an encoder that generates encoded text
   * @return created fragments or null when no fragments created.
   *         size of the array can be less than maxNumFragments
   * @throws IOException If there is a low-level I/O error
   */
  public final String[] getBestFragments( final FieldQuery fieldQuery, IndexReader reader, int docId,
      String storedField, Set< String > matchedFields, int fragCharSize, int maxNumFragments,
      FragListBuilder fragListBuilder, FragmentsBuilder fragmentsBuilder,
      String[] preTags, String[] postTags, Encoder encoder ) throws IOException {
    FieldFragList fieldFragList =
      getFieldFragList( fragListBuilder, fieldQuery, reader, docId, matchedFields, fragCharSize );
    return fragmentsBuilder.createFragments( reader, docId, storedField, fieldFragList, maxNumFragments,
        preTags, postTags, encoder );
  }

  /**
   * Build a FieldFragList for one field.
   */
  private FieldFragList getFieldFragList( FragListBuilder fragListBuilder,
      final FieldQuery fieldQuery, IndexReader reader, int docId,
      String matchedField, int fragCharSize ) throws IOException {
    FieldTermStack fieldTermStack = new FieldTermStack( reader, docId, matchedField, fieldQuery );
    FieldPhraseList fieldPhraseList = new FieldPhraseList( fieldTermStack, fieldQuery, phraseLimit );
    return fragListBuilder.createFieldFragList( fieldPhraseList, fragCharSize );
  }

  /**
   * Build a FieldFragList for more than one field.
   */
  private FieldFragList getFieldFragList( FragListBuilder fragListBuilder,
      final FieldQuery fieldQuery, IndexReader reader, int docId,
      Set< String > matchedFields, int fragCharSize ) throws IOException {
    Iterator< String > matchedFieldsItr = matchedFields.iterator();
    if ( !matchedFieldsItr.hasNext() ) {
      throw new IllegalArgumentException( "matchedFields must contain at least on field name." );
    }
    FieldPhraseList[] toMerge = new FieldPhraseList[ matchedFields.size() ];
    int i = 0;
    while ( matchedFieldsItr.hasNext() ) {
      FieldTermStack stack = new FieldTermStack( reader, docId, matchedFieldsItr.next(), fieldQuery );
      toMerge[ i++ ] = new FieldPhraseList( stack, fieldQuery, phraseLimit );
    } 
    return fragListBuilder.createFieldFragList( new FieldPhraseList( toMerge ), fragCharSize );
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
  
  /**
   * @return the maximum number of phrases to analyze when searching for the highest-scoring phrase.
   */
  public int getPhraseLimit () { return phraseLimit; }
  
  /**
   * set the maximum number of phrases to analyze when searching for the highest-scoring phrase.
   * The default is unlimited (Integer.MAX_VALUE).
   */
  public void setPhraseLimit (int phraseLimit) { this.phraseLimit = phraseLimit; }
}
