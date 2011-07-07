package org.apache.lucene.document;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.FieldInvertState; // for javadocs
import org.apache.lucene.index.values.IndexDocValues;
import org.apache.lucene.index.values.PerDocFieldValues;
import org.apache.lucene.index.values.ValueType;
import org.apache.lucene.search.PhraseQuery; // for javadocs
import org.apache.lucene.search.spans.SpanQuery; // for javadocs

import java.io.Reader;

/**
 * Synonymous with {@link Field}.
 *
 * <p><bold>WARNING</bold>: This interface may change within minor versions, despite Lucene's backward compatibility requirements.
 * This means new methods may be added from version to version.  This change only affects the Fieldable API; other backwards
 * compatibility promises remain intact. For example, Lucene can still
 * read and write indices created within the same major version.
 * </p>
 *
 **/
public interface Fieldable {
  /** Sets the boost factor hits on this field.  This value will be
   * multiplied into the score of all hits on this this field of this
   * document.
   *
   * <p>The boost is multiplied by {@link org.apache.lucene.document.Document#getBoost()} of the document
   * containing this field.  If a document has multiple fields with the same
   * name, all such values are multiplied together.  This product is then
   * used to compute the norm factor for the field.  By
   * default, in the {@link
   * org.apache.lucene.search.Similarity#computeNorm(FieldInvertState)} method, the boost value is multiplied
   * by the length normalization factor
   * and then rounded by {@link org.apache.lucene.search.DefaultSimilarity#encodeNormValue(float)} before it is stored in the
   * index.  One should attempt to ensure that this product does not overflow
   * the range of that encoding.
   *
   * @see org.apache.lucene.document.Document#setBoost(float)
   * @see org.apache.lucene.search.Similarity#computeNorm(FieldInvertState)
   * @see org.apache.lucene.search.DefaultSimilarity#encodeNormValue(float)
   */
  void setBoost(float boost);

  /** Returns the boost factor for hits for this field.
   *
   * <p>The default value is 1.0.
   *
   * <p>Note: this value is not stored directly with the document in the index.
   * Documents returned from {@link org.apache.lucene.index.IndexReader#document(int)} and
   * {@link org.apache.lucene.search.IndexSearcher#doc(int)} may thus not have the same value present as when
   * this field was indexed.
   *
   * @see #setBoost(float)
   */
  float getBoost();

  /** Returns the name of the field.
   * For example "date", "title", "body", ...
   */
  String name();

  /** The value of the field as a String, or null.
   * <p>
   * For indexing, if isStored()==true, the stringValue() will be used as the stored field value
   * unless isBinary()==true, in which case getBinaryValue() will be used.
   *
   * If isIndexed()==true and isTokenized()==false, this String value will be indexed as a single token.
   * If isIndexed()==true and isTokenized()==true, then tokenStreamValue() will be used to generate indexed tokens if not null,
   * else readerValue() will be used to generate indexed tokens if not null, else stringValue() will be used to generate tokens.
   */
  public String stringValue();
  
  /** The value of the field as a Reader, which can be used at index time to generate indexed tokens.
   * @see #stringValue()
   */
  public Reader readerValue();
  
  /** The TokenStream for this field to be used when indexing, or null.
   * @see #stringValue()
   */
  public TokenStream tokenStreamValue();

  /** True if the value of the field is to be stored in the index for return
    with search hits. */
  boolean  isStored();

  /** True if the value of the field is to be indexed, so that it may be
    searched on. */
  boolean  isIndexed();

  /** True if the value of the field should be tokenized as text prior to
    indexing.  Un-tokenized fields are indexed as a single word and may not be
    Reader-valued. */
  boolean  isTokenized();

  /** True if the term or terms used to index this field are stored as a term
   *  vector, available from {@link org.apache.lucene.index.IndexReader#getTermFreqVector(int,String)}.
   *  These methods do not provide access to the original content of the field,
   *  only to terms used to index it. If the original content must be
   *  preserved, use the <code>stored</code> attribute instead.
   *
   * @see org.apache.lucene.index.IndexReader#getTermFreqVector(int, String)
   */
  boolean isTermVectorStored();

  /**
   * True if terms are stored as term vector together with their offsets 
   * (start and end positon in source text).
   */
  boolean isStoreOffsetWithTermVector();

  /**
   * True if terms are stored as term vector together with their token positions.
   */
  boolean isStorePositionWithTermVector();

  /** True if the value of the field is stored as binary */
  boolean  isBinary();

  /** True if norms are omitted for this indexed field */
  boolean getOmitNorms();

  /** Expert:
   *
   * If set, omit normalization factors associated with this indexed field.
   * This effectively disables indexing boosts and length normalization for this field.
   */
  void setOmitNorms(boolean omitNorms);

  /**
   * Indicates whether a Field is Lazy or not.  The semantics of Lazy loading are such that if a Field is lazily loaded, retrieving
   * it's values via {@link #stringValue()} or {@link #getBinaryValue()} is only valid as long as the {@link org.apache.lucene.index.IndexReader} that
   * retrieved the {@link Document} is still open.
   *  
   * @return true if this field can be loaded lazily
   */
  boolean isLazy();
  
  /**
   * Returns offset into byte[] segment that is used as value, if Field is not binary
   * returned value is undefined
   * @return index of the first character in byte[] segment that represents this Field value
   */
  abstract int getBinaryOffset();
  
  /**
   * Returns length of byte[] segment that is used as value, if Field is not binary
   * returned value is undefined
   * @return length of byte[] segment that represents this Field value
   */
  abstract int getBinaryLength();

  /**
   * Return the raw byte[] for the binary field.  Note that
   * you must also call {@link #getBinaryLength} and {@link
   * #getBinaryOffset} to know which range of bytes in this
   * returned array belong to the field.
   * @return reference to the Field value as byte[].
   */
  abstract byte[] getBinaryValue();

  /**
   * Return the raw byte[] for the binary field.  Note that
   * you must also call {@link #getBinaryLength} and {@link
   * #getBinaryOffset} to know which range of bytes in this
   * returned array belong to the field.<p>
   * About reuse: if you pass in the result byte[] and it is
   * used, likely the underlying implementation will hold
   * onto this byte[] and return it in future calls to
   * {@link #getBinaryValue()}.
   * So if you subsequently re-use the same byte[] elsewhere
   * it will alter this Fieldable's value.
   * @param result  User defined buffer that will be used if
   *  possible.  If this is null or not large enough, a new
   *  buffer is allocated
   * @return reference to the Field value as byte[].
   */
  abstract byte[] getBinaryValue(byte[] result);
  
  /** @see #setOmitTermFreqAndPositions */
  boolean getOmitTermFreqAndPositions();
  
  /** Expert:
  *
  * If set, omit term freq, positions and payloads from
  * postings for this field.
  *
  * <p><b>NOTE</b>: While this option reduces storage space
  * required in the index, it also means any query
  * requiring positional information, such as {@link
  * PhraseQuery} or {@link SpanQuery} subclasses will
  * fail with an exception.
  */
  void setOmitTermFreqAndPositions(boolean omitTermFreqAndPositions);
  
  /**
   * Returns the {@link PerDocFieldValues}
   */
  public PerDocFieldValues getDocValues();

  /**
   * Sets the {@link PerDocFieldValues} for this field. If
   * {@link PerDocFieldValues} is set this field will store per-document values
   * 
   * @see IndexDocValues
   */
  public void setDocValues(PerDocFieldValues docValues);

  /**
   * Returns <code>true</code> iff {@link PerDocFieldValues} are set on this
   * field.
   */
  public boolean hasDocValues();

  /**
   * Returns the {@link ValueType} of the set {@link PerDocFieldValues} or
   * <code>null</code> if not set.
   */
  public ValueType docValuesType();
}
