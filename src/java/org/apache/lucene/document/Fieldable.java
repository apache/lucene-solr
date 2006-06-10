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

import java.io.Reader;
import java.io.Serializable;

/**
 * Synonymous with {@link Field}.
 *
 **/
public interface Fieldable extends Serializable {
  /** Sets the boost factor hits on this field.  This value will be
   * multiplied into the score of all hits on this this field of this
   * document.
   *
   * <p>The boost is multiplied by {@link org.apache.lucene.document.Document#getBoost()} of the document
   * containing this field.  If a document has multiple fields with the same
   * name, all such values are multiplied together.  This product is then
   * multipled by the value {@link org.apache.lucene.search.Similarity#lengthNorm(String,int)}, and
   * rounded by {@link org.apache.lucene.search.Similarity#encodeNorm(float)} before it is stored in the
   * index.  One should attempt to ensure that this product does not overflow
   * the range of that encoding.
   *
   * @see org.apache.lucene.document.Document#setBoost(float)
   * @see org.apache.lucene.search.Similarity#lengthNorm(String, int)
   * @see org.apache.lucene.search.Similarity#encodeNorm(float)
   */
  void setBoost(float boost);

  /** Returns the boost factor for hits for this field.
   *
   * <p>The default value is 1.0.
   *
   * <p>Note: this value is not stored directly with the document in the index.
   * Documents returned from {@link org.apache.lucene.index.IndexReader#document(int)} and
   * {@link org.apache.lucene.search.Hits#doc(int)} may thus not have the same value present as when
   * this field was indexed.
   *
   * @see #setBoost(float)
   */
  float getBoost();

  /** Returns the name of the field as an interned string.
   * For example "date", "title", "body", ...
   */
  String name();

  /** The value of the field as a String, or null.  If null, the Reader value
   * or binary value is used.  Exactly one of stringValue(), readerValue(), and
   * binaryValue() must be set. */
  String stringValue();

  /** The value of the field as a Reader, or null.  If null, the String value
   * or binary value is  used.  Exactly one of stringValue(), readerValue(),
   * and binaryValue() must be set. */
  Reader readerValue();

  /** The value of the field in Binary, or null.  If null, the Reader or
   * String value is used.  Exactly one of stringValue(), readerValue() and
   * binaryValue() must be set. */
  byte[] binaryValue();

  /** True iff the value of the field is to be stored in the index for return
    with search hits.  It is an error for this to be true if a field is
    Reader-valued. */
  boolean  isStored();

  /** True iff the value of the field is to be indexed, so that it may be
    searched on. */
  boolean  isIndexed();

  /** True iff the value of the field should be tokenized as text prior to
    indexing.  Un-tokenized fields are indexed as a single word and may not be
    Reader-valued. */
  boolean  isTokenized();

  /** True if the value of the field is stored and compressed within the index */
  boolean  isCompressed();

  /** True iff the term or terms used to index this field are stored as a term
   *  vector, available from {@link org.apache.lucene.index.IndexReader#getTermFreqVector(int,String)}.
   *  These methods do not provide access to the original content of the field,
   *  only to terms used to index it. If the original content must be
   *  preserved, use the <code>stored</code> attribute instead.
   *
   * @see org.apache.lucene.index.IndexReader#getTermFreqVector(int, String)
   */
  boolean isTermVectorStored();

  /**
   * True iff terms are stored as term vector together with their offsets 
   * (start and end positon in source text).
   */
  boolean isStoreOffsetWithTermVector();

  /**
   * True iff terms are stored as term vector together with their token positions.
   */
  boolean isStorePositionWithTermVector();

  /** True iff the value of the filed is stored as binary */
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
   * it's values via {@link #stringValue()} or {@link #binaryValue()} is only valid as long as the {@link org.apache.lucene.index.IndexReader} that
   * retrieved the {@link Document} is still open.
   *  
   * @return true if this field can be loaded lazily
   */
  boolean isLazy();
}
