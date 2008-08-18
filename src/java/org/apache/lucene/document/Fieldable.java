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

import java.io.Reader;
import java.io.Serializable;

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

  /** The value of the field as a String, or null.  If null, the Reader value,
   * binary value, or TokenStream value is used.  Exactly one of stringValue(), 
   * readerValue(), binaryValue(), and tokenStreamValue() must be set. */
  public String stringValue();
  
  /** The value of the field as a Reader, or null.  If null, the String value,
   * binary value, or TokenStream value is used.  Exactly one of stringValue(), 
   * readerValue(), binaryValue(), and tokenStreamValue() must be set. */
  public Reader readerValue();
  
  /** The value of the field in Binary, or null.  If null, the Reader value,
   * String value, or TokenStream value is used. Exactly one of stringValue(), 
   * readerValue(), binaryValue(), and tokenStreamValue() must be set. */
  public byte[] binaryValue();
  
  /** The value of the field as a TokenStream, or null.  If null, the Reader value,
   * String value, or binary value is used. Exactly one of stringValue(), 
   * readerValue(), binaryValue(), and tokenStreamValue() must be set. */
  public TokenStream tokenStreamValue();

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

  /** Expert:
   *
   * If set, omit term freq, positions and payloads from postings for this field.
   */
  void setOmitTf(boolean omitTf);
  
  /** True if tf is omitted for this indexed field */
  boolean getOmitTf();

  /**
   * Indicates whether a Field is Lazy or not.  The semantics of Lazy loading are such that if a Field is lazily loaded, retrieving
   * it's values via {@link #stringValue()} or {@link #binaryValue()} is only valid as long as the {@link org.apache.lucene.index.IndexReader} that
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
   * {@link #binaryValue()} or {@link #getBinaryValue()}.
   * So if you subsequently re-use the same byte[] elsewhere
   * it will alter this Fieldable's value.
   * @param result  User defined buffer that will be used if
   *  possible.  If this is null or not large enough, a new
   *  buffer is allocated
   * @return reference to the Field value as byte[].
   */
  abstract byte[] getBinaryValue(byte[] result);
}
