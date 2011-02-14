package org.apache.lucene.document;
/**
 * Copyright 2006 The Apache Software Foundation
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

import org.apache.lucene.search.PhraseQuery; // for javadocs
import org.apache.lucene.search.spans.SpanQuery; // for javadocs
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.util.StringHelper; // for javadocs


/**
 *
 *
 **/
public abstract class AbstractField implements Fieldable {

  protected String name = "body";
  protected boolean storeTermVector = false;
  protected boolean storeOffsetWithTermVector = false;
  protected boolean storePositionWithTermVector = false;
  protected boolean omitNorms = false;
  protected boolean isStored = false;
  protected boolean isIndexed = true;
  protected boolean isTokenized = true;
  protected boolean isBinary = false;
  protected boolean lazy = false;
  protected boolean omitTermFreqAndPositions = false;
  protected float boost = 1.0f;
  // the data object for all different kind of field values
  protected Object fieldsData = null;
  // pre-analyzed tokenStream for indexed fields
  protected TokenStream tokenStream;
  // length/offset for all primitive types
  protected int binaryLength;
  protected int binaryOffset;

  protected AbstractField()
  {
  }

  protected AbstractField(String name, Field.Store store, Field.Index index, Field.TermVector termVector) {
    if (name == null)
      throw new NullPointerException("name cannot be null");
    this.name = StringHelper.intern(name);        // field names are interned

    this.isStored = store.isStored();
    this.isIndexed = index.isIndexed();
    this.isTokenized = index.isAnalyzed();
    this.omitNorms = index.omitNorms();

    this.isBinary = false;

    setStoreTermVector(termVector);
  }

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
   * by the length normalization factor and then
   * rounded by {@link org.apache.lucene.search.Similarity#encodeNormValue(float)} before it is stored in the
   * index.  One should attempt to ensure that this product does not overflow
   * the range of that encoding.
   *
   * @see org.apache.lucene.document.Document#setBoost(float)
   * @see org.apache.lucene.search.Similarity#computeNorm(FieldInvertState)
   * @see org.apache.lucene.search.Similarity#encodeNormValue(float)
   */
  public void setBoost(float boost) {
    this.boost = boost;
  }

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
  public float getBoost() {
    return boost;
  }

  /** Returns the name of the field as an interned string.
   * For example "date", "title", "body", ...
   */
  public String name()    { return name; }

  protected void setStoreTermVector(Field.TermVector termVector) {
    this.storeTermVector = termVector.isStored();
    this.storePositionWithTermVector = termVector.withPositions();
    this.storeOffsetWithTermVector = termVector.withOffsets();
  }

  /** True iff the value of the field is to be stored in the index for return
    with search hits.  It is an error for this to be true if a field is
    Reader-valued. */
  public final boolean  isStored()  { return isStored; }

  /** True iff the value of the field is to be indexed, so that it may be
    searched on. */
  public final boolean  isIndexed()   { return isIndexed; }

  /** True iff the value of the field should be tokenized as text prior to
    indexing.  Un-tokenized fields are indexed as a single word and may not be
    Reader-valued. */
  public final boolean  isTokenized()   { return isTokenized; }

  /** True iff the term or terms used to index this field are stored as a term
   *  vector, available from {@link org.apache.lucene.index.IndexReader#getTermFreqVector(int,String)}.
   *  These methods do not provide access to the original content of the field,
   *  only to terms used to index it. If the original content must be
   *  preserved, use the <code>stored</code> attribute instead.
   *
   * @see org.apache.lucene.index.IndexReader#getTermFreqVector(int, String)
   */
  public final boolean isTermVectorStored() { return storeTermVector; }

  /**
   * True iff terms are stored as term vector together with their offsets 
   * (start and end position in source text).
   */
  public boolean isStoreOffsetWithTermVector(){
    return storeOffsetWithTermVector;
  }

  /**
   * True iff terms are stored as term vector together with their token positions.
   */
  public boolean isStorePositionWithTermVector(){
    return storePositionWithTermVector;
  }

  /** True iff the value of the filed is stored as binary */
  public final boolean  isBinary() {
    return isBinary;
  }


  /**
   * Return the raw byte[] for the binary field.  Note that
   * you must also call {@link #getBinaryLength} and {@link
   * #getBinaryOffset} to know which range of bytes in this
   * returned array belong to the field.
   * @return reference to the Field value as byte[].
   */
  public byte[] getBinaryValue() {
    return getBinaryValue(null);
  }
  
  public byte[] getBinaryValue(byte[] result){
    if (isBinary || fieldsData instanceof byte[])
      return (byte[]) fieldsData;
    else
      return null;
  }

  /**
   * Returns length of byte[] segment that is used as value, if Field is not binary
   * returned value is undefined
   * @return length of byte[] segment that represents this Field value
   */
  public int getBinaryLength() {
    if (isBinary) {
      return binaryLength;
    } else if (fieldsData instanceof byte[])
      return ((byte[]) fieldsData).length;
    else
      return 0;
  }

  /**
   * Returns offset into byte[] segment that is used as value, if Field is not binary
   * returned value is undefined
   * @return index of the first character in byte[] segment that represents this Field value
   */
  public int getBinaryOffset() {
    return binaryOffset;
  }

  /** True if norms are omitted for this indexed field */
  public boolean getOmitNorms() { return omitNorms; }

  /** @see #setOmitTermFreqAndPositions */
  public boolean getOmitTermFreqAndPositions() { return omitTermFreqAndPositions; }
  
  /** Expert:
   *
   * If set, omit normalization factors associated with this indexed field.
   * This effectively disables indexing boosts and length normalization for this field.
   */
  public void setOmitNorms(boolean omitNorms) { this.omitNorms=omitNorms; }

  /** Expert:
   *
   * If set, omit term freq, positions and payloads from
   * postings for this field.
   *
   * <p><b>NOTE</b>: While this option reduces storage space
   * required in the index, it also means any query
   * requiring positional information, such as {@link
   * PhraseQuery} or {@link SpanQuery} subclasses will
   * silently fail to find results.
   */
  public void setOmitTermFreqAndPositions(boolean omitTermFreqAndPositions) { this.omitTermFreqAndPositions=omitTermFreqAndPositions; }
 
  public boolean isLazy() {
    return lazy;
  }

  /** Prints a Field for human consumption. */
  @Override
  public final String toString() {
    StringBuilder result = new StringBuilder();
    if (isStored) {
      result.append("stored");
    }
    if (isIndexed) {
      if (result.length() > 0)
        result.append(",");
      result.append("indexed");
    }
    if (isTokenized) {
      if (result.length() > 0)
        result.append(",");
      result.append("tokenized");
    }
    if (storeTermVector) {
      if (result.length() > 0)
        result.append(",");
      result.append("termVector");
    }
    if (storeOffsetWithTermVector) {
      if (result.length() > 0)
        result.append(",");
      result.append("termVectorOffsets");
    }
    if (storePositionWithTermVector) {
      if (result.length() > 0)
        result.append(",");
      result.append("termVectorPosition");
    }
    if (isBinary) {
      if (result.length() > 0)
        result.append(",");
      result.append("binary");
    }
    if (omitNorms) {
      result.append(",omitNorms");
    }
    if (omitTermFreqAndPositions) {
      result.append(",omitTermFreqAndPositions");
    }
    if (lazy){
      result.append(",lazy");
    }
    result.append('<');
    result.append(name);
    result.append(':');

    if (fieldsData != null && lazy == false) {
      result.append(fieldsData);
    }

    result.append('>');
    return result.toString();
  }
}
