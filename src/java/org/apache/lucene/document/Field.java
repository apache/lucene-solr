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
import java.util.Date;
import org.apache.lucene.index.IndexReader;       // for javadoc
import org.apache.lucene.search.Similarity;       // for javadoc
import org.apache.lucene.search.Hits;             // for javadoc

/**
  A field is a section of a Document.  Each field has two parts, a name and a
  value.  Values may be free text, provided as a String or as a Reader, or they
  may be atomic keywords, which are not further processed.  Such keywords may
  be used to represent dates, urls, etc.  Fields are optionally stored in the
  index, so that they may be returned with hits on the document.
  */

public final class Field implements java.io.Serializable {
  private String name = "body";
  private String stringValue = null;
  private boolean storeTermVector = false;
  private Reader readerValue = null;
  private boolean isStored = false;
  private boolean isIndexed = true;
  private boolean isTokenized = true;

  private float boost = 1.0f;

  /** Sets the boost factor hits on this field.  This value will be
   * multiplied into the score of all hits on this this field of this
   * document.
   *
   * <p>The boost is multiplied by {@link Document#getBoost()} of the document
   * containing this field.  If a document has multiple fields with the same
   * name, all such values are multiplied together.  This product is then
   * multipled by the value {@link Similarity#lengthNorm(String,int)}, and
   * rounded by {@link Similarity#encodeNorm(float)} before it is stored in the
   * index.  One should attempt to ensure that this product does not overflow
   * the range of that encoding.
   *
   * @see Document#setBoost(float)
   * @see Similarity#lengthNorm(String, int)
   * @see Similarity#encodeNorm(float)
   */
  public void setBoost(float boost) {
    this.boost = boost;
  }

  /** Returns the boost factor for hits on any field of this document.
   *
   * <p>The default value is 1.0.
   *
   * <p>Note: this value is not stored directly with the document in the index.
   * Documents returned from {@link IndexReader#document(int)} and {@link
   * Hits#doc(int)} may thus not have the same value present as when this field
   * was indexed.
   *
   * @see #setBoost(float)
   */
  public float getBoost() {
    return boost;
  }

  /** Constructs a String-valued Field that is not tokenized, but is indexed
    and stored.  Useful for non-text fields, e.g. date or url.  
   */
  public static final Field Keyword(String name, String value) {
    return new Field(name, value, true, true, false);
  }

  /** Constructs a String-valued Field that is not tokenized nor indexed,
    but is stored in the index, for return with hits. */
  public static final Field UnIndexed(String name, String value) {
    return new Field(name, value, true, false, false);
  }

  /** Constructs a String-valued Field that is tokenized and indexed,
    and is stored in the index, for return with hits.  Useful for short text
    fields, like "title" or "subject". Term vector will not be stored for this field. */
  public static final Field Text(String name, String value) {
    return Text(name, value, false);
  }

  /** Constructs a Date-valued Field that is not tokenized and is indexed,
      and stored in the index, for return with hits. */
  public static final Field Keyword(String name, Date value) {
    return new Field(name, DateField.dateToString(value), true, true, false);
  }

  /** Constructs a String-valued Field that is tokenized and indexed,
    and is stored in the index, for return with hits.  Useful for short text
    fields, like "title" or "subject". */
  public static final Field Text(String name, String value, boolean storeTermVector) {
    return new Field(name, value, true, true, true, storeTermVector);
  }

  /** Constructs a String-valued Field that is tokenized and indexed,
    but that is not stored in the index.  Term vector will not be stored for this field. */
  public static final Field UnStored(String name, String value) {
    return UnStored(name, value, false);
  }

  /** Constructs a String-valued Field that is tokenized and indexed,
    but that is not stored in the index. */
  public static final Field UnStored(String name, String value, boolean storeTermVector) {
    return new Field(name, value, false, true, true, storeTermVector);
  }

  /** Constructs a Reader-valued Field that is tokenized and indexed, but is
    not stored in the index verbatim.  Useful for longer text fields, like
    "body". Term vector will not be stored for this field. */
  public static final Field Text(String name, Reader value) {
    return Text(name, value, false);
  }

  /** Constructs a Reader-valued Field that is tokenized and indexed, but is
    not stored in the index verbatim.  Useful for longer text fields, like
    "body". */
  public static final Field Text(String name, Reader value, boolean storeTermVector) {
    Field f = new Field(name, value);
    f.storeTermVector = storeTermVector;
    return f;
  }

  /** The name of the field (e.g., "date", "subject", "title", or "body")
    as an interned string. */
  public String name() 		{ return name; }

  /** The value of the field as a String, or null.  If null, the Reader value
    is used.  Exactly one of stringValue() and readerValue() must be set. */
  public String stringValue()		{ return stringValue; }
  /** The value of the field as a Reader, or null.  If null, the String value
    is used.  Exactly one of stringValue() and readerValue() must be set. */
  public Reader readerValue()	{ return readerValue; }


  /** Create a field by specifying all parameters except for <code>storeTermVector</code>,
   *  which is set to <code>false</code>.
   */
  public Field(String name, String string,
	       boolean store, boolean index, boolean token) {
    this(name, string, store, index, token, false);
  }

  /**
   * 
   * @param name The name of the field
   * @param string The string to process
   * @param store true if the field should store the string
   * @param index true if the field should be indexed
   * @param token true if the field should be tokenized
   * @param storeTermVector true if we should store the Term Vector info
   */ 
  public Field(String name, String string,
	       boolean store, boolean index, boolean token, boolean storeTermVector) {
    if (name == null)
      throw new IllegalArgumentException("name cannot be null");
    if (string == null)
      throw new IllegalArgumentException("value cannot be null");
    if (!index && storeTermVector)
      throw new IllegalArgumentException("cannot store a term vector for fields that are not indexed.");

    this.name = name.intern();			  // field names are interned
    this.stringValue = string;
    this.isStored = store;
    this.isIndexed = index;
    this.isTokenized = token;
    this.storeTermVector = storeTermVector;
  }

  Field(String name, Reader reader) {
    if (name == null)
      throw new IllegalArgumentException("name cannot be null");
    if (reader == null)
      throw new IllegalArgumentException("value cannot be null");

    this.name = name.intern();			  // field names are interned
    this.readerValue = reader;
  }

  /** True iff the value of the field is to be stored in the index for return
    with search hits.  It is an error for this to be true if a field is
    Reader-valued. */
  public final boolean	isStored() 	{ return isStored; }

  /** True iff the value of the field is to be indexed, so that it may be
    searched on. */
  public final boolean 	isIndexed() 	{ return isIndexed; }

  /** True iff the value of the field should be tokenized as text prior to
    indexing.  Un-tokenized fields are indexed as a single word and may not be
    Reader-valued. */
  public final boolean 	isTokenized() 	{ return isTokenized; }

  /** True iff the term or terms used to index this field are stored as a term
   *  vector, available from {@link IndexReader#getTermFreqVector(int,String)}.
   *  These methods do not provide access to the original content of the field,
   *  only to terms used to index it. If the original content must be
   *  preserved, use the <code>stored</code> attribute instead.
   *
   * @see IndexReader#getTermFreqVector(int, String)
   */
  public final boolean isTermVectorStored() { return storeTermVector; }

  /** Prints a Field for human consumption. */
  public final String toString() {
	StringBuffer result = new StringBuffer();
	if (isStored)
	  result.append("stored");
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
	result.append('<');
	result.append(name);
	result.append(':');
	if (readerValue != null) {
	  result.append(readerValue.toString());
	} else {
	  result.append(stringValue);
	}
	result.append('>');
	return result.toString();
  }

}
