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
  
  public static final class Store {
    private String name;
    private Store() {}
    private Store(String name) {
      this.name = name;
    }
    public String toString() {
      return name;
    }
    /** Store the original field value in the index. This is useful for short texts
     * like a document's title which should be displayed with the results. The
     * value is stored in its original form, i.e. no analyzer is used before it is
     * stored. 
     */
    public static final Store YES = new Store("YES");
    /** Do not store the field value in the index. */
    public static final Store NO = new Store("NO");
  }
  
  public static final class Index {
    private String name;
    private Index() {}
    private Index(String name) {
      this.name = name;
    }
    public String toString() {
      return name;
    }
    /** Do not index the field value. This field can thus not be searched,
     * but one can still access its contents provided it is 
     * {@link Field.Store stored}. */
    public static final Index NO = new Index("NO");
    /** Index the field's value so it can be searched. An Analyzer will be used
     * to tokenize and possibly further normalize the text before its
     * terms will be stored in the index. This is useful for common text.
     */
    public static final Index TOKENIZED = new Index("TOKENIZED");
    /** Index the field's value without using an Analyzer, so it can be searched.
     * As no analyzer is used the value will be stored as a single term. This is 
     * useful for unique Ids like product numbers.
     */
    public static final Index UN_TOKENIZED = new Index("UN_TOKENIZED");
  }

  public static final class TermVector {
    private String name;
    private TermVector() {}
    private TermVector(String name) {
      this.name = name;
    }
    public String toString() {
      return name;
    }
    /** Do not store term vectors. 
     */
    public static final TermVector NO = new TermVector("NO");
    /** Store the term vectors of each document. A term vector is a list
     * of the document's terms and their number of occurences in that document. */
    public static final TermVector YES = new TermVector("YES");
  }

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

  /** Returns the boost factor for hits for this field.
   *
   * <p>The default value is 1.0.
   *
   * <p>Note: this value is not stored directly with the document in the index.
   * Documents returned from {@link IndexReader#document(int)} and
   * {@link Hits#doc(int)} may thus not have the same value present as when
   * this field was indexed.
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
    fields, like "title" or "subject". Term vector will not be stored for this field.
  @deprecated use {@link #Field(String, String, Field.Store, Field.Index)
    Field(name, value, Field.Store.YES, Field.Index.TOKENIZED)} instead */
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
    fields, like "title" or "subject".
    @deprecated use {@link #Field(String, String, Field.Store, Field.Index, Field.TermVector)
      Field(name, value, Field.Store.YES, Field.Index.TOKENIZED, boolean)} instead */
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


  /** Create a field by specifying all parameters except for <code>termVector</code>,
   *  which is set to <code>TermVector.NO</code>.
   */
  public Field(String name, String string, Store store, Index index) {
    this(name, string, store, index, TermVector.NO);
  }

  /**
   * Create a field by specifying its name, value and how it will
   * be saved in the index.
   * 
   * @param name The name of the field
   * @param value The string to process
   * @param store whether <code>value</code> should be stored in the index
   * @param index whether the field should be indexed, and if so, if it should
   *  be tokenized before indexing 
   * @param termVector whether Term Vector info should be stored
   * @throws IllegalArgumentException in any of the following situations:
   * <ul> 
   *  <li>the field is neither stored nor indexed</li> 
   *  <li>the field is not indexed but termVector is <code>TermVector.YES</code></li>
   * </ul> 
   */ 
  public Field(String name, String value, Store store, Index index, TermVector termVector) {
      if (name == null)
         throw new NullPointerException("name cannot be null");
      if (value == null)
        throw new NullPointerException("value cannot be null");
      if (index == Index.NO && store == Store.NO)
        throw new IllegalArgumentException("it doesn't make sense to have a field that "
            + "is neither indexed nor stored");
      if (index == Index.NO && termVector != TermVector.NO)
        throw new IllegalArgumentException("cannot store term vector information "
            + "for a field that is not indexed");

      this.name = name.intern();        // field names are interned
      this.stringValue = value;
      if (store == Store.YES)
        this.isStored = true;
      else if (store == Store.NO)
        this.isStored = false;
      else
        throw new IllegalArgumentException("unknown store parameter " + store);
      
      if (index == Index.NO) {
        this.isIndexed = false;
        this.isTokenized = false;
      } else if (index == Index.TOKENIZED) {
        this.isIndexed = true;
        this.isTokenized = true;
      } else if (index == Index.UN_TOKENIZED) {
        this.isIndexed = true;
        this.isTokenized = false;
      } else {
        throw new IllegalArgumentException("unknown index parameter " + index);
      }

      if (termVector == TermVector.NO) {
        this.storeTermVector = false;
      } else if (termVector == TermVector.YES) {
        this.storeTermVector = true;
      } else {
        throw new IllegalArgumentException("unknown termVector parameter " + termVector);
      }
}

  /** Create a field by specifying all parameters except for <code>storeTermVector</code>,
   *  which is set to <code>false</code>.
   * @deprecated use {@link #Field(String, String, Field.Store, Field.Index)} instead
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
   * @deprecated use {@link #Field(String, String, Field.Store, Field.Index, Field.TermVector)} instead
   */ 
  public Field(String name, String string,
	       boolean store, boolean index, boolean token, boolean storeTermVector) {
    if (name == null)
      throw new NullPointerException("name cannot be null");
    if (string == null)
      throw new NullPointerException("value cannot be null");
    if (!index && storeTermVector)
      throw new IllegalArgumentException("cannot store a term vector for fields that are not indexed");

    this.name = name.intern();			  // field names are interned
    this.stringValue = string;
    this.isStored = store;
    this.isIndexed = index;
    this.isTokenized = token;
    this.storeTermVector = storeTermVector;
  }

  Field(String name, Reader reader) {
    if (name == null)
      throw new NullPointerException("name cannot be null");
    if (reader == null)
      throw new NullPointerException("value cannot be null");

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
