package org.apache.lucene.document;

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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexWriter;   // for javadoc
import org.apache.lucene.util.Parameter;
import org.apache.lucene.util.StringHelper;

import java.io.Reader;
import java.io.Serializable;

/**
  A field is a section of a Document.  Each field has two parts, a name and a
  value.  Values may be free text, provided as a String or as a Reader, or they
  may be atomic keywords, which are not further processed.  Such keywords may
  be used to represent dates, urls, etc.  Fields are optionally stored in the
  index, so that they may be returned with hits on the document.
  */

public final class Field extends AbstractField implements Fieldable, Serializable {
  
  /** Specifies whether and how a field should be stored. */
  public static final class Store extends Parameter implements Serializable {

    private Store(String name) {
      super(name);
    }

    /** Store the original field value in the index in a compressed form. This is
     * useful for long documents and for binary valued fields.
     * @deprecated Please use {@link CompressionTools} instead.
     * For string fields that were previously indexed and stored using compression,
     * the new way to achieve this is: First add the field indexed-only (no store)
     * and additionally using the same field name as a binary, stored field
     * with {@link CompressionTools#compressString}.
     */
    public static final Store COMPRESS = new Store("COMPRESS");

    /** Store the original field value in the index. This is useful for short texts
     * like a document's title which should be displayed with the results. The
     * value is stored in its original form, i.e. no analyzer is used before it is
     * stored.
     */
    public static final Store YES = new Store("YES");

    /** Do not store the field value in the index. */
    public static final Store NO = new Store("NO");
  }

  /** Specifies whether and how a field should be indexed. */
  public static final class Index extends Parameter implements Serializable {

    private Index(String name) {
      super(name);
    }

    /** Do not index the field value. This field can thus not be searched,
     * but one can still access its contents provided it is
     * {@link Field.Store stored}. */
    public static final Index NO = new Index("NO");

    /** Index the tokens produced by running the field's
     * value through an Analyzer.  This is useful for
     * common text. */
    public static final Index ANALYZED = new Index("ANALYZED");

    /** @deprecated this has been renamed to {@link #ANALYZED} */
    public static final Index TOKENIZED = ANALYZED;

    /** Index the field's value without using an Analyzer, so it can be searched.
     * As no analyzer is used the value will be stored as a single term. This is
     * useful for unique Ids like product numbers.
     */
    public static final Index NOT_ANALYZED = new Index("NOT_ANALYZED");

    /** @deprecated This has been renamed to {@link #NOT_ANALYZED} */
    public static final Index UN_TOKENIZED = NOT_ANALYZED;

    /** Expert: Index the field's value without an Analyzer,
     * and also disable the storing of norms.  Note that you
     * can also separately enable/disable norms by calling
     * {@link Field#setOmitNorms}.  No norms means that
     * index-time field and document boosting and field
     * length normalization are disabled.  The benefit is
     * less memory usage as norms take up one byte of RAM
     * per indexed field for every document in the index,
     * during searching.  Note that once you index a given
     * field <i>with</i> norms enabled, disabling norms will
     * have no effect.  In other words, for this to have the
     * above described effect on a field, all instances of
     * that field must be indexed with NOT_ANALYZED_NO_NORMS
     * from the beginning. */
    public static final Index NOT_ANALYZED_NO_NORMS = new Index("NOT_ANALYZED_NO_NORMS");

    /** @deprecated This has been renamed to
     *  {@link #NOT_ANALYZED_NO_NORMS} */
    public static final Index NO_NORMS = NOT_ANALYZED_NO_NORMS;

    /** Expert: Index the tokens produced by running the
     *  field's value through an Analyzer, and also
     *  separately disable the storing of norms.  See
     *  {@link #NOT_ANALYZED_NO_NORMS} for what norms are
     *  and why you may want to disable them. */
    public static final Index ANALYZED_NO_NORMS = new Index("ANALYZED_NO_NORMS");
  }

  /** Specifies whether and how a field should have term vectors. */
  public static final class TermVector  extends Parameter implements Serializable {
    
    private TermVector(String name) {
      super(name);
    }
    
    /** Do not store term vectors. 
     */
    public static final TermVector NO = new TermVector("NO");
    
    /** Store the term vectors of each document. A term vector is a list
     * of the document's terms and their number of occurrences in that document. */
    public static final TermVector YES = new TermVector("YES");
    
    /**
     * Store the term vector + token position information
     * 
     * @see #YES
     */ 
    public static final TermVector WITH_POSITIONS = new TermVector("WITH_POSITIONS");
    
    /**
     * Store the term vector + Token offset information
     * 
     * @see #YES
     */ 
    public static final TermVector WITH_OFFSETS = new TermVector("WITH_OFFSETS");
    
    /**
     * Store the term vector + Token position and offset information
     * 
     * @see #YES
     * @see #WITH_POSITIONS
     * @see #WITH_OFFSETS
     */ 
    public static final TermVector WITH_POSITIONS_OFFSETS = new TermVector("WITH_POSITIONS_OFFSETS");
  }
  
  
  /** The value of the field as a String, or null.  If null, the Reader value or
   * binary value is used.  Exactly one of stringValue(),
   * readerValue(), and getBinaryValue() must be set. */
  public String stringValue()   { return fieldsData instanceof String ? (String)fieldsData : null; }
  
  /** The value of the field as a Reader, or null.  If null, the String value or
   * binary value is used.  Exactly one of stringValue(),
   * readerValue(), and getBinaryValue() must be set. */
  public Reader readerValue()   { return fieldsData instanceof Reader ? (Reader)fieldsData : null; }
  
  /** The value of the field in Binary, or null.  If null, the Reader value,
   * or String value is used. Exactly one of stringValue(),
   * readerValue(), and getBinaryValue() must be set.
   * @deprecated This method must allocate a new byte[] if
   * the {@link AbstractField#getBinaryOffset()} is non-zero
   * or {@link AbstractField#getBinaryLength()} is not the
   * full length of the byte[]. Please use {@link
   * AbstractField#getBinaryValue()} instead, which simply
   * returns the byte[].
   */ 
  public byte[] binaryValue() {
    if (!isBinary)
      return null;
    final byte[] data = (byte[]) fieldsData;
    if (binaryOffset == 0 && data.length == binaryLength)
      return data; //Optimization
    
    final byte[] ret = new byte[binaryLength];
    System.arraycopy(data, binaryOffset, ret, 0, binaryLength);
    return ret;    
  }
  
  /** The TokesStream for this field to be used when indexing, or null.  If null, the Reader value
   * or String value is analyzed to produce the indexed tokens. */
  public TokenStream tokenStreamValue()   { return tokenStream; }
  

  /** <p>Expert: change the value of this field.  This can
   *  be used during indexing to re-use a single Field
   *  instance to improve indexing speed by avoiding GC cost
   *  of new'ing and reclaiming Field instances.  Typically
   *  a single {@link Document} instance is re-used as
   *  well.  This helps most on small documents.</p>
   * 
   *  <p>Each Field instance should only be used once
   *  within a single {@link Document} instance.  See <a
   *  href="http://wiki.apache.org/lucene-java/ImproveIndexingSpeed">ImproveIndexingSpeed</a>
   *  for details.</p> */
  public void setValue(String value) {
    if (isBinary) {
      throw new IllegalArgumentException("cannot set a String value on a binary field");
    }
    fieldsData = value;
  }

  /** Expert: change the value of this field.  See <a href="#setValue(java.lang.String)">setValue(String)</a>. */
  public void setValue(Reader value) {
    if (isBinary) {
      throw new IllegalArgumentException("cannot set a Reader value on a binary field");
    }
    if (isStored) {
      throw new IllegalArgumentException("cannot set a Reader value on a stored field");
    }
    fieldsData = value;
  }

  /** Expert: change the value of this field.  See <a href="#setValue(java.lang.String)">setValue(String)</a>. */
  public void setValue(byte[] value) {
    if (!isBinary) {
      throw new IllegalArgumentException("cannot set a byte[] value on a non-binary field");
    }
    fieldsData = value;
    binaryLength = value.length;
    binaryOffset = 0;
  }

  /** Expert: change the value of this field.  See <a href="#setValue(java.lang.String)">setValue(String)</a>. */
  public void setValue(byte[] value, int offset, int length) {
    if (!isBinary) {
      throw new IllegalArgumentException("cannot set a byte[] value on a non-binary field");
    }
    fieldsData = value;
    binaryLength = length;
    binaryOffset = offset;
  }
  
  
  /** Expert: change the value of this field.  See <a href="#setValue(java.lang.String)">setValue(String)</a>.
   * @deprecated use {@link #setTokenStream} */
  public void setValue(TokenStream value) {
    if (isBinary) {
      throw new IllegalArgumentException("cannot set a TokenStream value on a binary field");
    }
    if (isStored) {
      throw new IllegalArgumentException("cannot set a TokenStream value on a stored field");
    }
    fieldsData = null;
    tokenStream = value;
  }

  /** Expert: sets the token stream to be used for indexing and causes isIndexed() and isTokenized() to return true.
   *  May be combined with stored values from stringValue() or binaryValue() */
  public void setTokenStream(TokenStream tokenStream) {
    this.isIndexed = true;
    this.isTokenized = true;
    this.tokenStream = tokenStream;
  }

  /**
   * Create a field by specifying its name, value and how it will
   * be saved in the index. Term vectors will not be stored in the index.
   * 
   * @param name The name of the field
   * @param value The string to process
   * @param store Whether <code>value</code> should be stored in the index
   * @param index Whether the field should be indexed, and if so, if it should
   *  be tokenized before indexing 
   * @throws NullPointerException if name or value is <code>null</code>
   * @throws IllegalArgumentException if the field is neither stored nor indexed 
   */
  public Field(String name, String value, Store store, Index index) {
    this(name, value, store, index, TermVector.NO);
  }
  
  /**
   * Create a field by specifying its name, value and how it will
   * be saved in the index.
   * 
   * @param name The name of the field
   * @param value The string to process
   * @param store Whether <code>value</code> should be stored in the index
   * @param index Whether the field should be indexed, and if so, if it should
   *  be tokenized before indexing 
   * @param termVector Whether term vector should be stored
   * @throws NullPointerException if name or value is <code>null</code>
   * @throws IllegalArgumentException in any of the following situations:
   * <ul> 
   *  <li>the field is neither stored nor indexed</li> 
   *  <li>the field is not indexed but termVector is <code>TermVector.YES</code></li>
   * </ul> 
   */ 
  public Field(String name, String value, Store store, Index index, TermVector termVector) {
	  this(name, true, value, store, index, termVector);
  }
  
  /**
   * Create a field by specifying its name, value and how it will
   * be saved in the index.
   * 
   * @param name The name of the field
   * @param internName Whether to .intern() name or not
   * @param value The string to process
   * @param store Whether <code>value</code> should be stored in the index
   * @param index Whether the field should be indexed, and if so, if it should
   *  be tokenized before indexing 
   * @param termVector Whether term vector should be stored
   * @throws NullPointerException if name or value is <code>null</code>
   * @throws IllegalArgumentException in any of the following situations:
   * <ul> 
   *  <li>the field is neither stored nor indexed</li> 
   *  <li>the field is not indexed but termVector is <code>TermVector.YES</code></li>
   * </ul> 
   */ 
  public Field(String name, boolean internName, String value, Store store, Index index, TermVector termVector) {
    if (name == null)
      throw new NullPointerException("name cannot be null");
    if (value == null)
      throw new NullPointerException("value cannot be null");
    if (name.length() == 0 && value.length() == 0)
      throw new IllegalArgumentException("name and value cannot both be empty");
    if (index == Index.NO && store == Store.NO)
      throw new IllegalArgumentException("it doesn't make sense to have a field that "
         + "is neither indexed nor stored");
    if (index == Index.NO && termVector != TermVector.NO)
      throw new IllegalArgumentException("cannot store term vector information "
         + "for a field that is not indexed");
          
    if (internName) // field names are optionally interned
      name = StringHelper.intern(name);
    
    this.name = name; 
    
    this.fieldsData = value;

    if (store == Store.YES){
      this.isStored = true;
      this.isCompressed = false;
    }
    else if (store == Store.COMPRESS) {
      this.isStored = true;
      this.isCompressed = true;
    }
    else if (store == Store.NO){
      this.isStored = false;
      this.isCompressed = false;
    }
    else
      throw new IllegalArgumentException("unknown store parameter " + store);
   
    if (index == Index.NO) {
      this.isIndexed = false;
      this.isTokenized = false;
      this.omitTermFreqAndPositions = false;
      this.omitNorms = true;
    } else if (index == Index.ANALYZED) {
      this.isIndexed = true;
      this.isTokenized = true;
    } else if (index == Index.NOT_ANALYZED) {
      this.isIndexed = true;
      this.isTokenized = false;
    } else if (index == Index.NOT_ANALYZED_NO_NORMS) {
      this.isIndexed = true;
      this.isTokenized = false;
      this.omitNorms = true;
    } else if (index == Index.ANALYZED_NO_NORMS) {
      this.isIndexed = true;
      this.isTokenized = true;
      this.omitNorms = true;
    } else {
      throw new IllegalArgumentException("unknown index parameter " + index);
    }
    
    this.isBinary = false;

    setStoreTermVector(termVector);
  }

  /**
   * Create a tokenized and indexed field that is not stored. Term vectors will
   * not be stored.  The Reader is read only when the Document is added to the index,
   * i.e. you may not close the Reader until {@link IndexWriter#addDocument(Document)}
   * has been called.
   * 
   * @param name The name of the field
   * @param reader The reader with the content
   * @throws NullPointerException if name or reader is <code>null</code>
   */
  public Field(String name, Reader reader) {
    this(name, reader, TermVector.NO);
  }

  /**
   * Create a tokenized and indexed field that is not stored, optionally with 
   * storing term vectors.  The Reader is read only when the Document is added to the index,
   * i.e. you may not close the Reader until {@link IndexWriter#addDocument(Document)}
   * has been called.
   * 
   * @param name The name of the field
   * @param reader The reader with the content
   * @param termVector Whether term vector should be stored
   * @throws NullPointerException if name or reader is <code>null</code>
   */ 
  public Field(String name, Reader reader, TermVector termVector) {
    if (name == null)
      throw new NullPointerException("name cannot be null");
    if (reader == null)
      throw new NullPointerException("reader cannot be null");
    
    this.name = StringHelper.intern(name);        // field names are interned
    this.fieldsData = reader;
    
    this.isStored = false;
    this.isCompressed = false;
    
    this.isIndexed = true;
    this.isTokenized = true;
    
    this.isBinary = false;
    
    setStoreTermVector(termVector);
  }

  /**
   * Create a tokenized and indexed field that is not stored. Term vectors will
   * not be stored. This is useful for pre-analyzed fields.
   * The TokenStream is read only when the Document is added to the index,
   * i.e. you may not close the TokenStream until {@link IndexWriter#addDocument(Document)}
   * has been called.
   * 
   * @param name The name of the field
   * @param tokenStream The TokenStream with the content
   * @throws NullPointerException if name or tokenStream is <code>null</code>
   */ 
  public Field(String name, TokenStream tokenStream) {
    this(name, tokenStream, TermVector.NO);
  }
  
  /**
   * Create a tokenized and indexed field that is not stored, optionally with 
   * storing term vectors.  This is useful for pre-analyzed fields.
   * The TokenStream is read only when the Document is added to the index,
   * i.e. you may not close the TokenStream until {@link IndexWriter#addDocument(Document)}
   * has been called.
   * 
   * @param name The name of the field
   * @param tokenStream The TokenStream with the content
   * @param termVector Whether term vector should be stored
   * @throws NullPointerException if name or tokenStream is <code>null</code>
   */ 
  public Field(String name, TokenStream tokenStream, TermVector termVector) {
    if (name == null)
      throw new NullPointerException("name cannot be null");
    if (tokenStream == null)
      throw new NullPointerException("tokenStream cannot be null");
    
    this.name = StringHelper.intern(name);        // field names are interned
    this.fieldsData = null;
    this.tokenStream = tokenStream;

    this.isStored = false;
    this.isCompressed = false;
    
    this.isIndexed = true;
    this.isTokenized = true;
    
    this.isBinary = false;
    
    setStoreTermVector(termVector);
  }

  
  /**
   * Create a stored field with binary value. Optionally the value may be compressed.
   * 
   * @param name The name of the field
   * @param value The binary value
   * @param store How <code>value</code> should be stored (compressed or not)
   * @throws IllegalArgumentException if store is <code>Store.NO</code> 
   */
  public Field(String name, byte[] value, Store store) {
    this(name, value, 0, value.length, store);
  }

  /**
   * Create a stored field with binary value. Optionally the value may be compressed.
   * 
   * @param name The name of the field
   * @param value The binary value
   * @param offset Starting offset in value where this Field's bytes are
   * @param length Number of bytes to use for this Field, starting at offset
   * @param store How <code>value</code> should be stored (compressed or not)
   * @throws IllegalArgumentException if store is <code>Store.NO</code> 
   */
  public Field(String name, byte[] value, int offset, int length, Store store) {

    if (name == null)
      throw new IllegalArgumentException("name cannot be null");
    if (value == null)
      throw new IllegalArgumentException("value cannot be null");
    
    this.name = StringHelper.intern(name);        // field names are interned
    fieldsData = value;
    
    if (store == Store.YES) {
      isStored = true;
      isCompressed = false;
    }
    else if (store == Store.COMPRESS) {
      isStored = true;
      isCompressed = true;
    }
    else if (store == Store.NO)
      throw new IllegalArgumentException("binary values can't be unstored");
    else
      throw new IllegalArgumentException("unknown store parameter " + store);
    
    isIndexed   = false;
    isTokenized = false;
    omitTermFreqAndPositions = false;
    omitNorms = true;
    
    isBinary    = true;
    binaryLength = length;
    binaryOffset = offset;
    
    setStoreTermVector(TermVector.NO);
  }
}
