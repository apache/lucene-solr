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

import java.io.Reader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriter;

/**
  A field is a section of a Document.  Each field has two parts, a name and a
  value.  Values may be free text, provided as a String or as a Reader, or they
  may be atomic keywords, which are not further processed.  Such keywords may
  be used to represent dates, urls, etc.  Fields are optionally stored in the
  index, so that they may be returned with hits on the document.
  */

public final class Field extends AbstractField implements Fieldable {
  
  /** Specifies whether and how a field should be stored. */
  public static enum Store {

    /** Store the original field value in the index. This is useful for short texts
     * like a document's title which should be displayed with the results. The
     * value is stored in its original form, i.e. no analyzer is used before it is
     * stored.
     */
    YES {
      @Override
      public boolean isStored() { return true; }
    },

    /** Do not store the field value in the index. */
    NO {
      @Override
      public boolean isStored() { return false; }
    };

    public abstract boolean isStored();
  }

  /** Specifies whether and how a field should be indexed. */
  public static enum Index {

    /** Do not index the field value. This field can thus not be searched,
     * but one can still access its contents provided it is
     * {@link Field.Store stored}. */
    NO {
      @Override
      public boolean isIndexed()  { return false; }
      @Override
      public boolean isAnalyzed() { return false; }
      @Override
      public boolean omitNorms()  { return true;  }   
    },

    /** Index the tokens produced by running the field's
     * value through an Analyzer.  This is useful for
     * common text. */
    ANALYZED {
      @Override
      public boolean isIndexed()  { return true;  }
      @Override
      public boolean isAnalyzed() { return true;  }
      @Override
      public boolean omitNorms()  { return false; }   	
    },

    /** Index the field's value without using an Analyzer, so it can be searched.
     * As no analyzer is used the value will be stored as a single term. This is
     * useful for unique Ids like product numbers.
     */
    NOT_ANALYZED {
      @Override
      public boolean isIndexed()  { return true;  }
      @Override
      public boolean isAnalyzed() { return false; }
      @Override
      public boolean omitNorms()  { return false; }   	
    },

    /** Expert: Index the field's value without an Analyzer,
     * and also disable the indexing of norms.  Note that you
     * can also separately enable/disable norms by calling
     * {@link Field#setOmitNorms}.  No norms means that
     * index-time field and document boosting and field
     * length normalization are disabled.  The benefit is
     * less memory usage as norms take up one byte of RAM
     * per indexed field for every document in the index,
     * during searching.  Note that once you index a given
     * field <i>with</i> norms disabled, enabling norms will
     * have no effect.  In other words, for this to have the
     * above described effect on a field, one instance of
     * that field must be indexed with NOT_ANALYZED_NO_NORMS
     * at some point. */
    NOT_ANALYZED_NO_NORMS {
      @Override
      public boolean isIndexed()  { return true;  }
      @Override
      public boolean isAnalyzed() { return false; }
      @Override
      public boolean omitNorms()  { return true;  }   	
    },

    /** Expert: Index the tokens produced by running the
     *  field's value through an Analyzer, and also
     *  separately disable the storing of norms.  See
     *  {@link #NOT_ANALYZED_NO_NORMS} for what norms are
     *  and why you may want to disable them. */
    ANALYZED_NO_NORMS {
      @Override
      public boolean isIndexed()  { return true;  }
      @Override
      public boolean isAnalyzed() { return true;  }
      @Override
      public boolean omitNorms()  { return true;  }   	
    };

    /** Get the best representation of the index given the flags. */
    public static Index toIndex(boolean indexed, boolean analyzed) {
      return toIndex(indexed, analyzed, false);
    }

    /** Expert: Get the best representation of the index given the flags. */
    public static Index toIndex(boolean indexed, boolean analyzed, boolean omitNorms) {

      // If it is not indexed nothing else matters
      if (!indexed) {
        return Index.NO;
      }

      // typical, non-expert
      if (!omitNorms) {
        if (analyzed) {
          return Index.ANALYZED;
        }
        return Index.NOT_ANALYZED;
      }

      // Expert: Norms omitted
      if (analyzed) {
        return Index.ANALYZED_NO_NORMS;
      }
      return Index.NOT_ANALYZED_NO_NORMS;
    }

    public abstract boolean isIndexed();
    public abstract boolean isAnalyzed();
    public abstract boolean omitNorms();  	
  }

  /** Specifies whether and how a field should have term vectors. */
  public static enum TermVector {
    
    /** Do not store term vectors. 
     */
    NO {
      @Override
      public boolean isStored()      { return false; }
      @Override
      public boolean withPositions() { return false; }
      @Override
      public boolean withOffsets()   { return false; }
    },
    
    /** Store the term vectors of each document. A term vector is a list
     * of the document's terms and their number of occurrences in that document. */
    YES {
      @Override
      public boolean isStored()      { return true;  }
      @Override
      public boolean withPositions() { return false; }
      @Override
      public boolean withOffsets()   { return false; }
    },
    
    /**
     * Store the term vector + token position information
     * 
     * @see #YES
     */ 
    WITH_POSITIONS {
      @Override
      public boolean isStored()      { return true;  }
      @Override
      public boolean withPositions() { return true;  }
      @Override
      public boolean withOffsets()   { return false; }
    },
    
    /**
     * Store the term vector + Token offset information
     * 
     * @see #YES
     */ 
    WITH_OFFSETS {
      @Override
      public boolean isStored()      { return true;  }
      @Override
      public boolean withPositions() { return false; }
      @Override
      public boolean withOffsets()   { return true;  }
    },
    
    /**
     * Store the term vector + Token position and offset information
     * 
     * @see #YES
     * @see #WITH_POSITIONS
     * @see #WITH_OFFSETS
     */ 
    WITH_POSITIONS_OFFSETS {
      @Override
      public boolean isStored()      { return true;  }
      @Override
      public boolean withPositions() { return true;  }
      @Override
      public boolean withOffsets()   { return true;  }
    };

    /** Get the best representation of a TermVector given the flags. */
    public static TermVector toTermVector(boolean stored, boolean withOffsets, boolean withPositions) {

      // If it is not stored, nothing else matters.
      if (!stored) {
        return TermVector.NO;
      }

      if (withOffsets) {
        if (withPositions) {
          return Field.TermVector.WITH_POSITIONS_OFFSETS;
        }
        return Field.TermVector.WITH_OFFSETS;
      }

      if (withPositions) {
        return Field.TermVector.WITH_POSITIONS;
      }
      return Field.TermVector.YES;
    }

    public abstract boolean isStored();
    public abstract boolean withPositions();
    public abstract boolean withOffsets();
  }
  
  
  /** The value of the field as a String, or null.  If null, the Reader value or
   * binary value is used.  Exactly one of stringValue(),
   * readerValue(), and getBinaryValue() must be set. */
  public String stringValue()   { return fieldsData instanceof String ? (String)fieldsData : null; }
  
  /** The value of the field as a Reader, or null.  If null, the String value or
   * binary value is used.  Exactly one of stringValue(),
   * readerValue(), and getBinaryValue() must be set. */
  public Reader readerValue()   { return fieldsData instanceof Reader ? (Reader)fieldsData : null; }
    
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
  
  /** Expert: sets the token stream to be used for indexing and causes isIndexed() and isTokenized() to return true.
   *  May be combined with stored values from stringValue() or getBinaryValue() */
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
          
    this.name = name; 
    
    this.fieldsData = value;

    this.isStored = store.isStored();
   
    this.isIndexed = index.isIndexed();
    this.isTokenized = index.isAnalyzed();
    this.omitNorms = index.omitNorms();
    if (index == Index.NO) {
      // note: now this reads even wierder than before
      this.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
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
    
    this.name = name;
    this.fieldsData = reader;
    
    this.isStored = false;
    
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
    
    this.name = name;
    this.fieldsData = null;
    this.tokenStream = tokenStream;

    this.isStored = false;
    
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
   */
  public Field(String name, byte[] value) {
    this(name, value, 0, value.length);
  }

  /**
   * Create a stored field with binary value. Optionally the value may be compressed.
   * 
   * @param name The name of the field
   * @param value The binary value
   * @param offset Starting offset in value where this Field's bytes are
   * @param length Number of bytes to use for this Field, starting at offset
   */
  public Field(String name, byte[] value, int offset, int length) {

    if (name == null)
      throw new IllegalArgumentException("name cannot be null");
    if (value == null)
      throw new IllegalArgumentException("value cannot be null");
    
    this.name = name;
    fieldsData = value;
    
    isStored = true;
    isIndexed   = false;
    isTokenized = false;
    indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    omitNorms = true;
    
    isBinary    = true;
    binaryLength = length;
    binaryOffset = offset;
    
    setStoreTermVector(TermVector.NO);
  }
}
