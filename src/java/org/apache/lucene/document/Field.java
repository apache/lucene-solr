package org.apache.lucene.document;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.Reader;

/**
  A field is a section of a Document.  Each field has two parts, a name and a
  value.  Values may be free text, provided as a String or as a Reader, or they
  may be atomic keywords, which are not further processed.  Such keywords may
  be used to represent dates, urls, etc.  Fields are optionally stored in the
  index, so that they may be returned with hits on the document.
  */

public final class Field {
  private String name = "body";
  private String stringValue = null;
  private Reader readerValue = null;
  private boolean isStored = false;
  private boolean isIndexed = true;
  private boolean isTokenized = true;

  /** Constructs a String-valued Field that is not tokenized, but is indexed
    and stored.  Useful for non-text fields, e.g. date or url.  */
  public static final Field Keyword(String name, String value) {
    return new Field(name, value, true, true, false);
  }

  /** Constructs a String-valued Field that is not tokenized or indexed,
    but is stored in the index, for return with hits. */
  public static final Field UnIndexed(String name, String value) {
    return new Field(name, value, true, false, false);
  }

  /** Constructs a String-valued Field that is tokenized and indexed,
    and is stored in the index, for return with hits.  Useful for short text
    fields, like "title" or "subject". */
  public static final Field Text(String name, String value) {
    return new Field(name, value, true, true, true);
  }

  /** Constructs a String-valued Field that is tokenized and indexed,
    but that is not stored in the index. */
  public static final Field UnStored(String name, String value) {
    return new Field(name, value, false, true, true);
  }

  /** Constructs a Reader-valued Field that is tokenized and indexed, but is
    not stored in the index verbatim.  Useful for longer text fields, like
    "body". */
  public static final Field Text(String name, Reader value) {
    return new Field(name, value);
  }

  /** The name of the field (e.g., "date", "subject", "title", "body", etc.)
    as an interned string. */
  public String name() 		{ return name; }

  /** The value of the field as a String, or null.  If null, the Reader value
    is used.  Exactly one of stringValue() and readerValue() must be set. */
  public String stringValue()		{ return stringValue; }
  /** The value of the field as a Reader, or null.  If null, the String value
    is used.  Exactly one of stringValue() and readerValue() must be set. */
  public Reader readerValue()	{ return readerValue; }

  public Field(String name, String string,
	       boolean store, boolean index, boolean token) {
    if (name == null)
      throw new IllegalArgumentException("name cannot be null");
    if (string == null)
      throw new IllegalArgumentException("value cannot be null");

    this.name = name.intern();			  // field names are interned
    this.stringValue = string;
    this.isStored = store;
    this.isIndexed = index;
    this.isTokenized = token;
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

  /** Prints a Field for human consumption. */
  public final String toString() {
    if (isStored && isIndexed && !isTokenized)
      return "Keyword<" + name + ":" + stringValue + ">";
    else if (isStored && !isIndexed && !isTokenized)
      return "Unindexed<" + name + ":" + stringValue + ">";
    else if (isStored && isIndexed && isTokenized && stringValue!=null)
      return "Text<" + name + ":" + stringValue + ">";
    else if (!isStored && isIndexed && isTokenized && readerValue!=null)
      return "Text<" + name + ":" + readerValue + ">";
    else
      return super.toString();
  }

}
