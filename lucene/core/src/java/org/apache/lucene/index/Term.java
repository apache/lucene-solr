package org.apache.lucene.index;

import org.apache.lucene.util.StringHelper;

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

/**
  A Term represents a word from text.  This is the unit of search.  It is
  composed of two elements, the text of the word, as a string, and the name of
  the field that the text occurred in, an interned string.

  Note that terms may represent more than words from text fields, but also
  things like dates, email addresses, urls, etc.  */

public final class Term implements Comparable<Term>, java.io.Serializable {
  String field;
  String text;

  /** Constructs a Term with the given field and text.
   * <p>Note that a null field or null text value results in undefined
   * behavior for most Lucene APIs that accept a Term parameter. */
  public Term(String fld, String txt) {
    field = StringHelper.intern(fld);
    text = txt;
  }

  /** Constructs a Term with the given field and empty text.
   * This serves two purposes: 1) reuse of a Term with the same field.
   * 2) pattern for a query.
   * 
   * @param fld
   */
  public Term(String fld) {
    this(fld, "", true);
  }

  Term(String fld, String txt, boolean intern) {
    field = intern ? StringHelper.intern(fld) : fld;	  // field names are interned
    text = txt;					          // unless already known to be
  }

  /** Returns the field of this term, an interned string.   The field indicates
    the part of a document which this term came from. */
  public final String field() { return field; }

  /** Returns the text of this term.  In the case of words, this is simply the
    text of the word.  In the case of dates and other types, this is an
    encoding of the object as a string.  */
  public final String text() { return text; }
  
  /**
   * Optimized construction of new Terms by reusing same field as this Term
   * - avoids field.intern() overhead 
   * @param text The text of the new term (field is implicitly same as this Term instance)
   * @return A new Term
   */
  public Term createTerm(String text)
  {
      return new Term(field,text,false);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Term other = (Term) obj;
    if (field == null) {
      if (other.field != null)
        return false;
    } else if (field != other.field)
      return false;
    if (text == null) {
      if (other.text != null)
        return false;
    } else if (!text.equals(other.text))
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((field == null) ? 0 : field.hashCode());
    result = prime * result + ((text == null) ? 0 : text.hashCode());
    return result;
  }

  /** Compares two terms, returning a negative integer if this
    term belongs before the argument, zero if this term is equal to the
    argument, and a positive integer if this term belongs after the argument.

    The ordering of terms is first by field, then by text.*/
  public final int compareTo(Term other) {
    if (field == other.field)			  // fields are interned
      return text.compareTo(other.text);
    else
      return field.compareTo(other.field);
  }

  /** Resets the field and text of a Term. */
  final void set(String fld, String txt) {
    field = fld;
    text = txt;
  }

  @Override
  public final String toString() { return field + ":" + text; }

  private void readObject(java.io.ObjectInputStream in)
    throws java.io.IOException, ClassNotFoundException
  {
      in.defaultReadObject();
      field = StringHelper.intern(field);
  }
}
