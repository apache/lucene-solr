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

import java.util.Enumeration;
import java.util.List;
import java.util.ArrayList;
import java.util.Vector;
import org.apache.lucene.index.IndexReader;       // for javadoc
import org.apache.lucene.search.Searcher;         // for javadoc
import org.apache.lucene.search.Hits;             // for javadoc

/** Documents are the unit of indexing and search.
 *
 * A Document is a set of fields.  Each field has a name and a textual value.
 * A field may be {@link Field#isStored() stored} with the document, in which
 * case it is returned with search hits on the document.  Thus each document
 * should typically contain one or more stored fields which uniquely identify
 * it.
 *
 * <p>Note that fields which are <i>not</i> {@link Field#isStored() stored} are
 * <i>not</i> available in documents retrieved from the index, e.g. with {@link
 * Hits#doc(int)}, {@link Searcher#doc(int)} or {@link
 * IndexReader#document(int)}.
 */

public final class Document implements java.io.Serializable {
  List fields = new Vector();
  private float boost = 1.0f;

  /** Constructs a new document with no fields. */
  public Document() {}


  /** Sets a boost factor for hits on any field of this document.  This value
   * will be multiplied into the score of all hits on this document.
   *
   * <p>Values are multiplied into the value of {@link Field#getBoost()} of
   * each field in this document.  Thus, this method in effect sets a default
   * boost for the fields of this document.
   *
   * @see Field#setBoost(float)
   */
  public void setBoost(float boost) {
    this.boost = boost;
  }

  /** Returns the boost factor for hits on any field of this document.
   *
   * <p>The default value is 1.0.
   *
   * <p>Note: This value is not stored directly with the document in the index.
   * Documents returned from {@link IndexReader#document(int)} and
   * {@link Hits#doc(int)} may thus not have the same value present as when
   * this document was indexed.
   *
   * @see #setBoost(float)
   */
  public float getBoost() {
    return boost;
  }

  /** Adds a field to a document.  Several fields may be added with
   * the same name.  In this case, if the fields are indexed, their text is
   * treated as though appended for the purposes of search. */
  public final void add(Field field) {
    fields.add(field);
  }

  /** Returns a field with the given name if any exist in this document, or
   * null.  If multiple fields exists with this name, this method returns the
   * first value added.
   */
  public final Field getField(String name) {
    for (int i = 0; i < fields.size(); i++) {
      Field field = (Field)fields.get(i);
      if (field.name().equals(name))
	return field;
    }
    return null;
  }

  /** Returns the string value of the field with the given name if any exist in
   * this document, or null.  If multiple fields exist with this name, this
   * method returns the first value added.
   */
  public final String get(String name) {
    Field field = getField(name);
    if (field != null)
      return field.stringValue();
    else
      return null;
  }

  /** Returns an Enumeration of all the fields in a document. */
  public final Enumeration fields() {
    return ((Vector)fields).elements();
  }

  /**
   * Returns an array of {@link Field}s with the given name.
   * This method can return <code>null</code>.
   *
   * @param name the name of the field
   * @return a <code>Field[]</code> array
   */
   public final Field[] getFields(String name) {
     List result = new ArrayList();
     for (int i = 0; i < fields.size(); i++) {
       Field field = (Field)fields.get(i);
       if (field.name().equals(name)) {
         result.add(field);
       }
     }

     if (result.size() == 0)
       return null;

     return (Field[])result.toArray(new Field[result.size()]);
   }

  /**
   * Returns an array of values of the field specified as the method parameter.
   * This method can return <code>null</code>.
   *
   * @param name the name of the field
   * @return a <code>String[]</code> of field values
   */
  public final String[] getValues(String name) {
    Field[] namedFields = getFields(name);
    if (namedFields == null)
      return null;
    String[] values = new String[namedFields.length];
    for (int i = 0; i < namedFields.length; i++) {
      values[i] = namedFields[i].stringValue();
    }
    return values;
  }

  /** Prints the fields of a document for human consumption. */
  public final String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Document<");
    for (int i = 0; i < fields.size(); i++) {
      Field field = (Field)fields.get(i);
      buffer.append(field.toString());
      if (i != fields.size()-1)
        buffer.append(" ");
    }
    buffer.append(">");
    return buffer.toString();
  }
}
