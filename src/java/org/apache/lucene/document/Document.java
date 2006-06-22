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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.Searcher;

import java.util.*;             // for javadoc

/** Documents are the unit of indexing and search.
 *
 * A Document is a set of fields.  Each field has a name and a textual value.
 * A field may be {@link Fieldable#isStored() stored} with the document, in which
 * case it is returned with search hits on the document.  Thus each document
 * should typically contain one or more stored fields which uniquely identify
 * it.
 *
 * <p>Note that fields which are <i>not</i> {@link Fieldable#isStored() stored} are
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
   * <p>Values are multiplied into the value of {@link Fieldable#getBoost()} of
   * each field in this document.  Thus, this method in effect sets a default
   * boost for the fields of this document.
   *
   * @see Fieldable#setBoost(float)
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

  /**
   * <p>Adds a field to a document.  Several fields may be added with
   * the same name.  In this case, if the fields are indexed, their text is
   * treated as though appended for the purposes of search.</p>
   * <p> Note that add like the removeField(s) methods only makes sense 
   * prior to adding a document to an index. These methods cannot
   * be used to change the content of an existing index! In order to achieve this,
   * a document has to be deleted from an index and a new changed version of that
   * document has to be added.</p>
   */
  public final void add(Fieldable field) {
    fields.add(field);
  }
  
  /**
   * <p>Removes field with the specified name from the document.
   * If multiple fields exist with this name, this method removes the first field that has been added.
   * If there is no field with the specified name, the document remains unchanged.</p>
   * <p> Note that the removeField(s) methods like the add method only make sense 
   * prior to adding a document to an index. These methods cannot
   * be used to change the content of an existing index! In order to achieve this,
   * a document has to be deleted from an index and a new changed version of that
   * document has to be added.</p>
   */
  public final void removeField(String name) {
    Iterator it = fields.iterator();
    while (it.hasNext()) {
      Fieldable field = (Fieldable)it.next();
      if (field.name().equals(name)) {
        it.remove();
        return;
      }
    }
  }
  
  /**
   * <p>Removes all fields with the given name from the document.
   * If there is no field with the specified name, the document remains unchanged.</p>
   * <p> Note that the removeField(s) methods like the add method only make sense 
   * prior to adding a document to an index. These methods cannot
   * be used to change the content of an existing index! In order to achieve this,
   * a document has to be deleted from an index and a new changed version of that
   * document has to be added.</p>
   */
  public final void removeFields(String name) {
    Iterator it = fields.iterator();
    while (it.hasNext()) {
      Fieldable field = (Fieldable)it.next();
      if (field.name().equals(name)) {
        it.remove();
      }
    }
  }

  /** Returns a field with the given name if any exist in this document, or
   * null.  If multiple fields exists with this name, this method returns the
   * first value added.
   * Do not use this method with lazy loaded fields.
   */
  public final Field getField(String name) {
    for (int i = 0; i < fields.size(); i++) {
      Field field = (Field)fields.get(i);
      if (field.name().equals(name))
        return field;
    }
    return null;
  }


 /** Returns a field with the given name if any exist in this document, or
   * null.  If multiple fields exists with this name, this method returns the
   * first value added.
   */
 public Fieldable getFieldable(String name) {
   for (int i = 0; i < fields.size(); i++) {
     Fieldable field = (Fieldable)fields.get(i);
     if (field.name().equals(name))
       return field;
   }
   return null;
 }

  /** Returns the string value of the field with the given name if any exist in
   * this document, or null.  If multiple fields exist with this name, this
   * method returns the first value added. If only binary fields with this name
   * exist, returns null.
   */
  public final String get(String name) {
    for (int i = 0; i < fields.size(); i++) {
      Fieldable field = (Fieldable)fields.get(i);
      if (field.name().equals(name) && (!field.isBinary()))
        return field.stringValue();
    }
    return null;
  }

  /** Returns an Enumeration of all the fields in a document.
   * @deprecated use {@link #getFields()} instead
   */
  public final Enumeration fields() {
    return ((Vector)fields).elements();
  }

  /** Returns a List of all the fields in a document.
   * <p>Note that fields which are <i>not</i> {@link Fieldable#isStored() stored} are
   * <i>not</i> available in documents retrieved from the index, e.g. with {@link
   * Hits#doc(int)}, {@link Searcher#doc(int)} or {@link IndexReader#document(int)}.
   */
  public final List getFields() {
    return fields;
  }

  /**
   * Returns an array of {@link Field}s with the given name.
   * This method can return <code>null</code>.
   * Do not use with lazy loaded fields.
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
   * Returns an array of {@link Fieldable}s with the given name.
   * This method can return <code>null</code>.
   *
   * @param name the name of the field
   * @return a <code>Fieldable[]</code> array or <code>null</code>
   */
   public Fieldable[] getFieldables(String name) {
     List result = new ArrayList();
     for (int i = 0; i < fields.size(); i++) {
       Fieldable field = (Fieldable)fields.get(i);
       if (field.name().equals(name)) {
         result.add(field);
       }
     }

     if (result.size() == 0)
       return null;

     return (Fieldable[])result.toArray(new Fieldable[result.size()]);
   }


  /**
   * Returns an array of values of the field specified as the method parameter.
   * This method can return <code>null</code>.
   *
   * @param name the name of the field
   * @return a <code>String[]</code> of field values or <code>null</code>
   */
  public final String[] getValues(String name) {
    List result = new ArrayList();
    for (int i = 0; i < fields.size(); i++) {
      Fieldable field = (Fieldable)fields.get(i);
      if (field.name().equals(name) && (!field.isBinary()))
        result.add(field.stringValue());
    }
    
    if (result.size() == 0)
      return null;
    
    return (String[])result.toArray(new String[result.size()]);
  }

  /**
  * Returns an array of byte arrays for of the fields that have the name specified
  * as the method parameter. This method will return <code>null</code> if no
  * binary fields with the specified name are available.
  *
  * @param name the name of the field
  * @return a  <code>byte[][]</code> of binary field values or <code>null</code>
  */
  public final byte[][] getBinaryValues(String name) {
    List result = new ArrayList();
    for (int i = 0; i < fields.size(); i++) {
      Fieldable field = (Fieldable)fields.get(i);
      if (field.name().equals(name) && (field.isBinary()))
        result.add(field.binaryValue());
    }
  
    if (result.size() == 0)
      return null;
  
    return (byte[][])result.toArray(new byte[result.size()][]);
  }
  
  /**
  * Returns an array of bytes for the first (or only) field that has the name
  * specified as the method parameter. This method will return <code>null</code>
  * if no binary fields with the specified name are available.
  * There may be non-binary fields with the same name.
  *
  * @param name the name of the field.
  * @return a <code>byte[]</code> containing the binary field value or <code>null</code>
  */
  public final byte[] getBinaryValue(String name) {
    for (int i=0; i < fields.size(); i++) {
      Fieldable field = (Fieldable)fields.get(i);
      if (field.name().equals(name) && (field.isBinary()))
        return field.binaryValue();
    }
    return null;
  }
  
  /** Prints the fields of a document for human consumption. */
  public final String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Document<");
    for (int i = 0; i < fields.size(); i++) {
      Fieldable field = (Fieldable)fields.get(i);
      buffer.append(field.toString());
      if (i != fields.size()-1)
        buffer.append(" ");
    }
    buffer.append(">");
    return buffer.toString();
  }
}
