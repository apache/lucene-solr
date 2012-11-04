package org.apache.lucene.document;

/*
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

import java.util.*;

import org.apache.lucene.index.IndexDocument;
import org.apache.lucene.index.IndexReader;  // for javadoc
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.search.IndexSearcher;  // for javadoc
import org.apache.lucene.search.ScoreDoc; // for javadoc
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FilterIterator;

/** Documents are the unit of indexing and search.
 *
 * A Document is a set of fields.  Each field has a name and a textual value.
 * A field may be {@link org.apache.lucene.index.IndexableFieldType#stored() stored} with the document, in which
 * case it is returned with search hits on the document.  Thus each document
 * should typically contain one or more stored fields which uniquely identify
 * it.
 *
 * <p>Note that fields which are <i>not</i> {@link org.apache.lucene.index.IndexableFieldType#stored() stored} are
 * <i>not</i> available in documents retrieved from the index, e.g. with {@link
 * ScoreDoc#doc} or {@link IndexReader#document(int)}.
 */

public final class Document implements IndexDocument {

  private final List<Field> fields = new ArrayList<Field>();

  /** Constructs a new document with no fields. */
  public Document() {}
  

  /**
  * Creates a Document from StoredDocument so it that can be used e.g. for another
  * round of indexing.
  *
  */
  public Document(StoredDocument storedDoc) {
    for (StorableField field : storedDoc.getFields()) {
      Field newField = new Field(field.name(), (FieldType) field.fieldType());
     
      newField.fieldsData = field.stringValue();
      if (newField.fieldsData == null) 
        newField.fieldsData = field.numericValue();
      if (newField.fieldsData == null) 
        newField.fieldsData = field.binaryValue();
      if (newField.fieldsData == null) 
        newField.fieldsData = field.readerValue();
     
      add(newField);
    }
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
  public final void add(Field field) {
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
    Iterator<Field> it = fields.iterator();
    while (it.hasNext()) {
      Field field = it.next();
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
    Iterator<Field> it = fields.iterator();
    while (it.hasNext()) {
      Field field = it.next();
      if (field.name().equals(name)) {
        it.remove();
      }
    }
  }


  /**
  * Returns an array of byte arrays for of the fields that have the name specified
  * as the method parameter.  This method returns an empty
  * array when there are no matching fields.  It never
  * returns null.
  *
  * @param name the name of the field
  * @return a <code>BytesRef[]</code> of binary field values
  */
  public final BytesRef[] getBinaryValues(String name) {
    final List<BytesRef> result = new ArrayList<BytesRef>();

    for (Iterator<StorableField> it = storedFieldsIterator(); it.hasNext(); ) {
      StorableField field = it.next();
      if (field.name().equals(name)) {
        final BytesRef bytes = field.binaryValue();
        if (bytes != null) {
          result.add(bytes);
        }
      }
    }
  
    return result.toArray(new BytesRef[result.size()]);
  }
  
  /**
  * Returns an array of bytes for the first (or only) field that has the name
  * specified as the method parameter. This method will return <code>null</code>
  * if no binary fields with the specified name are available.
  * There may be non-binary fields with the same name.
  *
  * @param name the name of the field.
  * @return a <code>BytesRef</code> containing the binary field value or <code>null</code>
  */
  public final BytesRef getBinaryValue(String name) {
    for (Iterator<StorableField> it = storedFieldsIterator(); it.hasNext(); ) {
      StorableField field = it.next();
      if (field.name().equals(name)) {
        final BytesRef bytes = field.binaryValue();
        if (bytes != null) {
          return bytes;
        }
      }
    }
    return null;
  }

  /** Returns a field with the given name if any exist in this document, or
   * null.  If multiple fields exists with this name, this method returns the
   * first value added.
   */
  public final Field getField(String name) {
    for (Field field : fields) {
      if (field.name().equals(name)) {
        return field;
      }
    }
    return null;
  }

  /**
   * Returns an array of {@link IndexableField}s with the given name.
   * This method returns an empty array when there are no
   * matching fields.  It never returns null.
   *
   * @param name the name of the field
   * @return a <code>Field[]</code> array
   */
  public Field[] getFields(String name) {
    List<Field> result = new ArrayList<Field>();
    for (Field field : fields) {
      if (field.name().equals(name)) {
        result.add(field);
      }
    }

    return result.toArray(new Field[result.size()]);
  }
  
  /** Returns a List of all the fields in a document.
   * <p>Note that fields which are <i>not</i> stored are
   * <i>not</i> available in documents retrieved from the
   * index, e.g. {@link IndexSearcher#doc(int)} or {@link
   * IndexReader#document(int)}.
   * 
   * @return an immutable <code>List&lt;Field&gt;</code> 
   */
  public final List<Field> getFields() {
    return Collections.unmodifiableList(fields);
  }
  
   private final static String[] NO_STRINGS = new String[0];

  /**
   * Returns an array of values of the field specified as the method parameter.
   * This method returns an empty array when there are no
   * matching fields.  It never returns null.
   * For {@link IntField}, {@link LongField}, {@link
   * FloatField} and {@link DoubleField} it returns the string value of the number. If you want
   * the actual numeric field instances back, use {@link #getFields}.
   * @param name the name of the field
   * @return a <code>String[]</code> of field values
   */
  public final String[] getValues(String name) {
    List<String> result = new ArrayList<String>();

    for (Iterator<StorableField> it = storedFieldsIterator(); it.hasNext(); ) {
      StorableField field = it.next();
      if (field.name().equals(name) && field.stringValue() != null) {
        result.add(field.stringValue());
      }
    }
    
    if (result.size() == 0) {
      return NO_STRINGS;
    }
    
    return result.toArray(new String[result.size()]);
  }

  /** Returns the string value of the field with the given name if any exist in
   * this document, or null.  If multiple fields exist with this name, this
   * method returns the first value added. If only binary fields with this name
   * exist, returns null.
   * For {@link IntField}, {@link LongField}, {@link
   * FloatField} and {@link DoubleField} it returns the string value of the number. If you want
   * the actual numeric field instance back, use {@link #getField}.
   */
  public final String get(String name) {
    for (Iterator<StorableField> it = storedFieldsIterator(); it.hasNext(); ) {
      StorableField field = it.next();
      if (field.name().equals(name) && field.stringValue() != null) {
        return field.stringValue();
      }
    }
    return null;
  }
  
  /** Prints the fields of a document for human consumption. */
  @Override
  public final String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("Document<");
    for (int i = 0; i < fields.size(); i++) {
      IndexableField field = fields.get(i);
      buffer.append(field.toString());
      if (i != fields.size()-1)
        buffer.append(" ");
    }
    buffer.append(">");
    return buffer.toString();
  }

  /** Obtains all indexed fields in document */
  @Override
  public Iterable<IndexableField> indexableFields() {
    return new Iterable<IndexableField>() {
      @Override
      public Iterator<IndexableField> iterator() {
        return Document.this.indexedFieldsIterator();
      }
    };
  }

  /** Obtains all stored fields in document. */
  @Override
  public Iterable<StorableField> storableFields() {
    return new Iterable<StorableField>() {
      @Override
      public Iterator<StorableField> iterator() {
        return Document.this.storedFieldsIterator();
      }
    };
  }

  private Iterator<StorableField> storedFieldsIterator() {
    return new FilterIterator<StorableField, Field>(fields.iterator()) {
      @Override
      protected boolean predicateFunction(Field field) {
        return field.type.stored() || field.type.docValueType() != null;
      }
    };
  }
  
  private Iterator<IndexableField> indexedFieldsIterator() {
    return new FilterIterator<IndexableField, Field>(fields.iterator()) {
      @Override
      protected boolean predicateFunction(Field field) {
        return field.type.indexed();
      }
    };
  }

  /** Removes all the fields from document. */
  public void clear() {
    fields.clear();
  }
}
