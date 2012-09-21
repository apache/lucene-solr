package org.apache.lucene.index;

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

/** 
* StoredDocument is retrieved from IndexReader containing only stored fields from indexed {@link IndexDocument}.
*/
// TODO: shouldn't this really be in the .document package?
public class StoredDocument implements Iterable<StorableField> {

  private final List<StorableField> fields = new ArrayList<StorableField>();

  /** Sole constructor. */
  public StoredDocument() {
  }
  
  /**
   * Adds a field to a document.
   * <p> This method supports construction of a StoredDocument from a 
   * {@link StoredFieldVisitor}. This method cannot
   * be used to change the content of an existing index! In order to achieve this,
   * a document has to be deleted from an index and a new changed version of that
   * document has to be added.</p>
   */
  public final void add(StorableField field) {
    fields.add(field);
  }
  
  /**
   * Returns an array of {@link StorableField}s with the given name.
   * This method returns an empty array when there are no
   * matching fields.  It never returns null.
   *
   * @param name the name of the field
   * @return a <code>StorableField[]</code> array
   */
  public StorableField[] getFields(String name) {
    List<StorableField> result = new ArrayList<StorableField>();
    for (StorableField field : fields) {
      if (field.name().equals(name)) {
        result.add(field);
      }
    }
  
    return result.toArray(new StorableField[result.size()]);
  }
  
  /** Returns a field with the given name if any exist in this document, or
   * null.  If multiple fields exists with this name, this method returns the
   * first value added.
   */
  public final StorableField getField(String name) {
    for (StorableField field : fields) {
      if (field.name().equals(name)) {
        return field;
      }
    }
    return null;
  }
  

  /** Returns a List of all the fields in a document.
   * <p>Note that fields which are <i>not</i> stored are
   * <i>not</i> available in documents retrieved from the
   * index, e.g. {@link IndexSearcher#doc(int)} or {@link
   * IndexReader#document(int)}.
   * 
   * @return an immutable <code>List&lt;StorableField&gt;</code> 
   */
  public final List<StorableField> getFields() {
    return fields;
  }
  
  @Override
  public Iterator<StorableField> iterator() {
    return this.fields.iterator();
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
     for (StorableField field : fields) {
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
     for (StorableField field : fields) {
       if (field.name().equals(name)) {
         final BytesRef bytes = field.binaryValue();
         if (bytes != null) {
           return bytes;
         }
       }
     }
     return null;
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
     for (StorableField field : fields) {
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
     for (StorableField field : fields) {
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
    buffer.append("StoredDocument<");
    for (int i = 0; i < fields.size(); i++) {
      StorableField field = fields.get(i);
      buffer.append(field.toString());
      if (i != fields.size()-1)
        buffer.append(" ");
    }
    buffer.append(">");
    return buffer.toString();
  }
}
