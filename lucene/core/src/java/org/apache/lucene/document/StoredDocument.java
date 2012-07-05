package org.apache.lucene.document;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.StorableField;
import org.apache.lucene.util.BytesRef;

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

public class StoredDocument implements Iterable<StorableField>{
  
  private final List<StorableField> fields = new ArrayList<StorableField>();
  
  
  public final void add(StorableField field) {
    fields.add(field);
  }
  
  public StorableField[] getFields(String name) {
    List<StorableField> result = new ArrayList<StorableField>();
    for (StorableField field : fields) {
      if (field.name().equals(name)) {
        result.add(field);
      }
    }
  
    return result.toArray(new StorableField[result.size()]);
  }
  
  public final StorableField getField(String name) {
    for (StorableField field : fields) {
      if (field.name().equals(name)) {
        return field;
      }
    }
    return null;
  }
  
  public final void removeField(String name) {
    Iterator<StorableField> it = fields.iterator();
    while (it.hasNext()) {
      StorableField field = it.next();
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
    Iterator<StorableField> it = fields.iterator();
    while (it.hasNext()) {
      StorableField field = it.next();
      if (field.name().equals(name)) {
        it.remove();
      }
    }
  }
  
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
   * @return a <code>byte[][]</code> of binary field values
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
   * @return a <code>byte[]</code> containing the binary field value or <code>null</code>
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

  public Document asIndexable() {
    Document doc = new Document();
    
    for (StorableField field : fields) {
      Field newField = new Field(field.name(), field.fieldType());
      
      newField.fieldsData = field.stringValue();
      if (newField.fieldsData == null) 
        newField.fieldsData = field.numericValue();
      if (newField.fieldsData == null) 
        newField.fieldsData = field.binaryValue();
      if (newField.fieldsData == null) 
        newField.fieldsData = field.readerValue();
      
      doc.add(newField);
    }
    
    return doc;
  }
}
