package org.apache.lucene.index;

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

import java.util.*;
import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.OutputStream;
import org.apache.lucene.store.InputStream;

/** Access to the Field Info file that describes document fields and whether or
 *  not they are indexed. Each segment has a separate Field Info file. Objects
 *  of this class are thread-safe for multiple readers, but only one thread can
 *  be adding documents at a time, with no other reader or writer threads
 *  accessing this object.
 */
final class FieldInfos {
  private ArrayList byNumber = new ArrayList();
  private HashMap byName = new HashMap();

  FieldInfos() {
    add("", false);
  }

  /**
   * Construct a FieldInfos object using the directory and the name of the file
   * InputStream
   * @param d The directory to open the InputStream from
   * @param name The name of the file to open the InputStream from in the Directory
   * @throws IOException
   */
  FieldInfos(Directory d, String name) throws IOException {
    InputStream input = d.openFile(name);
    try {
      read(input);
    } finally {
      input.close();
    }
  }

  /** Adds field info for a Document. */
  public void add(Document doc) {
    Enumeration fields = doc.fields();
    while (fields.hasMoreElements()) {
      Field field = (Field) fields.nextElement();
      add(field.name(), field.isIndexed(), field.isTermVectorStored());
    }
  }

  /**
   * @param names The names of the fields
   * @param storeTermVectors Whether the fields store term vectors or not
   */
  public void addIndexed(Collection names, boolean storeTermVectors) {
    Iterator i = names.iterator();
    while (i.hasNext()) {
      add((String)i.next(), true, storeTermVectors);
    }
  }

  /**
   * Assumes the field is not storing term vectors 
   * @param names The names of the fields
   * @param isIndexed Whether the fields are indexed or not
   * 
   * @see #add(String, boolean)
   */
  public void add(Collection names, boolean isIndexed) {
    Iterator i = names.iterator();
    while (i.hasNext()) {
      add((String)i.next(), isIndexed);
    }
  }

  /**
   * Calls three parameter add with false for the storeTermVector parameter 
   * @param name The name of the Field
   * @param isIndexed true if the field is indexed
   * @see #add(String, boolean, boolean)
   */
  public void add(String name, boolean isIndexed) {
    add(name, isIndexed, false);
  }


  /** If the field is not yet known, adds it. If it is known, checks to make
   *  sure that the isIndexed flag is the same as was given previously for this
   *  field. If not - marks it as being indexed.  Same goes for storeTermVector
   * 
   * @param name The name of the field
   * @param isIndexed true if the field is indexed
   * @param storeTermVector true if the term vector should be stored
   */
  public void add(String name, boolean isIndexed, boolean storeTermVector) {
    FieldInfo fi = fieldInfo(name);
    if (fi == null) {
      addInternal(name, isIndexed, storeTermVector);
    } else {
      if (fi.isIndexed != isIndexed) {
        fi.isIndexed = true;                      // once indexed, always index
      }
      if (fi.storeTermVector != storeTermVector) {
        fi.storeTermVector = true;                // once vector, always vector
      }
    }
  }

  private void addInternal(String name, boolean isIndexed,
                           boolean storeTermVector) {
    FieldInfo fi =
      new FieldInfo(name, isIndexed, byNumber.size(), storeTermVector);
    byNumber.add(fi);
    byName.put(name, fi);
  }

  public int fieldNumber(String fieldName) {
    FieldInfo fi = fieldInfo(fieldName);
    if (fi != null)
      return fi.number;
    else
      return -1;
  }

  public FieldInfo fieldInfo(String fieldName) {
    return (FieldInfo) byName.get(fieldName);
  }

  public String fieldName(int fieldNumber) {
    return fieldInfo(fieldNumber).name;
  }

  public FieldInfo fieldInfo(int fieldNumber) {
    return (FieldInfo) byNumber.get(fieldNumber);
  }

  public int size() {
    return byNumber.size();
  }

  public boolean hasVectors() {
    boolean hasVectors = false;
    for (int i = 0; i < size(); i++) {
      if (fieldInfo(i).storeTermVector)
        hasVectors = true;
    }
    return hasVectors;
  }

  public void write(Directory d, String name) throws IOException {
    OutputStream output = d.createFile(name);
    try {
      write(output);
    } finally {
      output.close();
    }
  }

  public void write(OutputStream output) throws IOException {
    output.writeVInt(size());
    for (int i = 0; i < size(); i++) {
      FieldInfo fi = fieldInfo(i);
      byte bits = 0x0;
      if (fi.isIndexed) bits |= 0x1;
      if (fi.storeTermVector) bits |= 0x2;
      output.writeString(fi.name);
      //Was REMOVE
      //output.writeByte((byte)(fi.isIndexed ? 1 : 0));
      output.writeByte(bits);
    }
  }

  private void read(InputStream input) throws IOException {
    int size = input.readVInt();//read in the size
    for (int i = 0; i < size; i++) {
      String name = input.readString().intern();
      byte bits = input.readByte();
      boolean isIndexed = (bits & 0x1) != 0;
      boolean storeTermVector = (bits & 0x2) != 0;
      addInternal(name, isIndexed, storeTermVector);
    }    
  }

}
