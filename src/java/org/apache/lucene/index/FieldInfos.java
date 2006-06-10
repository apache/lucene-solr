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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.*;

/** Access to the Fieldable Info file that describes document fields and whether or
 *  not they are indexed. Each segment has a separate Fieldable Info file. Objects
 *  of this class are thread-safe for multiple readers, but only one thread can
 *  be adding documents at a time, with no other reader or writer threads
 *  accessing this object.
 */
final class FieldInfos {
  
  static final byte IS_INDEXED = 0x1;
  static final byte STORE_TERMVECTOR = 0x2;
  static final byte STORE_POSITIONS_WITH_TERMVECTOR = 0x4;
  static final byte STORE_OFFSET_WITH_TERMVECTOR = 0x8;
  static final byte OMIT_NORMS = 0x10;
  
  private ArrayList byNumber = new ArrayList();
  private HashMap byName = new HashMap();

  FieldInfos() { }

  /**
   * Construct a FieldInfos object using the directory and the name of the file
   * IndexInput
   * @param d The directory to open the IndexInput from
   * @param name The name of the file to open the IndexInput from in the Directory
   * @throws IOException
   */
  FieldInfos(Directory d, String name) throws IOException {
    IndexInput input = d.openInput(name);
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
      Fieldable field = (Fieldable) fields.nextElement();
      add(field.name(), field.isIndexed(), field.isTermVectorStored(), field.isStorePositionWithTermVector(),
              field.isStoreOffsetWithTermVector(), field.getOmitNorms());
    }
  }
  
  /**
   * Add fields that are indexed. Whether they have termvectors has to be specified.
   * 
   * @param names The names of the fields
   * @param storeTermVectors Whether the fields store term vectors or not
   * @param storePositionWithTermVector treu if positions should be stored.
   * @param storeOffsetWithTermVector true if offsets should be stored
   */
  public void addIndexed(Collection names, boolean storeTermVectors, boolean storePositionWithTermVector, 
                         boolean storeOffsetWithTermVector) {
    Iterator i = names.iterator();
    while (i.hasNext()) {
      add((String)i.next(), true, storeTermVectors, storePositionWithTermVector, storeOffsetWithTermVector);
    }
  }

  /**
   * Assumes the fields are not storing term vectors.
   * 
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
   * Calls 5 parameter add with false for all TermVector parameters.
   * 
   * @param name The name of the Fieldable
   * @param isIndexed true if the field is indexed
   * @see #add(String, boolean, boolean, boolean, boolean)
   */
  public void add(String name, boolean isIndexed) {
    add(name, isIndexed, false, false, false, false);
  }

  /**
   * Calls 5 parameter add with false for term vector positions and offsets.
   * 
   * @param name The name of the field
   * @param isIndexed  true if the field is indexed
   * @param storeTermVector true if the term vector should be stored
   */
  public void add(String name, boolean isIndexed, boolean storeTermVector){
    add(name, isIndexed, storeTermVector, false, false, false);
  }
  
  /** If the field is not yet known, adds it. If it is known, checks to make
   *  sure that the isIndexed flag is the same as was given previously for this
   *  field. If not - marks it as being indexed.  Same goes for the TermVector
   * parameters.
   * 
   * @param name The name of the field
   * @param isIndexed true if the field is indexed
   * @param storeTermVector true if the term vector should be stored
   * @param storePositionWithTermVector true if the term vector with positions should be stored
   * @param storeOffsetWithTermVector true if the term vector with offsets should be stored
   */
  public void add(String name, boolean isIndexed, boolean storeTermVector,
                  boolean storePositionWithTermVector, boolean storeOffsetWithTermVector) {

    add(name, isIndexed, storeTermVector, storePositionWithTermVector, storeOffsetWithTermVector, false);
  }

    /** If the field is not yet known, adds it. If it is known, checks to make
   *  sure that the isIndexed flag is the same as was given previously for this
   *  field. If not - marks it as being indexed.  Same goes for the TermVector
   * parameters.
   *
   * @param name The name of the field
   * @param isIndexed true if the field is indexed
   * @param storeTermVector true if the term vector should be stored
   * @param storePositionWithTermVector true if the term vector with positions should be stored
   * @param storeOffsetWithTermVector true if the term vector with offsets should be stored
   * @param omitNorms true if the norms for the indexed field should be omitted
   */
  public void add(String name, boolean isIndexed, boolean storeTermVector,
                  boolean storePositionWithTermVector, boolean storeOffsetWithTermVector, boolean omitNorms) {
    FieldInfo fi = fieldInfo(name);
    if (fi == null) {
      addInternal(name, isIndexed, storeTermVector, storePositionWithTermVector, storeOffsetWithTermVector, omitNorms);
    } else {
      if (fi.isIndexed != isIndexed) {
        fi.isIndexed = true;                      // once indexed, always index
      }
      if (fi.storeTermVector != storeTermVector) {
        fi.storeTermVector = true;                // once vector, always vector
      }
      if (fi.storePositionWithTermVector != storePositionWithTermVector) {
        fi.storePositionWithTermVector = true;                // once vector, always vector
      }
      if (fi.storeOffsetWithTermVector != storeOffsetWithTermVector) {
        fi.storeOffsetWithTermVector = true;                // once vector, always vector
      }
      if (fi.omitNorms != omitNorms) {
        fi.omitNorms = false;                // once norms are stored, always store
      }

    }
  }


  private void addInternal(String name, boolean isIndexed,
                           boolean storeTermVector, boolean storePositionWithTermVector, 
                           boolean storeOffsetWithTermVector, boolean omitNorms) {
    FieldInfo fi =
      new FieldInfo(name, isIndexed, byNumber.size(), storeTermVector, storePositionWithTermVector,
              storeOffsetWithTermVector, omitNorms);
    byNumber.add(fi);
    byName.put(name, fi);
  }

  public int fieldNumber(String fieldName) {
    try {
      FieldInfo fi = fieldInfo(fieldName);
      if (fi != null)
        return fi.number;
    }
    catch (IndexOutOfBoundsException ioobe) {
      return -1;
    }
    return -1;
  }

  public FieldInfo fieldInfo(String fieldName) {
    return (FieldInfo) byName.get(fieldName);
  }

  /**
   * Return the fieldName identified by its number.
   * 
   * @param fieldNumber
   * @return the fieldName or an empty string when the field
   * with the given number doesn't exist.
   */  
  public String fieldName(int fieldNumber) {
    try {
      return fieldInfo(fieldNumber).name;
    }
    catch (NullPointerException npe) {
      return "";
    }
  }

  /**
   * Return the fieldinfo object referenced by the fieldNumber.
   * @param fieldNumber
   * @return the FieldInfo object or null when the given fieldNumber
   * doesn't exist.
   */  
  public FieldInfo fieldInfo(int fieldNumber) {
    try {
      return (FieldInfo) byNumber.get(fieldNumber);
    }
    catch (IndexOutOfBoundsException ioobe) {
      return null;
    }
  }

  public int size() {
    return byNumber.size();
  }

  public boolean hasVectors() {
    boolean hasVectors = false;
    for (int i = 0; i < size(); i++) {
      if (fieldInfo(i).storeTermVector) {
        hasVectors = true;
        break;
      }
    }
    return hasVectors;
  }

  public void write(Directory d, String name) throws IOException {
    IndexOutput output = d.createOutput(name);
    try {
      write(output);
    } finally {
      output.close();
    }
  }

  public void write(IndexOutput output) throws IOException {
    output.writeVInt(size());
    for (int i = 0; i < size(); i++) {
      FieldInfo fi = fieldInfo(i);
      byte bits = 0x0;
      if (fi.isIndexed) bits |= IS_INDEXED;
      if (fi.storeTermVector) bits |= STORE_TERMVECTOR;
      if (fi.storePositionWithTermVector) bits |= STORE_POSITIONS_WITH_TERMVECTOR;
      if (fi.storeOffsetWithTermVector) bits |= STORE_OFFSET_WITH_TERMVECTOR;
      if (fi.omitNorms) bits |= OMIT_NORMS;
      output.writeString(fi.name);
      output.writeByte(bits);
    }
  }

  private void read(IndexInput input) throws IOException {
    int size = input.readVInt();//read in the size
    for (int i = 0; i < size; i++) {
      String name = input.readString().intern();
      byte bits = input.readByte();
      boolean isIndexed = (bits & IS_INDEXED) != 0;
      boolean storeTermVector = (bits & STORE_TERMVECTOR) != 0;
      boolean storePositionsWithTermVector = (bits & STORE_POSITIONS_WITH_TERMVECTOR) != 0;
      boolean storeOffsetWithTermVector = (bits & STORE_OFFSET_WITH_TERMVECTOR) != 0;
      boolean omitNorms = (bits & OMIT_NORMS) != 0;

      addInternal(name, isIndexed, storeTermVector, storePositionsWithTermVector, storeOffsetWithTermVector, omitNorms);
    }    
  }

}
