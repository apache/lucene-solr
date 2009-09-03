package org.apache.lucene.index;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;
import java.util.*;

/** Access to the Fieldable Info file that describes document fields and whether or
 *  not they are indexed. Each segment has a separate Fieldable Info file. Objects
 *  of this class are thread-safe for multiple readers, but only one thread can
 *  be adding documents at a time, with no other reader or writer threads
 *  accessing this object.
 */
final class FieldInfos {

  // Used internally (ie not written to *.fnm files) for pre-2.9 files
  public static final int FORMAT_PRE = -1;

  // First used in 2.9; prior to 2.9 there was no format header
  public static final int FORMAT_START = -2;

  static final int CURRENT_FORMAT = FORMAT_START;
  
  static final byte IS_INDEXED = 0x1;
  static final byte STORE_TERMVECTOR = 0x2;
  static final byte STORE_POSITIONS_WITH_TERMVECTOR = 0x4;
  static final byte STORE_OFFSET_WITH_TERMVECTOR = 0x8;
  static final byte OMIT_NORMS = 0x10;
  static final byte STORE_PAYLOADS = 0x20;
  static final byte OMIT_TERM_FREQ_AND_POSITIONS = 0x40;
  
  private final ArrayList byNumber = new ArrayList();
  private final HashMap byName = new HashMap();
  private int format;

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
      try {
        read(input, name);
      } catch (IOException ioe) {
        if (format == FORMAT_PRE) {
          // LUCENE-1623: FORMAT_PRE (before there was a
          // format) may be 2.3.2 (pre-utf8) or 2.4.x (utf8)
          // encoding; retry with input set to pre-utf8
          input.seek(0);
          input.setModifiedUTF8StringsMode();
          byNumber.clear();
          byName.clear();
          try {
            read(input, name);
          } catch (Throwable t) {
            // Ignore any new exception & throw original IOE
            throw ioe;
          }
        } else {
          // The IOException cannot be caused by
          // LUCENE-1623, so re-throw it
          throw ioe;
        }
      }
    } finally {
      input.close();
    }
  }

  /**
   * Returns a deep clone of this FieldInfos instance.
   */
  synchronized public Object clone() {
    FieldInfos fis = new FieldInfos();
    final int numField = byNumber.size();
    for(int i=0;i<numField;i++) {
      FieldInfo fi = (FieldInfo) ((FieldInfo) byNumber.get(i)).clone();
      fis.byNumber.add(fi);
      fis.byName.put(fi.name, fi);
    }
    return fis;
  }

  /** Adds field info for a Document. */
  synchronized public void add(Document doc) {
    List fields = doc.getFields();
    Iterator fieldIterator = fields.iterator();
    while (fieldIterator.hasNext()) {
      Fieldable field = (Fieldable) fieldIterator.next();
      add(field.name(), field.isIndexed(), field.isTermVectorStored(), field.isStorePositionWithTermVector(),
              field.isStoreOffsetWithTermVector(), field.getOmitNorms(), false, field.getOmitTf());
    }
  }

  /** Returns true if any fields do not omitTermFreqAndPositions */
  boolean hasProx() {
    final int numFields = byNumber.size();
    for(int i=0;i<numFields;i++) {
      final FieldInfo fi = fieldInfo(i);
      if (fi.isIndexed && !fi.omitTermFreqAndPositions) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Add fields that are indexed. Whether they have termvectors has to be specified.
   * 
   * @param names The names of the fields
   * @param storeTermVectors Whether the fields store term vectors or not
   * @param storePositionWithTermVector true if positions should be stored.
   * @param storeOffsetWithTermVector true if offsets should be stored
   */
  synchronized public void addIndexed(Collection names, boolean storeTermVectors, boolean storePositionWithTermVector, 
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
  synchronized public void add(Collection names, boolean isIndexed) {
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
  synchronized public void add(String name, boolean isIndexed) {
    add(name, isIndexed, false, false, false, false);
  }

  /**
   * Calls 5 parameter add with false for term vector positions and offsets.
   * 
   * @param name The name of the field
   * @param isIndexed  true if the field is indexed
   * @param storeTermVector true if the term vector should be stored
   */
  synchronized public void add(String name, boolean isIndexed, boolean storeTermVector){
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
  synchronized public void add(String name, boolean isIndexed, boolean storeTermVector,
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
  synchronized public void add(String name, boolean isIndexed, boolean storeTermVector,
                  boolean storePositionWithTermVector, boolean storeOffsetWithTermVector, boolean omitNorms) {
    add(name, isIndexed, storeTermVector, storePositionWithTermVector,
        storeOffsetWithTermVector, omitNorms, false, false);
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
   * @param storePayloads true if payloads should be stored for this field
   * @param omitTermFreqAndPositions true if term freqs should be omitted for this field
   */
  synchronized public FieldInfo add(String name, boolean isIndexed, boolean storeTermVector,
                       boolean storePositionWithTermVector, boolean storeOffsetWithTermVector,
                       boolean omitNorms, boolean storePayloads, boolean omitTermFreqAndPositions) {
    FieldInfo fi = fieldInfo(name);
    if (fi == null) {
      return addInternal(name, isIndexed, storeTermVector, storePositionWithTermVector, storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions);
    } else {
      fi.update(isIndexed, storeTermVector, storePositionWithTermVector, storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions);
    }
    return fi;
  }

  private FieldInfo addInternal(String name, boolean isIndexed,
                                boolean storeTermVector, boolean storePositionWithTermVector, 
                                boolean storeOffsetWithTermVector, boolean omitNorms, boolean storePayloads, boolean omitTermFreqAndPositions) {
    name = StringHelper.intern(name);
    FieldInfo fi = new FieldInfo(name, isIndexed, byNumber.size(), storeTermVector, storePositionWithTermVector,
                                 storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions);
    byNumber.add(fi);
    byName.put(name, fi);
    return fi;
  }

  public int fieldNumber(String fieldName) {
    FieldInfo fi = fieldInfo(fieldName);
    return (fi != null) ? fi.number : -1;
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
	FieldInfo fi = fieldInfo(fieldNumber);
	return (fi != null) ? fi.name : "";
  }

  /**
   * Return the fieldinfo object referenced by the fieldNumber.
   * @param fieldNumber
   * @return the FieldInfo object or null when the given fieldNumber
   * doesn't exist.
   */  
  public FieldInfo fieldInfo(int fieldNumber) {
	return (fieldNumber >= 0) ? (FieldInfo) byNumber.get(fieldNumber) : null;
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
    output.writeVInt(CURRENT_FORMAT);
    output.writeVInt(size());
    for (int i = 0; i < size(); i++) {
      FieldInfo fi = fieldInfo(i);
      byte bits = 0x0;
      if (fi.isIndexed) bits |= IS_INDEXED;
      if (fi.storeTermVector) bits |= STORE_TERMVECTOR;
      if (fi.storePositionWithTermVector) bits |= STORE_POSITIONS_WITH_TERMVECTOR;
      if (fi.storeOffsetWithTermVector) bits |= STORE_OFFSET_WITH_TERMVECTOR;
      if (fi.omitNorms) bits |= OMIT_NORMS;
      if (fi.storePayloads) bits |= STORE_PAYLOADS;
      if (fi.omitTermFreqAndPositions) bits |= OMIT_TERM_FREQ_AND_POSITIONS;
      
      output.writeString(fi.name);
      output.writeByte(bits);
    }
  }

  private void read(IndexInput input, String fileName) throws IOException {
    int firstInt = input.readVInt();

    if (firstInt < 0) {
      // This is a real format
      format = firstInt;
    } else {
      format = FORMAT_PRE;
    }

    if (format != FORMAT_PRE & format != FORMAT_START) {
      throw new CorruptIndexException("unrecognized format " + format + " in file \"" + fileName + "\"");
    }

    int size;
    if (format == FORMAT_PRE) {
      size = firstInt;
    } else {
      size = input.readVInt(); //read in the size
    }

    for (int i = 0; i < size; i++) {
      String name = StringHelper.intern(input.readString());
      byte bits = input.readByte();
      boolean isIndexed = (bits & IS_INDEXED) != 0;
      boolean storeTermVector = (bits & STORE_TERMVECTOR) != 0;
      boolean storePositionsWithTermVector = (bits & STORE_POSITIONS_WITH_TERMVECTOR) != 0;
      boolean storeOffsetWithTermVector = (bits & STORE_OFFSET_WITH_TERMVECTOR) != 0;
      boolean omitNorms = (bits & OMIT_NORMS) != 0;
      boolean storePayloads = (bits & STORE_PAYLOADS) != 0;
      boolean omitTermFreqAndPositions = (bits & OMIT_TERM_FREQ_AND_POSITIONS) != 0;
      
      addInternal(name, isIndexed, storeTermVector, storePositionsWithTermVector, storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions);
    }

    if (input.getFilePointer() != input.length()) {
      throw new CorruptIndexException("did not read all bytes from file \"" + fileName + "\": read " + input.getFilePointer() + " vs size " + input.length());
    }    
  }

}
