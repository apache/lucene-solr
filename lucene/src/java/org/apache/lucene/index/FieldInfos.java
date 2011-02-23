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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.StringHelper;

/** Access to the Fieldable Info file that describes document fields and whether or
 *  not they are indexed. Each segment has a separate Fieldable Info file. Objects
 *  of this class are thread-safe for multiple readers, but only one thread can
 *  be adding documents at a time, with no other reader or writer threads
 *  accessing this object.
 *  @lucene.experimental
 */
public final class FieldInfos implements Iterable<FieldInfo> {
  private static final class FieldNumberBiMap {
    private final Map<Integer,String> numberToName;
    private final Map<String,Integer> nameToNumber;

    private FieldNumberBiMap() {
      this.nameToNumber = new HashMap<String, Integer>();
      this.numberToName = new HashMap<Integer, String>();
    }

    synchronized int addOrGet(String fieldName, FieldInfoBiMap fieldInfoMap, int preferredFieldNumber) {
      Integer fieldNumber = nameToNumber.get(fieldName);
      if (fieldNumber == null) {
        if (!numberToName.containsKey(preferredFieldNumber)) {
          // cool - we can use this number globally
          fieldNumber = preferredFieldNumber;
        } else {
          fieldNumber = findNextAvailableFieldNumber(preferredFieldNumber + 1, numberToName.keySet());
        }

        numberToName.put(fieldNumber, fieldName);
        nameToNumber.put(fieldName, fieldNumber);
      }

      return fieldNumber;
    }

    synchronized void setIfNotSet(int fieldNumber, String fieldName) {
     if (!numberToName.containsKey(fieldNumber) && !nameToNumber.containsKey(fieldName)) {
        numberToName.put(fieldNumber, fieldName);
        nameToNumber.put(fieldName, fieldNumber);
      }
    }
  }

  private static final class FieldInfoBiMap implements Iterable<FieldInfo> {
    private final SortedMap<Integer,FieldInfo> byNumber = new TreeMap<Integer,FieldInfo>();
    private final HashMap<String,FieldInfo> byName = new HashMap<String,FieldInfo>();
    private int nextAvailableNumber = 0;

    public void put(FieldInfo fi) {
      assert !byNumber.containsKey(fi.number);
      assert !byName.containsKey(fi.name);

      byNumber.put(fi.number, fi);
      byName.put(fi.name, fi);
    }

    public FieldInfo get(String fieldName) {
      return byName.get(fieldName);
    }

    public FieldInfo get(int fieldNumber) {
      return byNumber.get(fieldNumber);
    }

    public int size() {
      assert byNumber.size() == byName.size();
      return byNumber.size();
    }

    public Iterator<FieldInfo> iterator() {
      return byNumber.values().iterator();
    }
  }

  // First used in 2.9; prior to 2.9 there was no format header
  public static final int FORMAT_START = -2;
  public static final int FORMAT_PER_FIELD_CODEC = -3;

  // whenever you add a new format, make it 1 smaller (negative version logic)!
  static final int FORMAT_CURRENT = FORMAT_PER_FIELD_CODEC;
  
  static final int FORMAT_MINIMUM = FORMAT_START;
  
  static final byte IS_INDEXED = 0x1;
  static final byte STORE_TERMVECTOR = 0x2;
  static final byte STORE_POSITIONS_WITH_TERMVECTOR = 0x4;
  static final byte STORE_OFFSET_WITH_TERMVECTOR = 0x8;
  static final byte OMIT_NORMS = 0x10;
  static final byte STORE_PAYLOADS = 0x20;
  static final byte OMIT_TERM_FREQ_AND_POSITIONS = 0x40;
  
  private final FieldNumberBiMap globalFieldNumbers;
  private final FieldInfoBiMap localFieldInfos;

  private int format;

  public FieldInfos() {
    this(new FieldNumberBiMap());
  }

  private FieldInfos(FieldNumberBiMap globalFieldNumbers) {
    this.globalFieldNumbers = globalFieldNumbers;
    this.localFieldInfos = new FieldInfoBiMap();
  }

  /**
   * Construct a FieldInfos object using the directory and the name of the file
   * IndexInput
   * @param d The directory to open the IndexInput from
   * @param name The name of the file to open the IndexInput from in the Directory
   * @throws IOException
   */
  public FieldInfos(Directory d, String name) throws IOException {
    this(new FieldNumberBiMap());
    IndexInput input = d.openInput(name);
    try {
      read(input, name);
    } finally {
      input.close();
    }
  }

  private static final int findNextAvailableFieldNumber(int nextPreferredNumber, Set<Integer> unavailableNumbers) {
    while (unavailableNumbers.contains(nextPreferredNumber)) {
      nextPreferredNumber++;
    }

    return nextPreferredNumber;
  }

  public FieldInfos newFieldInfosWithGlobalFieldNumberMap() {
    return new FieldInfos(this.globalFieldNumbers);
  }

  /**
   * Returns a deep clone of this FieldInfos instance.
   */
  @Override
  synchronized public Object clone() {
    FieldInfos fis = new FieldInfos(globalFieldNumbers);
    for (FieldInfo fi : this) {
      FieldInfo clone = (FieldInfo) (fi).clone();
      fis.localFieldInfos.put(clone);
    }
    return fis;
  }

  /** Adds field info for a Document. */
  synchronized public void add(Document doc) {
    List<Fieldable> fields = doc.getFields();
    for (Fieldable field : fields) {
      add(field.name(), field.isIndexed(), field.isTermVectorStored(), field.isStorePositionWithTermVector(),
              field.isStoreOffsetWithTermVector(), field.getOmitNorms(), false, field.getOmitTermFreqAndPositions());
    }
  }

  /** Returns true if any fields do not omitTermFreqAndPositions */
  public boolean hasProx() {
    for (FieldInfo fi : this) {
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
  synchronized public void addIndexed(Collection<String> names, boolean storeTermVectors, boolean storePositionWithTermVector, 
                         boolean storeOffsetWithTermVector) {
    for (String name : names) {
      add(name, true, storeTermVectors, storePositionWithTermVector, storeOffsetWithTermVector);
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
  synchronized public void add(Collection<String> names, boolean isIndexed) {
    for (String name : names) {
      add(name, isIndexed);
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
    return addOrUpdateInternal(name, -1, isIndexed, storeTermVector, storePositionWithTermVector,
                               storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions);
  }

  synchronized private FieldInfo addOrUpdateInternal(String name, int preferredFieldNumber, boolean isIndexed,
      boolean storeTermVector, boolean storePositionWithTermVector, boolean storeOffsetWithTermVector,
      boolean omitNorms, boolean storePayloads, boolean omitTermFreqAndPositions) {

    FieldInfo fi = fieldInfo(name);
    if (fi == null) {
      if (preferredFieldNumber == -1) {
        preferredFieldNumber = findNextAvailableFieldNumber(localFieldInfos.nextAvailableNumber, localFieldInfos.byNumber.keySet());
        localFieldInfos.nextAvailableNumber = preferredFieldNumber;
      }

      // get a global number for this field
      int fieldNumber = globalFieldNumbers.addOrGet(name, localFieldInfos, preferredFieldNumber);
      if (localFieldInfos.get(fieldNumber) != null) {
        // fall back if the global number is already taken
        fieldNumber = preferredFieldNumber;
      }
      return addInternal(name, fieldNumber, isIndexed, storeTermVector, storePositionWithTermVector, storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions);
    } else {
      fi.update(isIndexed, storeTermVector, storePositionWithTermVector, storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions);
    }
    return fi;
  }

  synchronized public FieldInfo add(FieldInfo fi) {
    int preferredFieldNumber = fi.number;
    FieldInfo other = localFieldInfos.get(preferredFieldNumber);
    if (other == null || !other.name.equals(fi.name)) {
      preferredFieldNumber = -1;
    }
    return addOrUpdateInternal(fi.name, preferredFieldNumber, fi.isIndexed, fi.storeTermVector,
               fi.storePositionWithTermVector, fi.storeOffsetWithTermVector,
               fi.omitNorms, fi.storePayloads,
               fi.omitTermFreqAndPositions);
  }

  private FieldInfo addInternal(String name, int fieldNumber, boolean isIndexed,
                                boolean storeTermVector, boolean storePositionWithTermVector, 
                                boolean storeOffsetWithTermVector, boolean omitNorms, boolean storePayloads, boolean omitTermFreqAndPositions) {
    name = StringHelper.intern(name);
    globalFieldNumbers.setIfNotSet(fieldNumber, name);
    FieldInfo fi = new FieldInfo(name, isIndexed, fieldNumber, storeTermVector, storePositionWithTermVector,
                                 storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions);

    assert localFieldInfos.get(fi.number) == null;
    localFieldInfos.put(fi);
    return fi;
  }

  public int fieldNumber(String fieldName) {
    FieldInfo fi = fieldInfo(fieldName);
    return (fi != null) ? fi.number : -1;
  }

  public FieldInfo fieldInfo(String fieldName) {
    return localFieldInfos.get(fieldName);
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
	return (fieldNumber >= 0) ? localFieldInfos.get(fieldNumber) : null;
  }

  public Iterator<FieldInfo> iterator() {
    return localFieldInfos.iterator();
  }

  public int size() {
    return localFieldInfos.size();
  }

  public boolean hasVectors() {
    for (FieldInfo fi : this) {
      if (fi.storeTermVector) {
        return true;
      }
    }
    return false;
  }

  void clearVectors() {
    for (FieldInfo fi : this) {
      fi.storeTermVector = false;
      fi.storeOffsetWithTermVector = false;
      fi.storePositionWithTermVector = false;
    }
  }

  public boolean hasNorms() {
    for (FieldInfo fi : this) {
      if (!fi.omitNorms) {
        return true;
      }
    }
    return false;
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
    output.writeVInt(FORMAT_CURRENT);
    output.writeVInt(size());
    for (FieldInfo fi : this) {
      byte bits = 0x0;
      if (fi.isIndexed) bits |= IS_INDEXED;
      if (fi.storeTermVector) bits |= STORE_TERMVECTOR;
      if (fi.storePositionWithTermVector) bits |= STORE_POSITIONS_WITH_TERMVECTOR;
      if (fi.storeOffsetWithTermVector) bits |= STORE_OFFSET_WITH_TERMVECTOR;
      if (fi.omitNorms) bits |= OMIT_NORMS;
      if (fi.storePayloads) bits |= STORE_PAYLOADS;
      if (fi.omitTermFreqAndPositions) bits |= OMIT_TERM_FREQ_AND_POSITIONS;
      output.writeString(fi.name);
      output.writeInt(fi.number);
      output.writeInt(fi.getCodecId());
      output.writeByte(bits);
    }
  }

  private void read(IndexInput input, String fileName) throws IOException {
    format = input.readVInt();

    if (format > FORMAT_MINIMUM) {
      throw new IndexFormatTooOldException(fileName, format, FORMAT_MINIMUM, FORMAT_CURRENT);
    }
    if (format < FORMAT_CURRENT) {
      throw new IndexFormatTooNewException(fileName, format, FORMAT_MINIMUM, FORMAT_CURRENT);
    }

    final int size = input.readVInt(); //read in the size

    for (int i = 0; i < size; i++) {
      String name = StringHelper.intern(input.readString());
      // if this is a previous format codec 0 will be preflex!
      final int fieldNumber = format <= FORMAT_PER_FIELD_CODEC? input.readInt():i;
      final int codecId = format <= FORMAT_PER_FIELD_CODEC? input.readInt():0;
      byte bits = input.readByte();
      boolean isIndexed = (bits & IS_INDEXED) != 0;
      boolean storeTermVector = (bits & STORE_TERMVECTOR) != 0;
      boolean storePositionsWithTermVector = (bits & STORE_POSITIONS_WITH_TERMVECTOR) != 0;
      boolean storeOffsetWithTermVector = (bits & STORE_OFFSET_WITH_TERMVECTOR) != 0;
      boolean omitNorms = (bits & OMIT_NORMS) != 0;
      boolean storePayloads = (bits & STORE_PAYLOADS) != 0;
      boolean omitTermFreqAndPositions = (bits & OMIT_TERM_FREQ_AND_POSITIONS) != 0;
      final FieldInfo addInternal = addInternal(name, fieldNumber, isIndexed, storeTermVector, storePositionsWithTermVector, storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions);
      addInternal.setCodecId(codecId);
    }

    if (input.getFilePointer() != input.length()) {
      throw new CorruptIndexException("did not read all bytes from file \"" + fileName + "\": read " + input.getFilePointer() + " vs size " + input.length());
    }    
  }

}
