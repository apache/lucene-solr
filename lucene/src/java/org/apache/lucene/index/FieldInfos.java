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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.lucene.index.SegmentCodecs; // Required for Java 1.5 javadocs
import org.apache.lucene.index.SegmentCodecs.SegmentCodecsBuilder;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.values.ValueType;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.StringHelper;

/** Access to the Fieldable Info file that describes document fields and whether or
 *  not they are indexed. Each segment has a separate Fieldable Info file. Objects
 *  of this class are thread-safe for multiple readers, but only one thread can
 *  be adding documents at a time, with no other reader or writer threads
 *  accessing this object.
 *  @lucene.experimental
 */
public final class FieldInfos implements Iterable<FieldInfo> {
  static final class FieldNumberBiMap {
    
    final static String CODEC_NAME = "GLOBAL_FIELD_MAP";
    
    // Initial format
    private static final int VERSION_START = 0;

    private static final int VERSION_CURRENT = VERSION_START;

    private final Map<Integer,String> numberToName;
    private final Map<String,Integer> nameToNumber;
    private int lowestUnassignedFieldNumber = -1;
    private long lastVersion = 0;
    private long version = 0;
    
    FieldNumberBiMap() {
      this.nameToNumber = new HashMap<String, Integer>();
      this.numberToName = new HashMap<Integer, String>();
    }
    
    /**
     * Returns the global field number for the given field name. If the name
     * does not exist yet it tries to add it with the given preferred field
     * number assigned if possible otherwise the first unassigned field number
     * is used as the field number.
     */
    synchronized int addOrGet(String fieldName, int preferredFieldNumber) {
      Integer fieldNumber = nameToNumber.get(fieldName);
      if (fieldNumber == null) {
        final Integer preferredBoxed = Integer.valueOf(preferredFieldNumber);

        if (preferredFieldNumber != -1 && !numberToName.containsKey(preferredBoxed)) {
            // cool - we can use this number globally
            fieldNumber = preferredBoxed;
        } else {
          // find a new FieldNumber
          while (numberToName.containsKey(++lowestUnassignedFieldNumber)) {
            // might not be up to date - lets do the work once needed
          }
          fieldNumber = lowestUnassignedFieldNumber;
        }
        
        version++;
        numberToName.put(fieldNumber, fieldName);
        nameToNumber.put(fieldName, fieldNumber);
        
      }

      return fieldNumber.intValue();
    }

    /**
     * Sets the given field number and name if not yet set. 
     */
    synchronized void setIfNotSet(int fieldNumber, String fieldName) {
      final Integer boxedFieldNumber = Integer.valueOf(fieldNumber);
      if (!numberToName.containsKey(boxedFieldNumber)
          && !nameToNumber.containsKey(fieldName)) {
        version++;
        numberToName.put(boxedFieldNumber, fieldName);
        nameToNumber.put(fieldName, boxedFieldNumber);
      } else {
        assert containsConsistent(boxedFieldNumber, fieldName);
      }
    }
    
    /**
     * Writes this {@link FieldNumberBiMap} to the given output and returns its
     * version.
     */
    public synchronized long write(IndexOutput output) throws IOException{
      Set<Entry<String, Integer>> entrySet = nameToNumber.entrySet();
      CodecUtil.writeHeader(output, CODEC_NAME, VERSION_CURRENT); 
      output.writeVInt(entrySet.size());
      for (Entry<String, Integer> entry : entrySet) {
        output.writeVInt(entry.getValue().intValue());
        output.writeString(entry.getKey());
      }
      return version;
    }

    /**
     * Reads the {@link FieldNumberBiMap} from the given input and resets the
     * version to 0.
     */
    public synchronized void read(IndexInput input) throws IOException{
      CodecUtil.checkHeader(input, CODEC_NAME,
          VERSION_START,
          VERSION_CURRENT);
      final int size = input.readVInt();
      for (int i = 0; i < size; i++) {
        final int num = input.readVInt();
        final String name = input.readString();
        setIfNotSet(num, name);
      }
      version = lastVersion = 0;
    }
    
    /**
     * Returns a new {@link FieldInfos} instance with this as the global field
     * map
     * 
     * @return a new {@link FieldInfos} instance with this as the global field
     *         map
     */
    public FieldInfos newFieldInfos(SegmentCodecsBuilder segmentCodecsBuilder) {
      return new FieldInfos(this, segmentCodecsBuilder);
    }

    /**
     * Returns <code>true</code> iff the last committed version differs from the
     * current version, otherwise <code>false</code>
     * 
     * @return <code>true</code> iff the last committed version differs from the
     *         current version, otherwise <code>false</code>
     */
    public synchronized boolean isDirty() {
      return lastVersion != version;
    }
    
    /**
     * commits the given version if the given version is greater than the previous committed version
     * 
     * @param version
     *          the version to commit
     * @return <code>true</code> iff the version was successfully committed otherwise <code>false</code>
     * @see #write(IndexOutput)
     */
    public synchronized boolean commitLastVersion(long version) {
      if (version > lastVersion) {
        lastVersion = version;
        return true;
      }
      return false;
    }
    
    // just for testing
    Set<Entry<String, Integer>> entries() {
      return new HashSet<Entry<String, Integer>>(nameToNumber.entrySet());
    }
    
    // used by assert
    synchronized boolean containsConsistent(Integer number, String name) {
      return name.equals(numberToName.get(number))
          && number.equals(nameToNumber.get(name));
    }
  }
  
  private final SortedMap<Integer,FieldInfo> byNumber = new TreeMap<Integer,FieldInfo>();
  private final HashMap<String,FieldInfo> byName = new HashMap<String,FieldInfo>();
  private final FieldNumberBiMap globalFieldNumbers;
  private final SegmentCodecsBuilder segmentCodecsBuilder;
  
  // First used in 2.9; prior to 2.9 there was no format header
  public static final int FORMAT_START = -2;
  public static final int FORMAT_PER_FIELD_CODEC = -3;

  // Records index values for this field
  public static final int FORMAT_INDEX_VALUES = -3;

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

  private int format;
  private boolean hasProx; // only set if readonly
  private boolean hasVectors; // only set if readonly
  private long version; // internal use to track changes
  

  /**
   * Creates a new {@link FieldInfos} instance with a private
   * {@link org.apache.lucene.index.FieldInfos.FieldNumberBiMap} and a default {@link SegmentCodecsBuilder}
   * initialized with {@link CodecProvider#getDefault()}.
   * <p>
   * Note: this ctor should not be used during indexing use
   * {@link FieldInfos#FieldInfos(FieldInfos)} or
   * {@link FieldInfos#FieldInfos(FieldNumberBiMap,org.apache.lucene.index.SegmentCodecs.SegmentCodecsBuilder)}
   * instead.
   */
  public FieldInfos() {
    this(new FieldNumberBiMap(), SegmentCodecsBuilder.create(CodecProvider.getDefault()));
  }
  
  /**
   * Creates a new {@link FieldInfo} instance from the given instance. If the given instance is
   * read-only this instance will be read-only too.
   * 
   * @see #isReadOnly()
   */
  FieldInfos(FieldInfos other) {
    this(other.globalFieldNumbers, other.segmentCodecsBuilder);
  }
  
  /**
   * Creates a new FieldInfos instance with the given {@link FieldNumberBiMap}. 
   * If the {@link FieldNumberBiMap} is <code>null</code> this instance will be read-only.
   * @see #isReadOnly()
   */
  FieldInfos(FieldNumberBiMap globalFieldNumbers, SegmentCodecsBuilder segmentCodecsBuilder) {
    this.globalFieldNumbers = globalFieldNumbers;
    this.segmentCodecsBuilder = segmentCodecsBuilder;
  }

  /**
   * Construct a FieldInfos object using the directory and the name of the file
   * IndexInput. 
   * <p>
   * Note: The created instance will be read-only
   * 
   * @param d The directory to open the IndexInput from
   * @param name The name of the file to open the IndexInput from in the Directory
   * @throws IOException
   */
  public FieldInfos(Directory d, String name) throws IOException {
    this((FieldNumberBiMap)null, null); // use null here to make this FIs Read-Only
    final IndexInput input = d.openInput(name, IOContext.READONCE);
    try {
      read(input, name);
    } finally {
      input.close();
    }
  }
  
  /**
   * adds the given field to this FieldInfos name / number mapping. The given FI
   * must be present in the global field number mapping before this method it
   * called
   */
  private void putInternal(FieldInfo fi) {
    assert !byNumber.containsKey(fi.number);
    assert !byName.containsKey(fi.name);
    assert globalFieldNumbers == null || globalFieldNumbers.containsConsistent(Integer.valueOf(fi.number), fi.name);
    byNumber.put(fi.number, fi);
    byName.put(fi.name, fi);
  }
  
  private int nextFieldNumber(String name, int preferredFieldNumber) {
    // get a global number for this field
    final int fieldNumber = globalFieldNumbers.addOrGet(name,
        preferredFieldNumber);
    assert byNumber.get(fieldNumber) == null : "field number " + fieldNumber
        + " already taken";
    return fieldNumber;
  }

  /**
   * Returns a deep clone of this FieldInfos instance.
   */
  @Override
  synchronized public Object clone() {
    FieldInfos fis = new FieldInfos(globalFieldNumbers, segmentCodecsBuilder);
    fis.format = format;
    fis.hasProx = hasProx;
    fis.hasVectors = hasVectors;
    for (FieldInfo fi : this) {
      FieldInfo clone = (FieldInfo) (fi).clone();
      fis.putInternal(clone);
    }
    return fis;
  }

  /** Returns true if any fields do not omitTermFreqAndPositions */
  public boolean hasProx() {
    if (isReadOnly()) {
      return hasProx;
    }
    // mutable FIs must check!
    for (FieldInfo fi : this) {
      if (fi.isIndexed && !fi.omitTermFreqAndPositions) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Adds or updates fields that are indexed. Whether they have termvectors has to be specified.
   * 
   * @param names The names of the fields
   * @param storeTermVectors Whether the fields store term vectors or not
   * @param storePositionWithTermVector true if positions should be stored.
   * @param storeOffsetWithTermVector true if offsets should be stored
   */
  synchronized public void addOrUpdateIndexed(Collection<String> names, boolean storeTermVectors, boolean storePositionWithTermVector, 
                         boolean storeOffsetWithTermVector) {
    for (String name : names) {
      addOrUpdate(name, true, storeTermVectors, storePositionWithTermVector, storeOffsetWithTermVector);
    }
  }

  /**
   * Assumes the fields are not storing term vectors.
   * 
   * @param names The names of the fields
   * @param isIndexed Whether the fields are indexed or not
   * 
   * @see #addOrUpdate(String, boolean)
   */
  synchronized public void addOrUpdate(Collection<String> names, boolean isIndexed) {
    for (String name : names) {
      addOrUpdate(name, isIndexed);
    }
  }

  /**
   * Calls 5 parameter add with false for all TermVector parameters.
   * 
   * @param name The name of the Fieldable
   * @param isIndexed true if the field is indexed
   * @see #addOrUpdate(String, boolean, boolean, boolean, boolean)
   */
  synchronized public void addOrUpdate(String name, boolean isIndexed) {
    addOrUpdate(name, isIndexed, false, false, false, false);
  }

  /**
   * Calls 5 parameter add with false for term vector positions and offsets.
   * 
   * @param name The name of the field
   * @param isIndexed  true if the field is indexed
   * @param storeTermVector true if the term vector should be stored
   */
  synchronized public void addOrUpdate(String name, boolean isIndexed, boolean storeTermVector){
    addOrUpdate(name, isIndexed, storeTermVector, false, false, false);
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
  synchronized public void addOrUpdate(String name, boolean isIndexed, boolean storeTermVector,
                  boolean storePositionWithTermVector, boolean storeOffsetWithTermVector) {

    addOrUpdate(name, isIndexed, storeTermVector, storePositionWithTermVector, storeOffsetWithTermVector, false);
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
  synchronized public void addOrUpdate(String name, boolean isIndexed, boolean storeTermVector,
                  boolean storePositionWithTermVector, boolean storeOffsetWithTermVector, boolean omitNorms) {
    addOrUpdate(name, isIndexed, storeTermVector, storePositionWithTermVector,
        storeOffsetWithTermVector, omitNorms, false, false, null);
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
  synchronized public FieldInfo addOrUpdate(String name, boolean isIndexed, boolean storeTermVector,
                       boolean storePositionWithTermVector, boolean storeOffsetWithTermVector,
                       boolean omitNorms, boolean storePayloads, boolean omitTermFreqAndPositions, ValueType docValues) {
    return addOrUpdateInternal(name, -1, isIndexed, storeTermVector, storePositionWithTermVector,
                               storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions, docValues);
  }

  synchronized private FieldInfo addOrUpdateInternal(String name, int preferredFieldNumber, boolean isIndexed,
      boolean storeTermVector, boolean storePositionWithTermVector, boolean storeOffsetWithTermVector,
      boolean omitNorms, boolean storePayloads, boolean omitTermFreqAndPositions, ValueType docValues) {
    if (globalFieldNumbers == null) {
      throw new IllegalStateException("FieldInfos are read-only, create a new instance with a global field map to make modifications to FieldInfos");
    }
    assert segmentCodecsBuilder != null : "SegmentCodecsBuilder is set to null but FieldInfos is not read-only";
    FieldInfo fi = fieldInfo(name);
    if (fi == null) {
      final int fieldNumber = nextFieldNumber(name, preferredFieldNumber);
      fi = addInternal(name, fieldNumber, isIndexed, storeTermVector, storePositionWithTermVector, storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions, docValues);
    } else {
      fi.update(isIndexed, storeTermVector, storePositionWithTermVector, storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions);
      fi.setDocValues(docValues);
    }
    if ((fi.isIndexed || fi.hasDocValues()) && fi.getCodecId() == FieldInfo.UNASSIGNED_CODEC_ID) {
      segmentCodecsBuilder.tryAddAndSet(fi);
    }
    version++;
    return fi;
  }

  synchronized public FieldInfo add(FieldInfo fi) {
    // IMPORTANT - reuse the field number if possible for consistent field numbers across segments
    return addOrUpdateInternal(fi.name, fi.number, fi.isIndexed, fi.storeTermVector,
               fi.storePositionWithTermVector, fi.storeOffsetWithTermVector,
               fi.omitNorms, fi.storePayloads,
               fi.omitTermFreqAndPositions, fi.docValues);
  }
  
  /*
   * NOTE: if you call this method from a public method make sure you check if we are modifiable and throw an exception otherwise
   */
  private FieldInfo addInternal(String name, int fieldNumber, boolean isIndexed,
                                boolean storeTermVector, boolean storePositionWithTermVector, 
                                boolean storeOffsetWithTermVector, boolean omitNorms, boolean storePayloads, boolean omitTermFreqAndPositions, ValueType docValuesType) {
    // don't check modifiable here since we use that to initially build up FIs
    name = StringHelper.intern(name);
    if (globalFieldNumbers != null) {
      globalFieldNumbers.setIfNotSet(fieldNumber, name);
    } 
    final FieldInfo fi = new FieldInfo(name, isIndexed, fieldNumber, storeTermVector, storePositionWithTermVector,
                                 storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions, docValuesType);
    putInternal(fi);
    return fi;
  }

  public int fieldNumber(String fieldName) {
    FieldInfo fi = fieldInfo(fieldName);
    return (fi != null) ? fi.number : -1;
  }

  public FieldInfo fieldInfo(String fieldName) {
    return byName.get(fieldName);
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
	return (fieldNumber >= 0) ? byNumber.get(fieldNumber) : null;
  }

  public Iterator<FieldInfo> iterator() {
    return byNumber.values().iterator();
  }

  public int size() {
    assert byNumber.size() == byName.size();
    return byNumber.size();
  }

  public boolean hasVectors() {
    if (isReadOnly()) {
      return hasVectors;
    }
    // mutable FIs must check
    for (FieldInfo fi : this) {
      if (fi.storeTermVector) {
        return true;
      }
    }
    return false;
  }

  public boolean hasNorms() {
    for (FieldInfo fi : this) {
      if (!fi.omitNorms) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Builds the {@link SegmentCodecs} mapping for this {@link FieldInfos} instance.
   * @param clearBuilder <code>true</code> iff the internal {@link SegmentCodecsBuilder} must be cleared otherwise <code>false</code>
   */
  public SegmentCodecs buildSegmentCodecs(boolean clearBuilder) {
    if (globalFieldNumbers == null) {
      throw new IllegalStateException("FieldInfos are read-only no SegmentCodecs available");
    }
    assert segmentCodecsBuilder != null;
    final SegmentCodecs segmentCodecs = segmentCodecsBuilder.build();
    if (clearBuilder) {
      segmentCodecsBuilder.clear();
    }
    return segmentCodecs;
  }

  public void write(Directory d, String name) throws IOException {
    IndexOutput output = d.createOutput(name, IOContext.READONCE);
    try {
      write(output);
    } finally {
      output.close();
    }
  }
  
  /**
   * Returns <code>true</code> iff this instance is not backed by a
   * {@link org.apache.lucene.index.FieldInfos.FieldNumberBiMap}. Instances read from a directory via
   * {@link FieldInfos#FieldInfos(Directory, String)} will always be read-only
   * since no {@link org.apache.lucene.index.FieldInfos.FieldNumberBiMap} is supplied, otherwise 
   * <code>false</code>.
   */
  public final boolean isReadOnly() {
    return globalFieldNumbers == null;
  }
  
  synchronized final long getVersion() {
    return version;
  }

  public void write(IndexOutput output) throws IOException {
    output.writeVInt(FORMAT_CURRENT);
    output.writeVInt(size());
    for (FieldInfo fi : this) {
      assert !fi.omitTermFreqAndPositions || !fi.storePayloads;
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

      final byte b;

      if (fi.docValues == null) {
        b = 0;
      } else {
        switch(fi.docValues) {
        case INTS:
          b = 1;
          break;
        case FLOAT_32:
          b = 2;
          break;
        case FLOAT_64:
          b = 3;
          break;
        case BYTES_FIXED_STRAIGHT:
          b = 4;
          break;
        case BYTES_FIXED_DEREF:
          b = 5;
          break;
        case BYTES_FIXED_SORTED:
          b = 6;
          break;
        case BYTES_VAR_STRAIGHT:
          b = 7;
          break;
        case BYTES_VAR_DEREF:
          b = 8;
          break;
        case BYTES_VAR_SORTED:
          b = 9;
          break;
        default:
          throw new IllegalStateException("unhandled indexValues type " + fi.docValues);
        }
      }
      output.writeByte(b);
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

      // LUCENE-3027: past indices were able to write
      // storePayloads=true when omitTFAP is also true,
      // which is invalid.  We correct that, here:
      if (omitTermFreqAndPositions) {
        storePayloads = false;
      }
      hasVectors |= storeTermVector;
      hasProx |= isIndexed && !omitTermFreqAndPositions;
      ValueType docValuesType = null;
      if (format <= FORMAT_INDEX_VALUES) {
        final byte b = input.readByte();
        switch(b) {
        case 0:
          docValuesType = null;
          break;
        case 1:
          docValuesType = ValueType.INTS;
          break;
        case 2:
          docValuesType = ValueType.FLOAT_32;
          break;
        case 3:
          docValuesType = ValueType.FLOAT_64;
          break;
        case 4:
          docValuesType = ValueType.BYTES_FIXED_STRAIGHT;
          break;
        case 5:
          docValuesType = ValueType.BYTES_FIXED_DEREF;
          break;
        case 6:
          docValuesType = ValueType.BYTES_FIXED_SORTED;
          break;
        case 7:
          docValuesType = ValueType.BYTES_VAR_STRAIGHT;
          break;
        case 8:
          docValuesType = ValueType.BYTES_VAR_DEREF;
          break;
        case 9:
          docValuesType = ValueType.BYTES_VAR_SORTED;
          break;
        default:
          throw new IllegalStateException("unhandled indexValues type " + b);
        }
      }
      final FieldInfo addInternal = addInternal(name, fieldNumber, isIndexed, storeTermVector, storePositionsWithTermVector, storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions, docValuesType);
      addInternal.setCodecId(codecId);
    }

    if (input.getFilePointer() != input.length()) {
      throw new CorruptIndexException("did not read all bytes from file \"" + fileName + "\": read " + input.getFilePointer() + " vs size " + input.length());
    }    
  }
  
  /**
   * Reverts all uncommitted changes 
   * @see FieldInfo#revertUncommitted()
   */
  void revertUncommitted() {
    for (FieldInfo fieldInfo : this) {
      fieldInfo.revertUncommitted();
    }
  }
  
  final FieldInfos asReadOnly() {
    if (isReadOnly()) {
      return this;
    }
    final FieldInfos roFis = new FieldInfos((FieldNumberBiMap)null, null);
    for (FieldInfo fieldInfo : this) {
      FieldInfo clone = (FieldInfo) (fieldInfo).clone();
      roFis.putInternal(clone);
      roFis.hasVectors |= clone.storeTermVector;
      roFis.hasProx |= clone.isIndexed && !clone.omitTermFreqAndPositions;
    }
    return roFis;
  }
  
}
