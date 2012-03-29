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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.index.FieldInfo.IndexOptions;

/** 
 * Collection of {@link FieldInfo}s (accessible by number or by name).
 *  @lucene.experimental
 */
public final class FieldInfos implements Iterable<FieldInfo> {
  static final class FieldNumberBiMap {
    
    private final Map<Integer,String> numberToName;
    private final Map<String,Integer> nameToNumber;
    private int lowestUnassignedFieldNumber = -1;
    
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
        numberToName.put(boxedFieldNumber, fieldName);
        nameToNumber.put(fieldName, boxedFieldNumber);
      } else {
        assert containsConsistent(boxedFieldNumber, fieldName);
      }
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
  
  private boolean hasFreq; // only set if readonly
  private boolean hasProx; // only set if readonly
  private boolean hasVectors; // only set if readonly
  private long version; // internal use to track changes

  /**
   * Creates a new read-only FieldInfos: only public to be accessible
   * from the codecs package
   * 
   * @lucene.internal
   */
  public FieldInfos(FieldInfo[] infos, boolean hasFreq, boolean hasProx, boolean hasVectors) {
    this(null);
    this.hasFreq = hasFreq;
    this.hasProx = hasProx;
    this.hasVectors = hasVectors;
    for (FieldInfo info : infos) {
      putInternal(info);
    }
  }

  public FieldInfos() {
    this(new FieldNumberBiMap());
  }

  public void add(FieldInfos other) {
    for(FieldInfo fieldInfo : other){ 
      add(fieldInfo);
    }
  }

  /**
   * Creates a new FieldInfos instance with the given {@link FieldNumberBiMap}. 
   * If the {@link FieldNumberBiMap} is <code>null</code> this instance will be read-only.
   * @see #isReadOnly()
   */
  FieldInfos(FieldNumberBiMap globalFieldNumbers) {
    this.globalFieldNumbers = globalFieldNumbers;
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
  synchronized public FieldInfos clone() {
    FieldInfos fis = new FieldInfos(globalFieldNumbers);
    fis.hasFreq = hasFreq;
    fis.hasProx = hasProx;
    fis.hasVectors = hasVectors;
    for (FieldInfo fi : this) {
      FieldInfo clone = fi.clone();
      fis.putInternal(clone);
    }
    return fis;
  }

  /** Returns true if any fields have positions */
  public boolean hasProx() {
    if (isReadOnly()) {
      return hasProx;
    }
    // mutable FIs must check!
    for (FieldInfo fi : this) {
      if (fi.isIndexed && fi.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
        return true;
      }
    }
    return false;
  }
  
  /** Returns true if any fields have freqs */
  public boolean hasFreq() {
    if (isReadOnly()) {
      return hasFreq;
    }
    // mutable FIs must check!
    for (FieldInfo fi : this) {
      if (fi.isIndexed && fi.indexOptions != IndexOptions.DOCS_ONLY) {
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
   */
  synchronized public void addOrUpdateIndexed(Collection<String> names, boolean storeTermVectors) {
    for (String name : names) {
      addOrUpdate(name, true, storeTermVectors);
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
   * @param name The name of the IndexableField
   * @param isIndexed true if the field is indexed
   * @see #addOrUpdate(String, boolean, boolean)
   */
  synchronized public void addOrUpdate(String name, boolean isIndexed) {
    addOrUpdate(name, isIndexed, false, false);
  }

  /** If the field is not yet known, adds it. If it is known, checks to make
   *  sure that the isIndexed flag is the same as was given previously for this
   *  field. If not - marks it as being indexed.  Same goes for the TermVector
   * parameters.
   * 
   * @param name The name of the field
   * @param isIndexed true if the field is indexed
   * @param storeTermVector true if the term vector should be stored
   */
  synchronized public void addOrUpdate(String name, boolean isIndexed, boolean storeTermVector) {
    addOrUpdate(name, isIndexed, storeTermVector, false);
  }

    /** If the field is not yet known, adds it. If it is known, checks to make
   *  sure that the isIndexed flag is the same as was given previously for this
   *  field. If not - marks it as being indexed.  Same goes for the TermVector
   * parameters.
   *
   * @param name The name of the field
   * @param isIndexed true if the field is indexed
   * @param storeTermVector true if the term vector should be stored
   * @param omitNorms true if the norms for the indexed field should be omitted
   */
  synchronized public void addOrUpdate(String name, boolean isIndexed, boolean storeTermVector,
                  boolean omitNorms) {
    addOrUpdate(name, isIndexed, storeTermVector, omitNorms, false, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, null, null);
  }
  
  /** If the field is not yet known, adds it. If it is known, checks to make
   *  sure that the isIndexed flag is the same as was given previously for this
   *  field. If not - marks it as being indexed.  Same goes for the TermVector
   * parameters.
   *
   * @param name The name of the field
   * @param isIndexed true if the field is indexed
   * @param storeTermVector true if the term vector should be stored
   * @param omitNorms true if the norms for the indexed field should be omitted
   * @param storePayloads true if payloads should be stored for this field
   * @param indexOptions if term freqs should be omitted for this field
   */
  synchronized public FieldInfo addOrUpdate(String name, boolean isIndexed, boolean storeTermVector,
                       boolean omitNorms, boolean storePayloads, IndexOptions indexOptions, DocValues.Type docValues, DocValues.Type normType) {
    return addOrUpdateInternal(name, -1, isIndexed, storeTermVector, omitNorms, storePayloads, indexOptions, docValues, normType);
  }

  // NOTE: this method does not carry over termVector
  // booleans nor docValuesType; the indexer chain
  // (TermVectorsConsumerPerField, DocFieldProcessor) must
  // set these fields when they succeed in consuming
  // the document:
  public FieldInfo addOrUpdate(String name, IndexableFieldType fieldType) {
    // TODO: really, indexer shouldn't even call this
    // method (it's only called from DocFieldProcessor);
    // rather, each component in the chain should update
    // what it "owns".  EG fieldType.indexOptions() should
    // be updated by maybe FreqProxTermsWriterPerField:
    return addOrUpdateInternal(name, -1, fieldType.indexed(), false,
                               fieldType.omitNorms(), false,
                               fieldType.indexOptions(), null, null);
  }

  synchronized private FieldInfo addOrUpdateInternal(String name, int preferredFieldNumber, boolean isIndexed,
      boolean storeTermVector,
      boolean omitNorms, boolean storePayloads, IndexOptions indexOptions, DocValues.Type docValues, DocValues.Type normType) {
    if (globalFieldNumbers == null) {
      throw new IllegalStateException("FieldInfos are read-only, create a new instance with a global field map to make modifications to FieldInfos");
    }
    FieldInfo fi = fieldInfo(name);
    if (fi == null) {
      final int fieldNumber = nextFieldNumber(name, preferredFieldNumber);
      fi = addInternal(name, fieldNumber, isIndexed, storeTermVector, omitNorms, storePayloads, indexOptions, docValues, normType);
    } else {
      fi.update(isIndexed, storeTermVector, omitNorms, storePayloads, indexOptions);
      if (docValues != null) {
        fi.setDocValuesType(docValues, true);
      }
      if (normType != null) {
        fi.setNormValueType(normType, true);
      }
    }
    version++;
    return fi;
  }
  
  synchronized public FieldInfo add(FieldInfo fi) {
    // IMPORTANT - reuse the field number if possible for consistent field numbers across segments
    return addOrUpdateInternal(fi.name, fi.number, fi.isIndexed, fi.storeTermVector,
               fi.omitNorms, fi.storePayloads,
               fi.indexOptions, fi.getDocValuesType(), fi.getNormType());
  }
  
  /*
   * NOTE: if you call this method from a public method make sure you check if we are modifiable and throw an exception otherwise
   */
  private FieldInfo addInternal(String name, int fieldNumber, boolean isIndexed,
                                boolean storeTermVector, boolean omitNorms, boolean storePayloads,
                                IndexOptions indexOptions, DocValues.Type docValuesType, DocValues.Type normType) {
    // don't check modifiable here since we use that to initially build up FIs
    if (globalFieldNumbers != null) {
      globalFieldNumbers.setIfNotSet(fieldNumber, name);
    } 
    final FieldInfo fi = new FieldInfo(name, isIndexed, fieldNumber, storeTermVector, omitNorms, storePayloads, indexOptions, docValuesType, normType);
    putInternal(fi);
    return fi;
  }

  /**
   * lookup the number of a field by name.
   * 
   * @param fieldName field's name
   * @return number of field, or -1 if it does not exist.
   */
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

  /**
   * @return number of fields
   */
  public int size() {
    assert byNumber.size() == byName.size();
    return byNumber.size();
  }

  /**
   * @return true if at least one field has any vectors
   */
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

  /**
   * @return true if at least one field has any norms
   */
  public boolean hasNorms() {
    for (FieldInfo fi : this) {
      if (fi.hasNorms()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns <code>true</code> iff this instance is not backed by a
   * {@link org.apache.lucene.index.FieldInfos.FieldNumberBiMap}. Instances read from a directory via
   * {@link FieldInfos#FieldInfos(FieldInfo[], boolean, boolean, boolean)} will always be read-only
   * since no {@link org.apache.lucene.index.FieldInfos.FieldNumberBiMap} is supplied, otherwise 
   * <code>false</code>.
   */
  public final boolean isReadOnly() {
    return globalFieldNumbers == null;
  }
  
  synchronized final long getVersion() {
    return version;
  }
  
  final FieldInfos asReadOnly() {
    if (isReadOnly()) {
      return this;
    }
    final FieldInfos roFis = new FieldInfos((FieldNumberBiMap)null);
    for (FieldInfo fieldInfo : this) {
      FieldInfo clone = fieldInfo.clone();
      roFis.putInternal(clone);
      roFis.hasVectors |= clone.storeTermVector;
      roFis.hasProx |= clone.isIndexed && clone.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      roFis.hasFreq |= clone.isIndexed && clone.indexOptions != IndexOptions.DOCS_ONLY;
    }
    return roFis;
  }

  /**
   * @return true if at least one field has docValues
   */
  public boolean hasDocValues() {
    for (FieldInfo fi : this) {
      if (fi.hasDocValues()) { 
        return true;
      }
    }

    return false;
  }
  
  /**
   * Creates a new {@link FieldInfo} instance from the given instance. If the given instance is
   * read-only this instance will be read-only too.
   * 
   * @see #isReadOnly()
   */
  static FieldInfos from(FieldInfos other) {
    return new FieldInfos(other.globalFieldNumbers);
  }
}
