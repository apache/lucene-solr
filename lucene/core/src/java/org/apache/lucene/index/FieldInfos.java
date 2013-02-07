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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;

/** 
 * Collection of {@link FieldInfo}s (accessible by number or by name).
 *  @lucene.experimental
 */
public class FieldInfos implements Iterable<FieldInfo> {
  private final boolean hasFreq;
  private final boolean hasProx;
  private final boolean hasPayloads;
  private final boolean hasOffsets;
  private final boolean hasVectors;
  private final boolean hasNorms;
  private final boolean hasDocValues;
  
  private final SortedMap<Integer,FieldInfo> byNumber = new TreeMap<Integer,FieldInfo>();
  private final HashMap<String,FieldInfo> byName = new HashMap<String,FieldInfo>();
  private final Collection<FieldInfo> values; // for an unmodifiable iterator
  
  /**
   * Constructs a new FieldInfos from an array of FieldInfo objects
   */
  public FieldInfos(FieldInfo[] infos) {
    boolean hasVectors = false;
    boolean hasProx = false;
    boolean hasPayloads = false;
    boolean hasOffsets = false;
    boolean hasFreq = false;
    boolean hasNorms = false;
    boolean hasDocValues = false;
    
    for (FieldInfo info : infos) {
      FieldInfo previous = byNumber.put(info.number, info);
      if (previous != null) {
        throw new IllegalArgumentException("duplicate field numbers: " + previous.name + " and " + info.name + " have: " + info.number);
      }
      previous = byName.put(info.name, info);
      if (previous != null) {
        throw new IllegalArgumentException("duplicate field names: " + previous.number + " and " + info.number + " have: " + info.name);
      }
      
      hasVectors |= info.hasVectors();
      hasProx |= info.isIndexed() && info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      hasFreq |= info.isIndexed() && info.getIndexOptions() != IndexOptions.DOCS_ONLY;
      hasOffsets |= info.isIndexed() && info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      hasNorms |= info.hasNorms();
      hasDocValues |= info.hasDocValues();
      hasPayloads |= info.hasPayloads();
    }
    
    this.hasVectors = hasVectors;
    this.hasProx = hasProx;
    this.hasPayloads = hasPayloads;
    this.hasOffsets = hasOffsets;
    this.hasFreq = hasFreq;
    this.hasNorms = hasNorms;
    this.hasDocValues = hasDocValues;
    this.values = Collections.unmodifiableCollection(byNumber.values());
  }
  
  /** Returns true if any fields have freqs */
  public boolean hasFreq() {
    return hasFreq;
  }
  
  /** Returns true if any fields have positions */
  public boolean hasProx() {
    return hasProx;
  }

  /** Returns true if any fields have payloads */
  public boolean hasPayloads() {
    return hasPayloads;
  }

  /** Returns true if any fields have offsets */
  public boolean hasOffsets() {
    return hasOffsets;
  }
  
  /** Returns true if any fields have vectors */
  public boolean hasVectors() {
    return hasVectors;
  }
  
  /** Returns true if any fields have norms */
  public boolean hasNorms() {
    return hasNorms;
  }
  
  /** Returns true if any fields have DocValues */
  public boolean hasDocValues() {
    return hasDocValues;
  }
  
  /** Returns the number of fields */
  public int size() {
    assert byNumber.size() == byName.size();
    return byNumber.size();
  }
  
  /**
   * Returns an iterator over all the fieldinfo objects present,
   * ordered by ascending field number
   */
  // TODO: what happens if in fact a different order is used?
  @Override
  public Iterator<FieldInfo> iterator() {
    return values.iterator();
  }

  /**
   * Return the fieldinfo object referenced by the field name
   * @return the FieldInfo object or null when the given fieldName
   * doesn't exist.
   */  
  public FieldInfo fieldInfo(String fieldName) {
    return byName.get(fieldName);
  }

  /**
   * Return the fieldinfo object referenced by the fieldNumber.
   * @param fieldNumber field's number. if this is negative, this method
   *        always returns null.
   * @return the FieldInfo object or null when the given fieldNumber
   * doesn't exist.
   */  
  // TODO: fix this negative behavior, this was something related to Lucene3x?
  // if the field name is empty, i think it writes the fieldNumber as -1
  public FieldInfo fieldInfo(int fieldNumber) {
    return (fieldNumber >= 0) ? byNumber.get(fieldNumber) : null;
  }
  
  static final class FieldNumbers {
    
    private final Map<Integer,String> numberToName;
    private final Map<String,Integer> nameToNumber;
    // We use this to enforce that a given field never
    // changes DV type, even across segments / IndexWriter
    // sessions:
    private final Map<String,DocValuesType> docValuesType;

    // TODO: we should similarly catch an attempt to turn
    // norms back on after they were already ommitted; today
    // we silently discard the norm but this is badly trappy
    private int lowestUnassignedFieldNumber = -1;
    
    FieldNumbers() {
      this.nameToNumber = new HashMap<String, Integer>();
      this.numberToName = new HashMap<Integer, String>();
      this.docValuesType = new HashMap<String,DocValuesType>();
    }
    
    /**
     * Returns the global field number for the given field name. If the name
     * does not exist yet it tries to add it with the given preferred field
     * number assigned if possible otherwise the first unassigned field number
     * is used as the field number.
     */
    synchronized int addOrGet(String fieldName, int preferredFieldNumber, DocValuesType dvType) {
      if (dvType != null) {
        DocValuesType currentDVType = docValuesType.get(fieldName);
        if (currentDVType == null) {
          docValuesType.put(fieldName, dvType);
        } else if (currentDVType != null && currentDVType != dvType) {
          throw new IllegalArgumentException("cannot change DocValues type from " + currentDVType + " to " + dvType + " for field \"" + fieldName + "\"");
        }
      }
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

    // used by assert
    synchronized boolean containsConsistent(Integer number, String name, DocValuesType dvType) {
      return name.equals(numberToName.get(number))
          && number.equals(nameToNumber.get(name)) &&
        (dvType == null || docValuesType.get(name) == null || dvType == docValuesType.get(name));
    }

    synchronized void clear() {
      numberToName.clear();
      nameToNumber.clear();
      docValuesType.clear();
    }
  }
  
  static final class Builder {
    private final HashMap<String,FieldInfo> byName = new HashMap<String,FieldInfo>();
    final FieldNumbers globalFieldNumbers;

    Builder() {
      this(new FieldNumbers());
    }
    
    /**
     * Creates a new instance with the given {@link FieldNumbers}. 
     */
    Builder(FieldNumbers globalFieldNumbers) {
      assert globalFieldNumbers != null;
      this.globalFieldNumbers = globalFieldNumbers;
    }

    public void add(FieldInfos other) {
      for(FieldInfo fieldInfo : other){ 
        add(fieldInfo);
      }
    }
   
    /** NOTE: this method does not carry over termVector
     *  booleans nor docValuesType; the indexer chain
     *  (TermVectorsConsumerPerField, DocFieldProcessor) must
     *  set these fields when they succeed in consuming
     *  the document */
    public FieldInfo addOrUpdate(String name, IndexableFieldType fieldType) {
      // TODO: really, indexer shouldn't even call this
      // method (it's only called from DocFieldProcessor);
      // rather, each component in the chain should update
      // what it "owns".  EG fieldType.indexOptions() should
      // be updated by maybe FreqProxTermsWriterPerField:
      return addOrUpdateInternal(name, -1, fieldType.indexed(), false,
                                 fieldType.omitNorms(), false,
                                 fieldType.indexOptions(), fieldType.docValueType(), null);
    }

    private FieldInfo addOrUpdateInternal(String name, int preferredFieldNumber, boolean isIndexed,
        boolean storeTermVector,
        boolean omitNorms, boolean storePayloads, IndexOptions indexOptions, DocValuesType docValues, DocValuesType normType) {
      FieldInfo fi = fieldInfo(name);
      if (fi == null) {
        // This field wasn't yet added to this in-RAM
        // segment's FieldInfo, so now we get a global
        // number for this field.  If the field was seen
        // before then we'll get the same name and number,
        // else we'll allocate a new one:
        final int fieldNumber = globalFieldNumbers.addOrGet(name, preferredFieldNumber, docValues);
        fi = new FieldInfo(name, isIndexed, fieldNumber, storeTermVector, omitNorms, storePayloads, indexOptions, docValues, normType, null);
        assert !byName.containsKey(fi.name);
        assert globalFieldNumbers.containsConsistent(Integer.valueOf(fi.number), fi.name, fi.getDocValuesType());
        byName.put(fi.name, fi);
      } else {
        fi.update(isIndexed, storeTermVector, omitNorms, storePayloads, indexOptions);

        if (docValues != null) {
          fi.setDocValuesType(docValues);
        }

        if (!fi.omitsNorms() && normType != null) {
          fi.setNormValueType(normType);
        }
      }
      return fi;
    }
    
    public FieldInfo add(FieldInfo fi) {
      // IMPORTANT - reuse the field number if possible for consistent field numbers across segments
      return addOrUpdateInternal(fi.name, fi.number, fi.isIndexed(), fi.hasVectors(),
                 fi.omitsNorms(), fi.hasPayloads(),
                 fi.getIndexOptions(), fi.getDocValuesType(), fi.getNormType());
    }
    
    public FieldInfo fieldInfo(String fieldName) {
      return byName.get(fieldName);
    }
    
    final FieldInfos finish() {
      return new FieldInfos(byName.values().toArray(new FieldInfo[byName.size()]));
    }
  }
}
