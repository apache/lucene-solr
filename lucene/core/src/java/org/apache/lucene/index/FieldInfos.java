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
package org.apache.lucene.index;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.lucene.util.ArrayUtil;

/** 
 * Collection of {@link FieldInfo}s (accessible by number or by name).
 *  @lucene.experimental
 */
public class FieldInfos implements Iterable<FieldInfo> {

  /** An instance without any fields. */
  public final static FieldInfos EMPTY = new FieldInfos(new FieldInfo[0]);

  private final boolean hasFreq;
  private final boolean hasProx;
  private final boolean hasPayloads;
  private final boolean hasOffsets;
  private final boolean hasVectors;
  private final boolean hasNorms;
  private final boolean hasDocValues;
  private final boolean hasPointValues;
  private final String softDeletesField;
  
  // used only by fieldInfo(int)
  private final FieldInfo[] byNumber;
  
  private final HashMap<String,FieldInfo> byName = new HashMap<>();
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
    boolean hasPointValues = false;
    String softDeletesField = null;

    int size = 0; // number of elements in byNumberTemp, number of used array slots
    FieldInfo[] byNumberTemp = new FieldInfo[10]; // initial array capacity of 10
    for (FieldInfo info : infos) {
      if (info.number < 0) {
        throw new IllegalArgumentException("illegal field number: " + info.number + " for field " + info.name);
      }
      size = info.number >= size ? info.number+1 : size;
      if (info.number >= byNumberTemp.length){ //grow array
        byNumberTemp = ArrayUtil.grow(byNumberTemp, info.number + 1);
      }
      FieldInfo previous = byNumberTemp[info.number];
      if (previous != null) {
        throw new IllegalArgumentException("duplicate field numbers: " + previous.name + " and " + info.name + " have: " + info.number);
      }
      byNumberTemp[info.number] = info;

      previous = byName.put(info.name, info);
      if (previous != null) {
        throw new IllegalArgumentException("duplicate field names: " + previous.number + " and " + info.number + " have: " + info.name);
      }

      hasVectors |= info.hasVectors();
      hasProx |= info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      hasFreq |= info.getIndexOptions() != IndexOptions.DOCS;
      hasOffsets |= info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      hasNorms |= info.hasNorms();
      hasDocValues |= info.getDocValuesType() != DocValuesType.NONE;
      hasPayloads |= info.hasPayloads();
      hasPointValues |= (info.getPointDimensionCount() != 0);
      if (info.isSoftDeletesField()) {
        if (softDeletesField != null && softDeletesField.equals(info.name) == false) {
          throw new IllegalArgumentException("multiple soft-deletes fields [" + info.name + ", " + softDeletesField + "]");
        }
        softDeletesField = info.name;
      }
    }
    
    this.hasVectors = hasVectors;
    this.hasProx = hasProx;
    this.hasPayloads = hasPayloads;
    this.hasOffsets = hasOffsets;
    this.hasFreq = hasFreq;
    this.hasNorms = hasNorms;
    this.hasDocValues = hasDocValues;
    this.hasPointValues = hasPointValues;
    this.softDeletesField = softDeletesField;

    List<FieldInfo> valuesTemp = new ArrayList<>();
    byNumber = new FieldInfo[size];
    for(int i=0; i<size; i++){
      byNumber[i] = byNumberTemp[i];
      if (byNumberTemp[i] != null) {
        valuesTemp.add(byNumberTemp[i]);
      }
    }
    values = Collections.unmodifiableCollection(Arrays.asList(valuesTemp.toArray(new FieldInfo[0])));
  }

  /** Call this to get the (merged) FieldInfos for a
   *  composite reader.
   *  <p>
   *  NOTE: the returned field numbers will likely not
   *  correspond to the actual field numbers in the underlying
   *  readers, and codec metadata ({@link FieldInfo#getAttribute(String)}
   *  will be unavailable.
   */
  public static FieldInfos getMergedFieldInfos(IndexReader reader) {
    final List<LeafReaderContext> leaves = reader.leaves();
    if (leaves.isEmpty()) {
      return FieldInfos.EMPTY;
    } else if (leaves.size() == 1) {
      return leaves.get(0).reader().getFieldInfos();
    } else {
      final String softDeletesField = leaves.stream()
          .map(l -> l.reader().getFieldInfos().getSoftDeletesField())
          .filter(Objects::nonNull)
          .findAny().orElse(null);
      final Builder builder = new Builder(new FieldNumbers(softDeletesField));
      for (final LeafReaderContext ctx : leaves) {
        builder.add(ctx.reader().getFieldInfos());
      }
      return builder.finish();
    }
  }

  /** Returns a set of names of fields that have a terms index.  The order is undefined. */
  public static Collection<String> getIndexedFields(IndexReader reader) {
    return reader.leaves().stream()
        .flatMap(l -> StreamSupport.stream(l.reader().getFieldInfos().spliterator(), false)
        .filter(fi -> fi.getIndexOptions() != IndexOptions.NONE))
        .map(fi -> fi.name)
        .collect(Collectors.toSet());
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

  /** Returns true if any fields have PointValues */
  public boolean hasPointValues() {
    return hasPointValues;
  }

  /** Returns the soft-deletes field name if exists; otherwise returns null */
  public String getSoftDeletesField() {
    return softDeletesField;
  }
  
  /** Returns the number of fields */
  public int size() {
    return byName.size();
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
   * @param fieldNumber field's number.
   * @return the FieldInfo object or null when the given fieldNumber
   * doesn't exist.
   * @throws IllegalArgumentException if fieldNumber is negative
   */
  public FieldInfo fieldInfo(int fieldNumber) {
    if (fieldNumber < 0) {
      throw new IllegalArgumentException("Illegal field number: " + fieldNumber);
    }
    if (fieldNumber >= byNumber.length) {
      return null;
    }
    return byNumber[fieldNumber];
  }

  static final class FieldDimensions {
    public final int dimensionCount;
    public final int indexDimensionCount;
    public final int dimensionNumBytes;

    public FieldDimensions(int dimensionCount, int indexDimensionCount, int dimensionNumBytes) {
      this.dimensionCount = dimensionCount;
      this.indexDimensionCount = indexDimensionCount;
      this.dimensionNumBytes = dimensionNumBytes;
    }
  }
  
  static final class FieldNumbers {
    
    private final Map<Integer,String> numberToName;
    private final Map<String,Integer> nameToNumber;
    private final Map<String, IndexOptions> indexOptions;
    // We use this to enforce that a given field never
    // changes DV type, even across segments / IndexWriter
    // sessions:
    private final Map<String,DocValuesType> docValuesType;

    private final Map<String,FieldDimensions> dimensions;

    // TODO: we should similarly catch an attempt to turn
    // norms back on after they were already committed; today
    // we silently discard the norm but this is badly trappy
    private int lowestUnassignedFieldNumber = -1;

    // The soft-deletes field from IWC to enforce a single soft-deletes field
    private final String softDeletesFieldName;
    
    FieldNumbers(String softDeletesFieldName) {
      this.nameToNumber = new HashMap<>();
      this.numberToName = new HashMap<>();
      this.indexOptions = new HashMap<>();
      this.docValuesType = new HashMap<>();
      this.dimensions = new HashMap<>();
      this.softDeletesFieldName = softDeletesFieldName;
    }
    
    /**
     * Returns the global field number for the given field name. If the name
     * does not exist yet it tries to add it with the given preferred field
     * number assigned if possible otherwise the first unassigned field number
     * is used as the field number.
     */
    synchronized int addOrGet(String fieldName, int preferredFieldNumber, IndexOptions indexOptions, DocValuesType dvType, int dimensionCount, int indexDimensionCount, int dimensionNumBytes, boolean isSoftDeletesField) {
      if (indexOptions != IndexOptions.NONE) {
        IndexOptions currentOpts = this.indexOptions.get(fieldName);
        if (currentOpts == null) {
          this.indexOptions.put(fieldName, indexOptions);
        } else if (currentOpts != IndexOptions.NONE && currentOpts != indexOptions) {
          throw new IllegalArgumentException("cannot change field \"" + fieldName + "\" from index options=" + currentOpts + " to inconsistent index options=" + indexOptions);
        }
      }
      if (dvType != DocValuesType.NONE) {
        DocValuesType currentDVType = docValuesType.get(fieldName);
        if (currentDVType == null) {
          docValuesType.put(fieldName, dvType);
        } else if (currentDVType != DocValuesType.NONE && currentDVType != dvType) {
          throw new IllegalArgumentException("cannot change DocValues type from " + currentDVType + " to " + dvType + " for field \"" + fieldName + "\"");
        }
      }
      if (dimensionCount != 0) {
        FieldDimensions dims = dimensions.get(fieldName);
        if (dims != null) {
          if (dims.dimensionCount != dimensionCount) {
            throw new IllegalArgumentException("cannot change point dimension count from " + dims.dimensionCount + " to " + dimensionCount + " for field=\"" + fieldName + "\"");
          }
          if (dims.indexDimensionCount != indexDimensionCount) {
            throw new IllegalArgumentException("cannot change point index dimension count from " + dims.indexDimensionCount + " to " + indexDimensionCount + " for field=\"" + fieldName + "\"");
          }
          if (dims.dimensionNumBytes != dimensionNumBytes) {
            throw new IllegalArgumentException("cannot change point numBytes from " + dims.dimensionNumBytes + " to " + dimensionNumBytes + " for field=\"" + fieldName + "\"");
          }
        } else {
          dimensions.put(fieldName, new FieldDimensions(dimensionCount, indexDimensionCount, dimensionNumBytes));
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
        assert fieldNumber >= 0;
        numberToName.put(fieldNumber, fieldName);
        nameToNumber.put(fieldName, fieldNumber);
      }

      if (isSoftDeletesField) {
        if (softDeletesFieldName == null) {
          throw new IllegalArgumentException("this index has [" + fieldName + "] as soft-deletes already but soft-deletes field is not configured in IWC");
        } else if (fieldName.equals(softDeletesFieldName) == false) {
          throw new IllegalArgumentException("cannot configure [" + softDeletesFieldName + "] as soft-deletes; this index uses [" + fieldName + "] as soft-deletes already");
        }
      } else if (fieldName.equals(softDeletesFieldName)) {
        throw new IllegalArgumentException("cannot configure [" + softDeletesFieldName + "] as soft-deletes; this index uses [" + fieldName + "] as non-soft-deletes already");
      }

      return fieldNumber.intValue();
    }

    synchronized void verifyConsistent(Integer number, String name, IndexOptions indexOptions) {
      if (name.equals(numberToName.get(number)) == false) {
        throw new IllegalArgumentException("field number " + number + " is already mapped to field name \"" + numberToName.get(number) + "\", not \"" + name + "\"");
      }
      if (number.equals(nameToNumber.get(name)) == false) {
        throw new IllegalArgumentException("field name \"" + name + "\" is already mapped to field number \"" + nameToNumber.get(name) + "\", not \"" + number + "\"");
      }
      IndexOptions currentIndexOptions = this.indexOptions.get(name);
      if (indexOptions != IndexOptions.NONE && currentIndexOptions != null && currentIndexOptions != IndexOptions.NONE && indexOptions != currentIndexOptions) {
        throw new IllegalArgumentException("cannot change field \"" + name + "\" from index options=" + currentIndexOptions + " to inconsistent index options=" + indexOptions);
      }
    }

    synchronized void verifyConsistent(Integer number, String name, DocValuesType dvType) {
      if (name.equals(numberToName.get(number)) == false) {
        throw new IllegalArgumentException("field number " + number + " is already mapped to field name \"" + numberToName.get(number) + "\", not \"" + name + "\"");
      }
      if (number.equals(nameToNumber.get(name)) == false) {
        throw new IllegalArgumentException("field name \"" + name + "\" is already mapped to field number \"" + nameToNumber.get(name) + "\", not \"" + number + "\"");
      }
      DocValuesType currentDVType = docValuesType.get(name);
      if (dvType != DocValuesType.NONE && currentDVType != null && currentDVType != DocValuesType.NONE && dvType != currentDVType) {
        throw new IllegalArgumentException("cannot change DocValues type from " + currentDVType + " to " + dvType + " for field \"" + name + "\"");
      }
    }

    synchronized void verifyConsistentDimensions(Integer number, String name, int dataDimensionCount, int indexDimensionCount, int dimensionNumBytes) {
      if (name.equals(numberToName.get(number)) == false) {
        throw new IllegalArgumentException("field number " + number + " is already mapped to field name \"" + numberToName.get(number) + "\", not \"" + name + "\"");
      }
      if (number.equals(nameToNumber.get(name)) == false) {
        throw new IllegalArgumentException("field name \"" + name + "\" is already mapped to field number \"" + nameToNumber.get(name) + "\", not \"" + number + "\"");
      }
      FieldDimensions dim = dimensions.get(name);
      if (dim != null) {
        if (dim.dimensionCount != dataDimensionCount) {
          throw new IllegalArgumentException("cannot change point dimension count from " + dim.dimensionCount + " to " + dataDimensionCount + " for field=\"" + name + "\"");
        }
        if (dim.indexDimensionCount != indexDimensionCount) {
          throw new IllegalArgumentException("cannot change point index dimension count from " + dim.indexDimensionCount + " to " + indexDimensionCount + " for field=\"" + name + "\"");
        }
        if (dim.dimensionNumBytes != dimensionNumBytes) {
          throw new IllegalArgumentException("cannot change point numBytes from " + dim.dimensionNumBytes + " to " + dimensionNumBytes + " for field=\"" + name + "\"");
        }
      }
    }

    /**
     * Returns true if the {@code fieldName} exists in the map and is of the
     * same {@code dvType}.
     */
    synchronized boolean contains(String fieldName, DocValuesType dvType) {
      // used by IndexWriter.updateNumericDocValue
      if (!nameToNumber.containsKey(fieldName)) {
        return false;
      } else {
        // only return true if the field has the same dvType as the requested one
        return dvType == docValuesType.get(fieldName);
      }
    }

    @Deprecated
    synchronized Set<String> getFieldNames() {
      return Collections.unmodifiableSet(new HashSet<>(nameToNumber.keySet()));
    }

    synchronized void clear() {
      numberToName.clear();
      nameToNumber.clear();
      indexOptions.clear();
      docValuesType.clear();
      dimensions.clear();
      lowestUnassignedFieldNumber = -1;
    }

    synchronized void setIndexOptions(int number, String name, IndexOptions indexOptions) {
      verifyConsistent(number, name, indexOptions);
      this.indexOptions.put(name, indexOptions);
    }

    synchronized void setDocValuesType(int number, String name, DocValuesType dvType) {
      verifyConsistent(number, name, dvType);
      docValuesType.put(name, dvType);
    }

    synchronized void setDimensions(int number, String name, int dimensionCount, int indexDimensionCount, int dimensionNumBytes) {
      if (dimensionCount > PointValues.MAX_DIMENSIONS) {
        throw new IllegalArgumentException("dimensionCount must be <= PointValues.MAX_DIMENSIONS (= " + PointValues.MAX_DIMENSIONS + "); got " + dimensionCount + " for field=\"" + name + "\"");
      }
      if (dimensionNumBytes > PointValues.MAX_NUM_BYTES) {
        throw new IllegalArgumentException("dimension numBytes must be <= PointValues.MAX_NUM_BYTES (= " + PointValues.MAX_NUM_BYTES + "); got " + dimensionNumBytes + " for field=\"" + name + "\"");
      }
      if (indexDimensionCount > dimensionCount) {
        throw new IllegalArgumentException("indexDimensionCount must be <= dimensionCount (= " + dimensionCount + "); got " + indexDimensionCount + " for field=\"" + name + "\"");
      }
      if (indexDimensionCount > PointValues.MAX_INDEX_DIMENSIONS) {
        throw new IllegalArgumentException("indexDimensionCount must be <= PointValues.MAX_INDEX_DIMENSIONS (= " + PointValues.MAX_INDEX_DIMENSIONS + "); got " + indexDimensionCount + " for field=\"" + name + "\"");
      }
      verifyConsistentDimensions(number, name, dimensionCount, indexDimensionCount, dimensionNumBytes);
      dimensions.put(name, new FieldDimensions(dimensionCount, indexDimensionCount, dimensionNumBytes));
    }
  }
  
  static final class Builder {
    private final HashMap<String,FieldInfo> byName = new HashMap<>();
    final FieldNumbers globalFieldNumbers;
    private boolean finished;

    /**
     * Creates a new instance with the given {@link FieldNumbers}. 
     */
    Builder(FieldNumbers globalFieldNumbers) {
      assert globalFieldNumbers != null;
      this.globalFieldNumbers = globalFieldNumbers;
    }

    public void add(FieldInfos other) {
      assert assertNotFinished();
      for(FieldInfo fieldInfo : other){ 
        add(fieldInfo);
      }
    }

    /** Create a new field, or return existing one. */
    public FieldInfo getOrAdd(String name) {
      FieldInfo fi = fieldInfo(name);
      if (fi == null) {
        assert assertNotFinished();
        // This field wasn't yet added to this in-RAM
        // segment's FieldInfo, so now we get a global
        // number for this field.  If the field was seen
        // before then we'll get the same name and number,
        // else we'll allocate a new one:
        final boolean isSoftDeletesField = name.equals(globalFieldNumbers.softDeletesFieldName);
        final int fieldNumber = globalFieldNumbers.addOrGet(name, -1, IndexOptions.NONE, DocValuesType.NONE, 0, 0, 0, isSoftDeletesField);
        fi = new FieldInfo(name, fieldNumber, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, new HashMap<>(), 0, 0, 0, isSoftDeletesField);
        assert !byName.containsKey(fi.name);
        globalFieldNumbers.verifyConsistent(Integer.valueOf(fi.number), fi.name, DocValuesType.NONE);
        byName.put(fi.name, fi);
      }

      return fi;
    }
   
    private FieldInfo addOrUpdateInternal(String name, int preferredFieldNumber,
                                          boolean storeTermVector,
                                          boolean omitNorms, boolean storePayloads, IndexOptions indexOptions,
                                          DocValuesType docValues, long dvGen,
                                          Map<String, String> attributes,
                                          int dataDimensionCount, int indexDimensionCount, int dimensionNumBytes,
                                          boolean isSoftDeletesField) {
      assert assertNotFinished();
      if (docValues == null) {
        throw new NullPointerException("DocValuesType must not be null");
      }
      if (attributes != null) {
        // original attributes is UnmodifiableMap
        attributes = new HashMap<>(attributes);
      }

      FieldInfo fi = fieldInfo(name);
      if (fi == null) {
        // This field wasn't yet added to this in-RAM
        // segment's FieldInfo, so now we get a global
        // number for this field.  If the field was seen
        // before then we'll get the same name and number,
        // else we'll allocate a new one:
        final int fieldNumber = globalFieldNumbers.addOrGet(name, preferredFieldNumber, indexOptions, docValues, dataDimensionCount, indexDimensionCount, dimensionNumBytes, isSoftDeletesField);
        fi = new FieldInfo(name, fieldNumber, storeTermVector, omitNorms, storePayloads, indexOptions, docValues, dvGen, attributes, dataDimensionCount, indexDimensionCount, dimensionNumBytes, isSoftDeletesField);
        assert !byName.containsKey(fi.name);
        globalFieldNumbers.verifyConsistent(Integer.valueOf(fi.number), fi.name, fi.getDocValuesType());
        byName.put(fi.name, fi);
      } else {
        fi.update(storeTermVector, omitNorms, storePayloads, indexOptions, attributes, dataDimensionCount, indexDimensionCount, dimensionNumBytes);

        if (docValues != DocValuesType.NONE) {
          // Only pay the synchronization cost if fi does not already have a DVType
          boolean updateGlobal = fi.getDocValuesType() == DocValuesType.NONE;
          if (updateGlobal) {
            // Must also update docValuesType map so it's
            // aware of this field's DocValuesType.  This will throw IllegalArgumentException if
            // an illegal type change was attempted.
            globalFieldNumbers.setDocValuesType(fi.number, name, docValues);
          }

          fi.setDocValuesType(docValues); // this will also perform the consistency check.
          fi.setDocValuesGen(dvGen);
        }
      }
      return fi;
    }

    public FieldInfo add(FieldInfo fi) {
      return add(fi, -1);
    }

    public FieldInfo add(FieldInfo fi, long dvGen) {
      // IMPORTANT - reuse the field number if possible for consistent field numbers across segments
      return addOrUpdateInternal(fi.name, fi.number, fi.hasVectors(),
                                 fi.omitsNorms(), fi.hasPayloads(),
                                 fi.getIndexOptions(), fi.getDocValuesType(), dvGen,
                                 fi.attributes(),
                                 fi.getPointDimensionCount(), fi.getPointIndexDimensionCount(), fi.getPointNumBytes(),
                                 fi.isSoftDeletesField());
    }
    
    public FieldInfo fieldInfo(String fieldName) {
      return byName.get(fieldName);
    }

    /** Called only from assert */
    private boolean assertNotFinished() {
      if (finished) {
        throw new IllegalStateException("FieldInfos.Builder was already finished; cannot add new fields");
      }
      return true;
    }
    
    FieldInfos finish() {
      finished = true;
      return new FieldInfos(byName.values().toArray(new FieldInfo[byName.size()]));
    }
  }
}
