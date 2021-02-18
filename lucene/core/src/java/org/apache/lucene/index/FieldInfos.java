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

import static org.apache.lucene.index.FieldInfo.verifySameDocValuesType;
import static org.apache.lucene.index.FieldInfo.verifySameIndexOptions;
import static org.apache.lucene.index.FieldInfo.verifySamePointsOptions;
import static org.apache.lucene.index.FieldInfo.verifySameVectorOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
 *
 * @lucene.experimental
 */
public class FieldInfos implements Iterable<FieldInfo> {

  /** An instance without any fields. */
  public static final FieldInfos EMPTY = new FieldInfos(new FieldInfo[0]);

  private final boolean hasFreq;
  private final boolean hasProx;
  private final boolean hasPayloads;
  private final boolean hasOffsets;
  private final boolean hasVectors;
  private final boolean hasNorms;
  private final boolean hasDocValues;
  private final boolean hasPointValues;
  private final boolean hasVectorValues;
  private final String softDeletesField;

  // used only by fieldInfo(int)
  private final FieldInfo[] byNumber;

  private final HashMap<String, FieldInfo> byName = new HashMap<>();
  private final Collection<FieldInfo> values; // for an unmodifiable iterator

  /** Constructs a new FieldInfos from an array of FieldInfo objects */
  public FieldInfos(FieldInfo[] infos) {
    boolean hasVectors = false;
    boolean hasProx = false;
    boolean hasPayloads = false;
    boolean hasOffsets = false;
    boolean hasFreq = false;
    boolean hasNorms = false;
    boolean hasDocValues = false;
    boolean hasPointValues = false;
    boolean hasVectorValues = false;
    String softDeletesField = null;

    int size = 0; // number of elements in byNumberTemp, number of used array slots
    FieldInfo[] byNumberTemp = new FieldInfo[10]; // initial array capacity of 10
    for (FieldInfo info : infos) {
      if (info.number < 0) {
        throw new IllegalArgumentException(
            "illegal field number: " + info.number + " for field " + info.name);
      }
      size = info.number >= size ? info.number + 1 : size;
      if (info.number >= byNumberTemp.length) { // grow array
        byNumberTemp = ArrayUtil.grow(byNumberTemp, info.number + 1);
      }
      FieldInfo previous = byNumberTemp[info.number];
      if (previous != null) {
        throw new IllegalArgumentException(
            "duplicate field numbers: "
                + previous.name
                + " and "
                + info.name
                + " have: "
                + info.number);
      }
      byNumberTemp[info.number] = info;

      previous = byName.put(info.name, info);
      if (previous != null) {
        throw new IllegalArgumentException(
            "duplicate field names: "
                + previous.number
                + " and "
                + info.number
                + " have: "
                + info.name);
      }

      hasVectors |= info.hasVectors();
      hasProx |= info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      hasFreq |= info.getIndexOptions() != IndexOptions.DOCS;
      hasOffsets |=
          info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
              >= 0;
      hasNorms |= info.hasNorms();
      hasDocValues |= info.getDocValuesType() != DocValuesType.NONE;
      hasPayloads |= info.hasPayloads();
      hasPointValues |= (info.getPointDimensionCount() != 0);
      hasVectorValues |= (info.getVectorDimension() != 0);
      if (info.isSoftDeletesField()) {
        if (softDeletesField != null && softDeletesField.equals(info.name) == false) {
          throw new IllegalArgumentException(
              "multiple soft-deletes fields [" + info.name + ", " + softDeletesField + "]");
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
    this.hasVectorValues = hasVectorValues;
    this.softDeletesField = softDeletesField;

    List<FieldInfo> valuesTemp = new ArrayList<>();
    byNumber = new FieldInfo[size];
    for (int i = 0; i < size; i++) {
      byNumber[i] = byNumberTemp[i];
      if (byNumberTemp[i] != null) {
        valuesTemp.add(byNumberTemp[i]);
      }
    }
    values =
        Collections.unmodifiableCollection(Arrays.asList(valuesTemp.toArray(new FieldInfo[0])));
  }

  /**
   * Call this to get the (merged) FieldInfos for a composite reader.
   *
   * <p>NOTE: the returned field numbers will likely not correspond to the actual field numbers in
   * the underlying readers, and codec metadata ({@link FieldInfo#getAttribute(String)} will be
   * unavailable.
   */
  public static FieldInfos getMergedFieldInfos(IndexReader reader) {
    final List<LeafReaderContext> leaves = reader.leaves();
    if (leaves.isEmpty()) {
      return FieldInfos.EMPTY;
    } else if (leaves.size() == 1) {
      return leaves.get(0).reader().getFieldInfos();
    } else {
      final String softDeletesField =
          leaves.stream()
              .map(l -> l.reader().getFieldInfos().getSoftDeletesField())
              .filter(Objects::nonNull)
              .findAny()
              .orElse(null);
      final Builder builder = new Builder(new FieldNumbers(softDeletesField));
      for (final LeafReaderContext ctx : leaves) {
        builder.add(ctx.reader().getFieldInfos());
      }
      return builder.finish();
    }
  }

  /** Returns a set of names of fields that have a terms index. The order is undefined. */
  public static Collection<String> getIndexedFields(IndexReader reader) {
    return reader.leaves().stream()
        .flatMap(
            l ->
                StreamSupport.stream(l.reader().getFieldInfos().spliterator(), false)
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

  /** Returns true if any fields have VectorValues */
  public boolean hasVectorValues() {
    return hasVectorValues;
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
   * Returns an iterator over all the fieldinfo objects present, ordered by ascending field number
   */
  // TODO: what happens if in fact a different order is used?
  @Override
  public Iterator<FieldInfo> iterator() {
    return values.iterator();
  }

  /**
   * Return the fieldinfo object referenced by the field name
   *
   * @return the FieldInfo object or null when the given fieldName doesn't exist.
   */
  public FieldInfo fieldInfo(String fieldName) {
    return byName.get(fieldName);
  }

  /**
   * Return the fieldinfo object referenced by the fieldNumber.
   *
   * @param fieldNumber field's number.
   * @return the FieldInfo object or null when the given fieldNumber doesn't exist.
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

  static final class FieldVectorProperties {
    final int numDimensions;
    final VectorValues.SearchStrategy searchStrategy;

    FieldVectorProperties(int numDimensions, VectorValues.SearchStrategy searchStrategy) {
      this.numDimensions = numDimensions;
      this.searchStrategy = searchStrategy;
    }
  }

  static final class FieldNumbers {

    private final Map<Integer, String> numberToName;
    private final Map<String, Integer> nameToNumber;
    private final Map<String, IndexOptions> indexOptions;
    // We use this to enforce that a given field never
    // changes DV type, even across segments / IndexWriter
    // sessions:
    private final Map<String, DocValuesType> docValuesType;

    private final Map<String, FieldDimensions> dimensions;

    private final Map<String, FieldVectorProperties> vectorProps;

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
      this.vectorProps = new HashMap<>();
      this.softDeletesFieldName = softDeletesFieldName;
    }

    /**
     * Returns the global field number for the given field name. If the name does not exist yet it
     * tries to add it with the given preferred field number assigned if possible otherwise the
     * first unassigned field number is used as the field number.
     */
    synchronized int addOrGet(
        String fieldName,
        int preferredFieldNumber,
        IndexOptions indexOptions,
        DocValuesType dvType,
        int dimensionCount,
        int indexDimensionCount,
        int dimensionNumBytes,
        int vectorDimension,
        VectorValues.SearchStrategy searchStrategy,
        boolean isSoftDeletesField) {
      Integer fieldNumber = nameToNumber.get(fieldName);
      if (fieldNumber == null) { // first time we see this field in this index
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
        FieldInfo.checkOptionsCorrectness(
            fieldName,
            false,
            false,
            false,
            indexOptions,
            dvType,
            -1,
            dimensionCount,
            indexDimensionCount,
            dimensionNumBytes,
            vectorDimension,
            searchStrategy);
        numberToName.put(fieldNumber, fieldName);
        nameToNumber.put(fieldName, fieldNumber);
        this.indexOptions.put(fieldName, indexOptions);
        docValuesType.put(fieldName, dvType);
        dimensions.put(
            fieldName, new FieldDimensions(dimensionCount, indexDimensionCount, dimensionNumBytes));
        vectorProps.put(fieldName, new FieldVectorProperties(vectorDimension, searchStrategy));
      } else {
        verifySameSchema(
            fieldName,
            indexOptions,
            dvType,
            dimensionCount,
            indexDimensionCount,
            dimensionNumBytes,
            vectorDimension,
            searchStrategy);
      }

      if (isSoftDeletesField) {
        if (softDeletesFieldName == null) {
          throw new IllegalArgumentException(
              "this index has ["
                  + fieldName
                  + "] as soft-deletes already but soft-deletes field is not configured in IWC");
        } else if (fieldName.equals(softDeletesFieldName) == false) {
          throw new IllegalArgumentException(
              "cannot configure ["
                  + softDeletesFieldName
                  + "] as soft-deletes; this index uses ["
                  + fieldName
                  + "] as soft-deletes already");
        }
      } else if (fieldName.equals(softDeletesFieldName)) {
        throw new IllegalArgumentException(
            "cannot configure ["
                + softDeletesFieldName
                + "] as soft-deletes; this index uses ["
                + fieldName
                + "] as non-soft-deletes already");
      }
      return fieldNumber.intValue();
    }

    private void verifySameSchema(
        String fieldName,
        IndexOptions indexOptions,
        DocValuesType dvType,
        int dimensionCount,
        int indexDimensionCount,
        int dimensionNumBytes,
        int vectorDimension,
        VectorValues.SearchStrategy searchStrategy) {

      IndexOptions currentOpts = this.indexOptions.get(fieldName);
      verifySameIndexOptions(fieldName, currentOpts, indexOptions);

      DocValuesType currentDVType = docValuesType.get(fieldName);
      verifySameDocValuesType(fieldName, currentDVType, dvType);

      FieldDimensions dims = dimensions.get(fieldName);
      verifySamePointsOptions(
          fieldName,
          dims.dimensionCount,
          dims.indexDimensionCount,
          dims.dimensionNumBytes,
          dimensionCount,
          indexDimensionCount,
          dimensionNumBytes);

      FieldVectorProperties props = vectorProps.get(fieldName);
      verifySameVectorOptions(
          fieldName, props.numDimensions, props.searchStrategy, vectorDimension, searchStrategy);
    }

    /**
     * Returns true if the {@code fieldName} exists in the map and is of the same {@code dvType}.
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

    /**
     * Construct a new FieldInfo based on the options in global field numbers This method is not
     * synchronized as all the options it uses are not modifiable.
     *
     * @param fieldName name of the field
     * @param dvType doc values type
     * @param newFieldNumber a new field number
     * @return {@code null} if {@code fieldName} doesn't exist in the map or is not of the same
     *     {@code dvType} returns a new FieldInfo based based on the options in global field numbers
     */
    FieldInfo constructFieldInfo(String fieldName, DocValuesType dvType, int newFieldNumber) {
      Integer fieldNumber;
      synchronized (this) {
        fieldNumber = nameToNumber.get(fieldName);
      }
      if (fieldNumber == null) return null;
      DocValuesType dvType0 = docValuesType.get(fieldName);
      if (dvType != dvType0) return null;

      boolean isSoftDeletesField = fieldName.equals(softDeletesFieldName);
      FieldDimensions dims = dimensions.get(fieldName);
      FieldVectorProperties vectors = vectorProps.get(fieldName);
      return new FieldInfo(
          fieldName,
          newFieldNumber,
          false,
          false,
          false,
          indexOptions.get(fieldName),
          dvType,
          -1,
          new HashMap<>(),
          dims.dimensionCount,
          dims.indexDimensionCount,
          dims.dimensionNumBytes,
          vectors.numDimensions,
          vectors.searchStrategy,
          isSoftDeletesField);
    }

    synchronized Set<String> getFieldNames() {
      return Set.copyOf(nameToNumber.keySet());
    }

    synchronized void clear() {
      numberToName.clear();
      nameToNumber.clear();
      indexOptions.clear();
      docValuesType.clear();
      dimensions.clear();
      lowestUnassignedFieldNumber = -1;
    }
  }

  static final class Builder {
    private final HashMap<String, FieldInfo> byName = new HashMap<>();
    final FieldNumbers globalFieldNumbers;
    private boolean finished;

    /** Creates a new instance with the given {@link FieldNumbers}. */
    Builder(FieldNumbers globalFieldNumbers) {
      assert globalFieldNumbers != null;
      this.globalFieldNumbers = globalFieldNumbers;
    }

    public void add(FieldInfos other) {
      for (FieldInfo fieldInfo : other) {
        add(fieldInfo);
      }
    }

    /** Create a new field, or return existing one. */
    public FieldInfo getOrAdd(String name) {
      FieldInfo fi = fieldInfo(name);
      if (fi != null) {
        return fi;
      } else {
        return addField(
            name,
            -1,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NONE,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            VectorValues.SearchStrategy.NONE,
            name.equals(globalFieldNumbers.softDeletesFieldName));
      }
    }

    public FieldInfo add(FieldInfo fi) {
      return add(fi, -1);
    }

    public FieldInfo add(FieldInfo fi, long dvGen) {
      // IMPORTANT - reuse the field number if possible for consistent field numbers across segments
      if (fi.getDocValuesType() == null) {
        throw new NullPointerException("DocValuesType must not be null");
      }
      final FieldInfo curFi = fieldInfo(fi.name);
      if (curFi == null) {
        // original attributes is UnmodifiableMap
        Map<String, String> attributes =
            fi.attributes() == null ? null : new HashMap<>(fi.attributes());
        return addField(
            fi.name,
            fi.number,
            fi.hasVectors(),
            fi.omitsNorms(),
            fi.hasPayloads(),
            fi.getIndexOptions(),
            fi.getDocValuesType(),
            dvGen,
            attributes,
            fi.getPointDimensionCount(),
            fi.getPointIndexDimensionCount(),
            fi.getPointNumBytes(),
            fi.getVectorDimension(),
            fi.getVectorSearchStrategy(),
            fi.isSoftDeletesField());
      } else {
        curFi.verifySameSchema(fi, dvGen);
        if (fi.attributes() != null) {
          fi.attributes().forEach((k, v) -> curFi.putAttribute(k, v));
        }
        return curFi;
      }
    }

    public FieldInfo add(
        String name,
        boolean storeTermVector,
        boolean omitNorms,
        boolean storePayloads,
        IndexOptions indexOptions,
        DocValuesType docValuesType,
        long dvGen,
        Map<String, String> attributes,
        int dataDimensionCount,
        int indexDimensionCount,
        int dimensionNumBytes,
        int vectorDimension,
        VectorValues.SearchStrategy vectorSearchStrategy) {
      return addField(
          name,
          -1,
          storeTermVector,
          omitNorms,
          storePayloads,
          indexOptions,
          docValuesType,
          dvGen,
          attributes,
          dataDimensionCount,
          indexDimensionCount,
          dimensionNumBytes,
          vectorDimension,
          vectorSearchStrategy,
          name.equals(globalFieldNumbers.softDeletesFieldName));
    }

    public FieldInfo fieldInfo(String fieldName) {
      return byName.get(fieldName);
    }

    /** Called only from assert */
    private boolean assertNotFinished() {
      if (finished) {
        throw new IllegalStateException(
            "FieldInfos.Builder was already finished; cannot add new fields");
      }
      return true;
    }

    private FieldInfo addField(
        String name,
        int preferredFieldNumber,
        boolean storeTermVector,
        boolean omitNorms,
        boolean storePayloads,
        IndexOptions indexOptions,
        DocValuesType docValues,
        long dvGen,
        Map<String, String> attributes,
        int dataDimensionCount,
        int indexDimensionCount,
        int dimensionNumBytes,
        int vectorDimension,
        VectorValues.SearchStrategy vectorSearchStrategy,
        boolean isSoftDeletesField) {
      // This field wasn't yet added to this in-RAM
      // segment's FieldInfo, so now we get a global
      // number for this field.  If the field was seen
      // before then we'll get the same name and number,
      // else we'll allocate a new one:
      assert assertNotFinished();
      final int fieldNumber =
          globalFieldNumbers.addOrGet(
              name,
              preferredFieldNumber,
              indexOptions,
              docValues,
              dataDimensionCount,
              indexDimensionCount,
              dimensionNumBytes,
              vectorDimension,
              vectorSearchStrategy,
              isSoftDeletesField);
      FieldInfo fi =
          new FieldInfo(
              name,
              fieldNumber,
              storeTermVector,
              omitNorms,
              storePayloads,
              indexOptions,
              docValues,
              dvGen,
              attributes,
              dataDimensionCount,
              indexDimensionCount,
              dimensionNumBytes,
              vectorDimension,
              vectorSearchStrategy,
              isSoftDeletesField);
      assert byName.containsKey(fi.name) == false;
      byName.put(fi.name, fi);
      return fi;
    }

    FieldInfos finish() {
      finished = true;
      return new FieldInfos(byName.values().toArray(new FieldInfo[byName.size()]));
    }
  }
}
