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
package org.apache.solr.uninverting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Function;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.uninverting.FieldCache.CacheEntry;

/**
 * A FilterReader that exposes <i>indexed</i> values as if they also had
 * docvalues.
 * <p>
 * This is accomplished by "inverting the inverted index" or "uninversion".
 * <p>
 * The uninversion process happens lazily: upon the first request for the 
 * field's docvalues (e.g. via {@link org.apache.lucene.index.LeafReader#getNumericDocValues(String)} 
 * or similar), it will create the docvalues on-the-fly if needed and cache it,
 * based on the core cache key of the wrapped LeafReader.
 */
public class UninvertingReader extends FilterLeafReader {

  /**
   * Specifies the type of uninversion to apply for the field. 
   */
  public static enum Type {
    /** 
     * Single-valued Integer, (e.g. indexed with {@link org.apache.lucene.document.IntPoint})
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link NumericDocValuesField}.
     */
    INTEGER_POINT,
    /** 
     * Single-valued Integer, (e.g. indexed with {@link org.apache.lucene.document.LongPoint})
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link NumericDocValuesField}.
     */
    LONG_POINT,
    /** 
     * Single-valued Integer, (e.g. indexed with {@link org.apache.lucene.document.FloatPoint})
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link NumericDocValuesField}.
     */
    FLOAT_POINT,
    /** 
     * Single-valued Integer, (e.g. indexed with {@link org.apache.lucene.document.DoublePoint})
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link NumericDocValuesField}.
     */
    DOUBLE_POINT,
    /** 
     * Single-valued Integer, (e.g. indexed with {@link org.apache.solr.legacy.LegacyIntField})
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link NumericDocValuesField}.
     * @deprecated Index with points and use {@link #INTEGER_POINT} instead.
     */
    @Deprecated
    LEGACY_INTEGER,
    /** 
     * Single-valued Long, (e.g. indexed with {@link org.apache.solr.legacy.LegacyLongField})
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link NumericDocValuesField}.
     * @deprecated Index with points and use {@link #LONG_POINT} instead.
     */
    @Deprecated
    LEGACY_LONG,
    /** 
     * Single-valued Float, (e.g. indexed with {@link org.apache.solr.legacy.LegacyFloatField})
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link NumericDocValuesField}.
     * @deprecated Index with points and use {@link #FLOAT_POINT} instead.
     */
    @Deprecated
    LEGACY_FLOAT,
    /** 
     * Single-valued Double, (e.g. indexed with {@link org.apache.solr.legacy.LegacyDoubleField})
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link NumericDocValuesField}.
     * @deprecated Index with points and use {@link #DOUBLE_POINT} instead.
     */
    @Deprecated
    LEGACY_DOUBLE,
    /** 
     * Single-valued Binary, (e.g. indexed with {@link StringField}) 
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link BinaryDocValuesField}.
     */
    BINARY,
    /** 
     * Single-valued Binary, (e.g. indexed with {@link StringField}) 
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link SortedDocValuesField}.
     */
    SORTED,
    /** 
     * Multi-valued Binary, (e.g. indexed with {@link StringField}) 
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link SortedSetDocValuesField}.
     */
    SORTED_SET_BINARY,
    /** 
     * Multi-valued Integer, (e.g. indexed with {@link org.apache.solr.legacy.LegacyIntField})
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link SortedSetDocValuesField}.
     */
    SORTED_SET_INTEGER,
    /** 
     * Multi-valued Float, (e.g. indexed with {@link org.apache.solr.legacy.LegacyFloatField})
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link SortedSetDocValuesField}.
     */
    SORTED_SET_FLOAT,
    /** 
     * Multi-valued Long, (e.g. indexed with {@link org.apache.solr.legacy.LegacyLongField})
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link SortedSetDocValuesField}.
     */
    SORTED_SET_LONG,
    /** 
     * Multi-valued Double, (e.g. indexed with {@link org.apache.solr.legacy.LegacyDoubleField})
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link SortedSetDocValuesField}.
     */
    SORTED_SET_DOUBLE

  }

  /** @see #wrap(DirectoryReader, Function) */
  public static DirectoryReader wrap(DirectoryReader reader, Map<String, Type> mapping) throws IOException {
    return wrap(reader, mapping::get);
  }

  /**
   * Wraps a provided {@link DirectoryReader}. Note that for convenience, the returned reader
   * can be used normally (e.g. passed to {@link DirectoryReader#openIfChanged(DirectoryReader)})
   * and so on. 
   * 
   * @param in input directory reader
   * @param mapper function to map a field name to an uninversion type.  A Null result means to not uninvert.
   * @return a wrapped directory reader
   */
  public static DirectoryReader wrap(DirectoryReader in, Function<String, Type> mapper) throws IOException {
    return new UninvertingDirectoryReader(in, mapper);
  }

  static class UninvertingDirectoryReader extends FilterDirectoryReader {
    final Function<String, Type> mapper;
    
    public UninvertingDirectoryReader(DirectoryReader in, final Function<String, Type> mapper) throws IOException {
      super(in, new FilterDirectoryReader.SubReaderWrapper() {
        @Override
        public LeafReader wrap(LeafReader reader) {
          return UninvertingReader.wrap(reader, mapper);
        }
      });
      this.mapper = mapper;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new UninvertingDirectoryReader(in, mapper);
    }

    // NOTE: delegating the cache helpers is wrong since this wrapper alters the
    // content of the reader, it is only fine to do that because Solr ALWAYS
    // consumes index readers through this wrapper

    @Override
    public CacheHelper getReaderCacheHelper() {
      return in.getReaderCacheHelper();
    }
  }

  /**
   * Create a new UninvertingReader with the specified mapping, wrapped around the input.  It may be deemed that there
   * is no mapping to do, in which case the input is returned.
   * <p>
   * Expert: This should almost never be used. Use {@link #wrap(DirectoryReader, Function)} instead.
   *
   * @lucene.internal
   */
  public static LeafReader wrap(LeafReader in, Function<String, Type> mapping) {
    boolean wrap = false;

    // Calculate a new FieldInfos that has DocValuesType where we didn't before
    ArrayList<FieldInfo> newFieldInfos = new ArrayList<>(in.getFieldInfos().size());
    for (FieldInfo fi : in.getFieldInfos()) {
      DocValuesType type = fi.getDocValuesType();
      // fields which currently don't have docValues, but are uninvertable (indexed or points data present)
      if (type == DocValuesType.NONE &&
          (fi.getIndexOptions() != IndexOptions.NONE || (fi.getPointNumBytes() > 0 && fi.getPointDimensionCount() == 1))) {
        Type t = mapping.apply(fi.name); // could definitely return null, thus still can't uninvert it
        if (t != null) {
          if (t == Type.INTEGER_POINT || t == Type.LONG_POINT || t == Type.FLOAT_POINT || t == Type.DOUBLE_POINT) {
            // type uses points
            if (fi.getPointDimensionCount() == 0) {
              continue;
            }
          } else {
            // type uses inverted index
            if (fi.getIndexOptions() == IndexOptions.NONE) {
              continue;
            }
          }
          switch(t) {
            case INTEGER_POINT:
            case LONG_POINT:
            case FLOAT_POINT:
            case DOUBLE_POINT:
            case LEGACY_INTEGER:
            case LEGACY_LONG:
            case LEGACY_FLOAT:
            case LEGACY_DOUBLE:
              type = DocValuesType.NUMERIC;
              break;
            case BINARY:
              type = DocValuesType.BINARY;
              break;
            case SORTED:
              type = DocValuesType.SORTED;
              break;
            case SORTED_SET_BINARY:
            case SORTED_SET_INTEGER:
            case SORTED_SET_FLOAT:
            case SORTED_SET_LONG:
            case SORTED_SET_DOUBLE:
              type = DocValuesType.SORTED_SET;
              break;
            default:
              throw new AssertionError();
          }
        }
      }
      if (type != fi.getDocValuesType()) { // we changed it
        wrap = true;
        newFieldInfos.add(new FieldInfo(fi.name, fi.number, fi.hasVectors(), fi.omitsNorms(),
            fi.hasPayloads(), fi.getIndexOptions(), type, fi.getDocValuesGen(), fi.attributes(),
            fi.getPointDimensionCount(), fi.getPointIndexDimensionCount(), fi.getPointNumBytes(), fi.isSoftDeletesField()));
      } else {
        newFieldInfos.add(fi);
      }
    }
    if (!wrap) {
      return in;
    } else {
      FieldInfos fieldInfos = new FieldInfos(newFieldInfos.toArray(new FieldInfo[newFieldInfos.size()]));
      return new UninvertingReader(in, mapping, fieldInfos);
    }
  }

  final Function<String, Type> mapping;
  final FieldInfos fieldInfos;

  private UninvertingReader(LeafReader in, Function<String, Type> mapping, FieldInfos fieldInfos) {
    super(in);
    this.mapping = mapping;
    this.fieldInfos = fieldInfos;
  }

  @Override
  public FieldInfos getFieldInfos() {
    return fieldInfos;
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    NumericDocValues values = super.getNumericDocValues(field);
    if (values != null) {
      return values;
    }
    Type v = getType(field);
    if (v != null) {
      switch (v) {
        case INTEGER_POINT: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.INT_POINT_PARSER);
        case FLOAT_POINT: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.FLOAT_POINT_PARSER);
        case LONG_POINT: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.LONG_POINT_PARSER);
        case DOUBLE_POINT: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.DOUBLE_POINT_PARSER);
        case LEGACY_INTEGER: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.LEGACY_INT_PARSER);
        case LEGACY_FLOAT: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.LEGACY_FLOAT_PARSER);
        case LEGACY_LONG: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.LEGACY_LONG_PARSER);
        case LEGACY_DOUBLE: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.LEGACY_DOUBLE_PARSER);
        case BINARY:
        case SORTED:
        case SORTED_SET_BINARY:
        case SORTED_SET_DOUBLE:
        case SORTED_SET_FLOAT:
        case SORTED_SET_INTEGER:
        case SORTED_SET_LONG:
          break;
      }
    }
    return null;
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    BinaryDocValues values = in.getBinaryDocValues(field);
    if (values != null) {
      return values;
    }
    Type v = getType(field);
    if (v == Type.BINARY) {
      return FieldCache.DEFAULT.getTerms(in, field);
    } else {
      return null;
    }
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    SortedDocValues values = in.getSortedDocValues(field);
    if (values != null) {
      return values;
    }
    Type v = getType(field);
    if (v == Type.SORTED) {
      return FieldCache.DEFAULT.getTermsIndex(in, field);
    } else {
      return null;
    }
  }
  
  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    SortedSetDocValues values = in.getSortedSetDocValues(field);
    if (values != null) {
      return values;
    }
    Type v = getType(field);
    if (v != null) {
      switch (v) {
        case SORTED_SET_INTEGER:
        case SORTED_SET_FLOAT: 
          return FieldCache.DEFAULT.getDocTermOrds(in, field, FieldCache.INT32_TERM_PREFIX);
        case SORTED_SET_LONG:
        case SORTED_SET_DOUBLE:
          return FieldCache.DEFAULT.getDocTermOrds(in, field, FieldCache.INT64_TERM_PREFIX);
        case SORTED_SET_BINARY:
          return FieldCache.DEFAULT.getDocTermOrds(in, field, null);
        case BINARY:
        case LEGACY_DOUBLE:
        case LEGACY_FLOAT:
        case LEGACY_INTEGER:
        case LEGACY_LONG:
        case DOUBLE_POINT:
        case FLOAT_POINT:
        case INTEGER_POINT:
        case LONG_POINT:
        case SORTED:
          break;
      }
    }
    return null;
  }

  /** 
   * Returns the field's uninversion type, or null 
   * if the field doesn't exist or doesn't have a mapping.
   */
  private Type getType(String field) {
    return mapping.apply(field);
  }

  // NOTE: delegating the cache helpers is wrong since this wrapper alters the
  // content of the reader, it is only fine to do that because Solr ALWAYS
  // consumes index readers through this wrapper

  @Override
  public CacheHelper getCoreCacheHelper() {
    return in.getCoreCacheHelper();
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return in.getReaderCacheHelper();
  }

  @Override
  public String toString() {
    return "Uninverting(" + in.toString() + ")";
  }
  
  /** 
   * Return information about the backing cache
   * @lucene.internal 
   */
  public static FieldCacheStats getUninvertedStats() {
    CacheEntry[] entries = FieldCache.DEFAULT.getCacheEntries();
    long totalBytesUsed = 0;
    String[] info = new String[entries.length];
    for (int i = 0; i < entries.length; i++) {
      info[i] = entries[i].toString();
      totalBytesUsed += entries[i].getValue().ramBytesUsed();
    }
    String totalSize = RamUsageEstimator.humanReadableUnits(totalBytesUsed);
    return new FieldCacheStats(totalSize, info);
  }

  public static int getUninvertedStatsSize() {
    return FieldCache.DEFAULT.getCacheEntries().length;
  }

  /**
   * Return information about the backing cache
   * @lucene.internal
   */
  public static class FieldCacheStats {
    public String totalSize;
    public String[] info;

    public FieldCacheStats(String totalSize, String[] info) {
      this.totalSize = totalSize;
      this.info = info;
    }

  }
}
