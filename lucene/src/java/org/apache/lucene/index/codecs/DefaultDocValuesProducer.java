package org.apache.lucene.index.codecs;

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
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.TreeMap;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.values.Bytes;
import org.apache.lucene.index.values.IndexDocValues;
import org.apache.lucene.index.values.Floats;
import org.apache.lucene.index.values.Ints;
import org.apache.lucene.index.values.ValueType;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Abstract base class for FieldsProducer implementations supporting
 * {@link IndexDocValues}.
 * 
 * @lucene.experimental
 */
public class DefaultDocValuesProducer extends PerDocValues {

  protected final TreeMap<String, IndexDocValues> docValues;
  private final boolean useCompoundFile;
  private final Closeable cfs;
  private final Comparator<BytesRef> sortComparator;

  /**
   * 
   * Creates a new {@link DefaultDocValuesProducer} instance and loads all
   * {@link IndexDocValues} instances for this segment and codec.
   * 
   * @param si
   *          the segment info to load the {@link IndexDocValues} for.
   * @param dir
   *          the directory to load the {@link IndexDocValues} from.
   * @param fieldInfo
   *          the {@link FieldInfos}
   * @param codecId
   *          the codec ID
   * @param useCompoundFile
   *          if <code>true</code> this producer opens a compound file to read
   *          IndexDocValues fields, otherwise each field defines its own set of
   *          files.
   * @param sortComparator
   *          defines the sort order for sorted IndexDocValues variants
   * @throws IOException
   *           if an {@link IOException} occurs
   */
  public DefaultDocValuesProducer(SegmentInfo si, Directory dir, 
      FieldInfos fieldInfo, int codecId, boolean useCompoundFile, Comparator<BytesRef> sortComparator, IOContext context) throws IOException {
    this.useCompoundFile = useCompoundFile;
    this.sortComparator = sortComparator;
    final Directory directory;
    if (useCompoundFile) {
      cfs = directory = new CompoundFileDirectory(dir, IndexFileNames.segmentFileName(si.name, codecId, IndexFileNames.COMPOUND_FILE_EXTENSION), context, false);
    } else {
      cfs = null;
      directory = dir;
    }
    docValues = load(fieldInfo, si.name, si.docCount, directory, codecId, context);
  }

  /**
   * Returns a {@link IndexDocValues} instance for the given field name or
   * <code>null</code> if this field has no {@link IndexDocValues}.
   */
  @Override
  public IndexDocValues docValues(String field) throws IOException {
    return docValues.get(field);
  }

  // Only opens files... doesn't actually load any values
  protected TreeMap<String, IndexDocValues> load(FieldInfos fieldInfos,
      String segment, int docCount, Directory dir, int codecId, IOContext context)
      throws IOException {
    TreeMap<String, IndexDocValues> values = new TreeMap<String, IndexDocValues>();
    boolean success = false;
    try {

      for (FieldInfo fieldInfo : fieldInfos) {
        if (codecId == fieldInfo.getCodecId() && fieldInfo.hasDocValues()) {
          final String field = fieldInfo.name;
          // TODO can we have a compound file per segment and codec for
          // docvalues?
          final String id = DefaultDocValuesConsumer.docValuesId(segment,
              codecId, fieldInfo.number);
          values.put(field,
              loadDocValues(docCount, dir, id, fieldInfo.getDocValues(), sortComparator, context));
        }
      }
      success = true;
    } finally {
      if (!success) {
        // if we fail we must close all opened resources if there are any
        closeInternal(values.values());
      }
    }
    return values;
  }
  

  /**
   * Loads a {@link IndexDocValues} instance depending on the given {@link ValueType}.
   * Codecs that use different implementations for a certain {@link ValueType} can
   * simply override this method and return their custom implementations.
   * 
   * @param docCount
   *          number of documents in the segment
   * @param dir
   *          the {@link Directory} to load the {@link IndexDocValues} from
   * @param id
   *          the unique file ID within the segment
   * @param type
   *          the type to load
   * @param sortComparator byte comparator used by sorted variants
   * @return a {@link IndexDocValues} instance for the given type
   * @throws IOException
   *           if an {@link IOException} occurs
   * @throws IllegalArgumentException
   *           if the given {@link ValueType} is not supported
   */
  protected IndexDocValues loadDocValues(int docCount, Directory dir, String id,
      ValueType type, Comparator<BytesRef> sortComparator, IOContext context) throws IOException {
    switch (type) {
    case FIXED_INTS_16:
    case FIXED_INTS_32:
    case FIXED_INTS_64:
    case FIXED_INTS_8:
    case VAR_INTS:
      return Ints.getValues(dir, id, docCount, type, context);
    case FLOAT_32:
      return Floats.getValues(dir, id, docCount, context);
    case FLOAT_64:
      return Floats.getValues(dir, id, docCount, context);
    case BYTES_FIXED_STRAIGHT:
      return Bytes.getValues(dir, id, Bytes.Mode.STRAIGHT, true, docCount, sortComparator, context);
    case BYTES_FIXED_DEREF:
      return Bytes.getValues(dir, id, Bytes.Mode.DEREF, true, docCount, sortComparator, context);
    case BYTES_FIXED_SORTED:
      return Bytes.getValues(dir, id, Bytes.Mode.SORTED, true, docCount, sortComparator, context);
    case BYTES_VAR_STRAIGHT:
      return Bytes.getValues(dir, id, Bytes.Mode.STRAIGHT, false, docCount, sortComparator, context);
    case BYTES_VAR_DEREF:
      return Bytes.getValues(dir, id, Bytes.Mode.DEREF, false, docCount, sortComparator, context);
    case BYTES_VAR_SORTED:
      return Bytes.getValues(dir, id, Bytes.Mode.SORTED, false, docCount, sortComparator, context);
    default:
      throw new IllegalStateException("unrecognized index values mode " + type);
    }
  }

  public void close() throws IOException {
    closeInternal(docValues.values());
  }

  private void closeInternal(Collection<? extends Closeable> closeables) throws IOException {
    final Collection<? extends Closeable> toClose;
    if (useCompoundFile) {
      final ArrayList<Closeable> list = new ArrayList<Closeable>(closeables);
      list.add(cfs);
      toClose = list; 
    } else {
      toClose = closeables;
    } 
    IOUtils.close(toClose);
  }

  @Override
  public Collection<String> fields() {
    return docValues.keySet();
  }
}
