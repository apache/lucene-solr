package org.apache.lucene.codecs;

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
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Type; // javadocs
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;

/**
 * Abstract base class for PerDocProducer implementations
 * @lucene.experimental
 */
public abstract class PerDocProducerBase extends PerDocProducer {

  protected abstract void closeInternal(Collection<? extends Closeable> closeables) throws IOException;
  protected abstract Map<String, DocValues> docValues();
  
  @Override
  public void close() throws IOException {
    closeInternal(docValues().values());
  }
  
  @Override
  public DocValues docValues(String field) throws IOException {
    return docValues().get(field);
  }
  
  public Comparator<BytesRef> getComparator() throws IOException {
    return BytesRef.getUTF8SortedAsUnicodeComparator();
  }

  // Only opens files... doesn't actually load any values
  protected TreeMap<String, DocValues> load(FieldInfos fieldInfos,
      String segment, int docCount, Directory dir, IOContext context)
      throws IOException {
    TreeMap<String, DocValues> values = new TreeMap<String, DocValues>();
    boolean success = false;
    try {

      for (FieldInfo fieldInfo : fieldInfos) {
        if (canLoad(fieldInfo)) {
          final String field = fieldInfo.name;
          final String id = docValuesId(segment,
              fieldInfo.number);
          values.put(field,
              loadDocValues(docCount, dir, id, getDocValuesType(fieldInfo), context));
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
  
  protected boolean canLoad(FieldInfo info) {
    return info.hasDocValues();
  }
  
  protected Type getDocValuesType(FieldInfo info) {
    return info.getDocValuesType();
  }
  
  protected boolean anyDocValuesFields(FieldInfos infos) {
    return infos.hasDocValues();
  }
  
  public static String docValuesId(String segmentsName, int fieldId) {
    return segmentsName + "_" + fieldId;
  }
  
  /**
   * Loads a {@link DocValues} instance depending on the given {@link Type}.
   * Codecs that use different implementations for a certain {@link Type} can
   * simply override this method and return their custom implementations.
   * 
   * @param docCount
   *          number of documents in the segment
   * @param dir
   *          the {@link Directory} to load the {@link DocValues} from
   * @param id
   *          the unique file ID within the segment
   * @param type
   *          the type to load
   * @return a {@link DocValues} instance for the given type
   * @throws IOException
   *           if an {@link IOException} occurs
   * @throws IllegalArgumentException
   *           if the given {@link Type} is not supported
   */
  protected abstract DocValues loadDocValues(int docCount, Directory dir, String id,
      DocValues.Type type, IOContext context) throws IOException;
}
