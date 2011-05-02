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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.index.values.DocValues;
import org.apache.lucene.index.values.MultiDocValues;
import org.apache.lucene.index.values.Type;
import org.apache.lucene.index.values.MultiDocValues.DocValuesIndex;
import org.apache.lucene.util.ReaderUtil;

/**
 * 
 * nocommit - javadoc
 * @experimental
 */
public class MultiPerDocValues extends PerDocValues {
  private final PerDocValues[] subs;
  private final ReaderUtil.Slice[] subSlices;
  private final Map<String, DocValues> docValues = new ConcurrentHashMap<String, DocValues>();
  private final TreeSet<String> fields;

  public MultiPerDocValues(PerDocValues[] subs, ReaderUtil.Slice[] subSlices) {
    this.subs = subs;
    this.subSlices = subSlices;
    fields = new TreeSet<String>();
    for (PerDocValues sub : subs) {
      fields.addAll(sub.fields());
    }
  }

  public static PerDocValues getPerDocs(IndexReader r) throws IOException {
    final IndexReader[] subs = r.getSequentialSubReaders();
    if (subs == null) {
      // already an atomic reader
      return r.perDocValues();
    } else if (subs.length == 0) {
      // no fields
      return null;
    } else if (subs.length == 1) {
      return getPerDocs(subs[0]);
    }
    PerDocValues perDocValues = r.retrievePerDoc();
    if (perDocValues == null) {

      final List<PerDocValues> producer = new ArrayList<PerDocValues>();
      final List<ReaderUtil.Slice> slices = new ArrayList<ReaderUtil.Slice>();

      new ReaderUtil.Gather(r) {
        @Override
        protected void add(int base, IndexReader r) throws IOException {
          final PerDocValues f = r.perDocValues();
          if (f != null) {
            producer.add(f);
            slices
                .add(new ReaderUtil.Slice(base, r.maxDoc(), producer.size() - 1));
          }
        }
      }.run();

      if (producer.size() == 0) {
        return null;
      } else if (producer.size() == 1) {
        perDocValues = producer.get(0);
      } else {
        perDocValues = new MultiPerDocValues(
            producer.toArray(PerDocValues.EMPTY_ARRAY),
            slices.toArray(ReaderUtil.Slice.EMPTY_ARRAY));
      }
      r.storePerDoc(perDocValues);
    }
    return perDocValues;
  }

  public DocValues docValues(String field) throws IOException {
    DocValues result = docValues.get(field);
    if (result == null) {
      // Lazy init: first time this field is requested, we
      // create & add to docValues:
      final List<MultiDocValues.DocValuesIndex> docValuesIndex = new ArrayList<MultiDocValues.DocValuesIndex>();
      int docsUpto = 0;
      Type type = null;
      // Gather all sub-readers that share this field
      for (int i = 0; i < subs.length; i++) {
        DocValues values = subs[i].docValues(field);
        final int start = subSlices[i].start;
        final int length = subSlices[i].length;
        if (values != null) {
          if (docsUpto != start) {
            type = values.type();
            docValuesIndex.add(new MultiDocValues.DocValuesIndex(
                new MultiDocValues.DummyDocValues(start, type), docsUpto, start
                    - docsUpto));
          }
          docValuesIndex.add(new MultiDocValues.DocValuesIndex(values, start,
              length));
          docsUpto = start + length;

        } else if (i + 1 == subs.length && !docValuesIndex.isEmpty()) {
          docValuesIndex.add(new MultiDocValues.DocValuesIndex(
              new MultiDocValues.DummyDocValues(start, type), docsUpto, start
                  - docsUpto));
        }
      }
      if (docValuesIndex.isEmpty()) {
        return null;
      }
      result = new MultiDocValues(
          docValuesIndex.toArray(DocValuesIndex.EMPTY_ARRAY));
      docValues.put(field, result);
    }
    return result;
  }

  public void close() throws IOException {
    PerDocValues[] perDocValues = this.subs;
    for (PerDocValues values : perDocValues) {
      values.close();
    }
  }

  @Override
  public Collection<String> fields() {
    return fields;
  }
}
