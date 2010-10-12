package org.apache.lucene.index.values;

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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.values.Reader.SortedSource;
import org.apache.lucene.index.values.Reader.Source;
import org.apache.lucene.util.BytesRef;

public class Cache {
  final IndexReader r;
  // TODO(simonw): use WeakHashMaps instead here?
  final Map<String, Source> ints = new HashMap<String, Source>();
  final Map<String, Source> floats = new HashMap<String, Source>();
  final Map<String, Source> bytes = new HashMap<String, Source>();
  final Map<String, SortedSource> sortedBytes = new HashMap<String, SortedSource>();

  public Cache(IndexReader r) {
    this.r = r;
  }

  synchronized public Source getInts(String id) throws IOException {
    Source s = ints.get(id);
    if (s == null) {
      final Reader indexValues = r.getIndexValues(id);
      if (indexValues == null) {
        return null;
      }
      s = indexValues.load();
      ints.put(id, s);
    }

    return s;
  }

  synchronized public Source getFloats(String id) throws IOException {
    Source s = floats.get(id);
    if (s == null) {
      final Reader indexValues = r.getIndexValues(id);
      if (indexValues == null) {
        return null;
      }
      s = indexValues.load();
      floats.put(id, s);
    }

    return s;
  }

  synchronized public SortedSource getSortedBytes(String id,
      Comparator<BytesRef> comp) throws IOException {
    SortedSource s = sortedBytes.get(id);
    if (s == null) {
      final Reader indexValues = r.getIndexValues(id);
      if (indexValues == null) {
        return null;
      }
      s = indexValues.loadSorted(comp);
      sortedBytes.put(id, s);
    } else {
      // TODO(simonw): verify comp is the same!
    }

    return s;
  }

  synchronized public Source getBytes(String id) throws IOException {
    Source s = bytes.get(id);
    if (s == null) {
      final Reader indexValues = r.getIndexValues(id);
      if (indexValues == null) {
        return null;
      }
      s = indexValues.load();
      bytes.put(id, s);
    }

    return s;
  }

  public void purgeInts(String id) {
    ints.remove(id);
  }

  public void purgeFloats(String id) {
    floats.remove(id);
  }

  public void purgeBytes(String id) {
    bytes.remove(id);
  }

  public void purgeSortedBytes(String id) {
    sortedBytes.remove(id);
  }
}
