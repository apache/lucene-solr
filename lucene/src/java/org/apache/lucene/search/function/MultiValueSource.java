package org.apache.lucene.search.function;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.ReaderUtil;

/** This class wraps another ValueSource, but protects
 *  against accidental double RAM usage in FieldCache when
 *  a composite reader is passed to {@link #getValues}.
 *
 *  <p><b>NOTE</b>: this class adds a CPU penalty to every
 *  lookup, as it must resolve the incoming document to the
 *  right sub-reader using a binary search.</p>
 *
 *  @deprecated (4.0) This class is temporary, to ease the
 *  migration to segment-based searching. Please change your
 *  code to not pass composite readers to these APIs. */

@Deprecated
public final class MultiValueSource extends ValueSource {

  final ValueSource other;
  public MultiValueSource(ValueSource other) {
    this.other = other;
  }

  @Override
  public DocValues getValues(IndexReader reader) throws IOException {

    IndexReader[] subReaders = reader.getSequentialSubReaders();
    if (subReaders != null) {
      // This is a composite reader
      return new MultiDocValues(subReaders);
    } else {
      // Already an atomic reader -- just delegate
      return other.getValues(reader);
    }
  }

  @Override
  public String description() {
    return other.description();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof MultiValueSource) {
      return ((MultiValueSource) o).other.equals(other);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return 31 * other.hashCode();
  }

  private final class MultiDocValues extends DocValues {

    final DocValues[] docValues;
    final int[] docStarts;

    MultiDocValues(IndexReader[] subReaders) throws IOException {
      docValues = new DocValues[subReaders.length];
      docStarts = new int[subReaders.length];
      int base = 0;
      for(int i=0;i<subReaders.length;i++) {
        docValues[i] = other.getValues(subReaders[i]);
        docStarts[i] = base;
        base += subReaders[i].maxDoc();
      }
    }
    
    @Override
    public float floatVal(int doc) {
      final int n = ReaderUtil.subIndex(doc, docStarts);
      return docValues[n].floatVal(doc-docStarts[n]);
    }

    @Override
    public int intVal(int doc) {
      final int n = ReaderUtil.subIndex(doc, docStarts);
      return docValues[n].intVal(doc-docStarts[n]);
    }

    @Override
    public long longVal(int doc) {
      final int n = ReaderUtil.subIndex(doc, docStarts);
      return docValues[n].longVal(doc-docStarts[n]);
    }

    @Override
    public double doubleVal(int doc) {
      final int n = ReaderUtil.subIndex(doc, docStarts);
      return docValues[n].doubleVal(doc-docStarts[n]);
    }

    @Override
    public String strVal(int doc) {
      final int n = ReaderUtil.subIndex(doc, docStarts);
      return docValues[n].strVal(doc-docStarts[n]);
    }

    @Override
    public String toString(int doc) {
      final int n = ReaderUtil.subIndex(doc, docStarts);
      return docValues[n].toString(doc-docStarts[n]);
    }

    @Override
    public Explanation explain(int doc) {
      final int n = ReaderUtil.subIndex(doc, docStarts);
      return docValues[n].explain(doc-docStarts[n]);
    }
  }
}
