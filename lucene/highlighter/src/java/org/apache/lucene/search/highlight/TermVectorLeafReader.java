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
package org.apache.lucene.search.highlight;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.Bits;

/**
 * Wraps a Terms with a {@link org.apache.lucene.index.LeafReader}, typically from term vectors.
 *
 * @lucene.experimental
 */
public class TermVectorLeafReader extends LeafReader {

  private final Fields fields;
  private final FieldInfos fieldInfos;

  public TermVectorLeafReader(String field, Terms terms) {
    fields = new Fields() {
      @Override
      public Iterator<String> iterator() {
        return Collections.singletonList(field).iterator();
      }

      @Override
      public Terms terms(String fld) throws IOException {
        if (!field.equals(fld)) {
          return null;
        }
        return terms;
      }

      @Override
      public int size() {
        return 1;
      }
    };

    IndexOptions indexOptions;
    if (!terms.hasFreqs()) {
      indexOptions = IndexOptions.DOCS;
    } else if (!terms.hasPositions()) {
      indexOptions = IndexOptions.DOCS_AND_FREQS;
    } else if (!terms.hasOffsets()) {
      indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    } else {
      indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
    }
    FieldInfo fieldInfo = new FieldInfo(field, 0,
                                        true, true, terms.hasPayloads(),
                                        indexOptions, DocValuesType.NONE, -1, Collections.emptyMap(), 0, 0);
    fieldInfos = new FieldInfos(new FieldInfo[]{fieldInfo});
  }

  @Override
  public void addCoreClosedListener(CoreClosedListener listener) {
    addCoreClosedListenerAsReaderClosedListener(this, listener);
  }

  @Override
  public void removeCoreClosedListener(CoreClosedListener listener) {
    removeCoreClosedListenerAsReaderClosedListener(this, listener);
  }

  @Override
  protected void doClose() throws IOException {
  }

  @Override
  public Fields fields() throws IOException {
    return fields;
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    return null;
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    return null;
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    return null;
  }

  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    return null;
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    return null;
  }

  @Override
  public Bits getDocsWithField(String field) throws IOException {
    return null;
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    return null;//Is this needed?  See MemoryIndex for a way to do it.
  }

  @Override
  public FieldInfos getFieldInfos() {
    return fieldInfos;
  }

  @Override
  public Bits getLiveDocs() {
    return null;
  }

  @Override
  public PointValues getPointValues() {
    return null;
  }

  @Override
  public void checkIntegrity() throws IOException {
  }

  @Override
  public Fields getTermVectors(int docID) throws IOException {
    if (docID != 0) {
      return null;
    }
    return fields();
  }

  @Override
  public int numDocs() {
    return 1;
  }

  @Override
  public int maxDoc() {
    return 1;
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
  }

  @Override
  public Sort getIndexSort() {
    return null;
  }
}
