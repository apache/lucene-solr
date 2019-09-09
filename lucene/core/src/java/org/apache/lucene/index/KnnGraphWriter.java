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


import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.GraphSearch;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Counter;

class KnnGraphWriter extends DocValuesWriter {

  private final FieldInfo fieldInfo;
  private final Counter iwBytesUsed;
  private final VectorDocValuesWriter vectorWriter;
  private final ReferenceDocValuesWriter refWriter;
  private final VectorDocValues vectorValues;
  private final SortedNumericDocValues refs;
  private final GraphSearch graphSearch;

  // in the usual Lucene sense - the maximum doc id, plus one
  private int maxDoc;

  public KnnGraphWriter(FieldInfo fieldInfo, Counter iwBytesUsed, ReferenceDocValuesWriter refWriter) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    this.refWriter = refWriter;
    refs = refWriter.getBufferedValues();
    vectorWriter = new VectorDocValuesWriter(fieldInfo, iwBytesUsed);
    vectorValues = vectorWriter.getBufferedValues();
    // nocommit magic number 6: what value of topK should we use here?
    graphSearch = GraphSearch.fromDimension(vectorValues.dimension());
  }

  public void addValue(int docId, float[] vector) throws IOException {
    if (maxDoc > 0) {
      for (ScoreDoc ref : graphSearch.search(() -> vectorValues, () -> refs, vector, maxDoc)) {
        if (ref.doc >= 0) {     // there can be sentinels present
          refWriter.addValue(docId, ref.doc);
        }
      }
    }
    assert docId >= maxDoc;
    maxDoc = docId + 1;
    vectorWriter.addValue(docId, vector);
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    vectorWriter.flush(state, sortMap, dvConsumer);
  }

  @Override
  DocIdSetIterator getDocIdSet() {
    return vectorWriter.getDocIdSet();
  }

  @Override
  Sorter.DocComparator getDocComparator(int numDoc, SortField sortField) throws IOException {
    return vectorWriter.getDocComparator(numDoc, sortField);
  }

  @Override
  public void finish(int maxDoc) {
  }

}
