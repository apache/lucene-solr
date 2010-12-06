package org.apache.lucene.index.codecs.docvalues;

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
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.values.DocValues;
import org.apache.lucene.index.values.PerDocFieldValues;
import org.apache.lucene.index.values.Writer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * @lucene.experimental
 */
// TODO this might need to go in the codec package since is a direct relative to
// TermsConsumer
public abstract class DocValuesConsumer {
  
  protected AtomicLong bytesUsed = new AtomicLong(0);
  
  protected DocValuesConsumer(AtomicLong bytesUsed) {
    this.bytesUsed = bytesUsed;
  }

  public final long bytesUsed() {
    return this.bytesUsed.get();
  }

  public abstract void add(int docID, PerDocFieldValues docValues) throws IOException;

  public abstract void finish(int docCount) throws IOException;

  public abstract void files(Collection<String> files) throws IOException;

  public void merge(org.apache.lucene.index.codecs.MergeState mergeState,
      DocValues values) throws IOException {
    // TODO we need some kind of compatibility notation for values such
    // that two slightly different segments can be merged eg. fixed vs.
    // variable byte len or float32 vs. float64
    int docBase = 0;
    boolean merged = false;
    /*
     * We ignore the given DocValues here and merge from the subReaders directly
     * to support bulk copies on the DocValues Writer level. if this gets merged
     * with MultiDocValues the writer can not optimize for bulk-copyable data
     */
    for (final IndexReader reader : mergeState.readers) {
      final DocValues r = reader.docValues(mergeState.fieldInfo.name);
      if (r != null) {
        merged = true;
        merge(new Writer.MergeState(r, docBase, reader.maxDoc(), reader
            .getDeletedDocs()));
      }
      docBase += reader.numDocs();
    }
    if (merged)
      finish(mergeState.mergedDocCount);
  }

  protected abstract void merge(MergeState mergeState) throws IOException;

  /*
   * specialized auxiliary MergeState is necessary since we don't want to
   * exploit internals up to the codec ones
   */
  public static class MergeState {
    public final DocValues reader;
    public final int docBase;
    public final int docCount;
    public final Bits bits;

    public MergeState(DocValues reader, int docBase, int docCount, Bits bits) {
      assert reader != null;
      this.reader = reader;
      this.docBase = docBase;
      this.docCount = docCount;
      this.bits = bits;
    }
  }

  public static DocValuesConsumer create(String id,
      Directory directory, FieldInfo field, Comparator<BytesRef> comp, AtomicLong bytesUsed)
      throws IOException {
    return Writer.create(field.getDocValues(), id, directory, comp, bytesUsed);
  }
}
