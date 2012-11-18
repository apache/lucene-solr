package org.apache.lucene.codecs;

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

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

public abstract class BinaryDocValuesConsumer {
  public abstract void add(BytesRef value) throws IOException;
  public abstract void finish() throws IOException;
  
  public int merge(MergeState mergeState) throws IOException {
    int docCount = 0;
    final BytesRef bytes = new BytesRef();
    for (AtomicReader reader : mergeState.readers) {
      final int maxDoc = reader.maxDoc();
      final Bits liveDocs = reader.getLiveDocs();

      BinaryDocValues source = reader.getBinaryDocValues(mergeState.fieldInfo.name);
      if (source == null) {
        source = BinaryDocValues.DEFAULT;
      }

      for (int i = 0; i < maxDoc; i++) {
        if (liveDocs == null || liveDocs.get(i)) {
          source.get(i, bytes);
          add(bytes);
        }
        docCount++;
        mergeState.checkAbort.work(300);
      }
    }
    finish();
    return docCount;
  }
}
