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
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.Bits;

public abstract class NumericDocValuesConsumer {
  public abstract void add(long value) throws IOException;
  public abstract void finish() throws IOException;
  
  public int merge(MergeState mergeState) throws IOException {
    int docCount = 0;
    for (AtomicReader reader : mergeState.readers) {
      final int maxDoc = reader.maxDoc();
      final Bits liveDocs = reader.getLiveDocs();
      // nocommit what if this is null...?  need default source?
      final NumericDocValues source = reader.getNumericDocValues(mergeState.fieldInfo.name);
      for (int i = 0; i < maxDoc; i++) {
        if (liveDocs == null || liveDocs.get(i)) {
          add(source.get(i));
        }
        docCount++;
        mergeState.checkAbort.work(300);
      }
    }
    finish();
    return docCount;
  }
}
