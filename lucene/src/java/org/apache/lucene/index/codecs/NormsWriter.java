package org.apache.lucene.index.codecs;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.util.Bits;

// simple api just for now before switching to docvalues apis
public abstract class NormsWriter implements Closeable {

  // TODO: I think IW should set info.normValueType from Similarity,
  // and then this method just returns DocValuesConsumer
  public abstract void startField(FieldInfo info) throws IOException;
  public abstract void writeNorm(byte norm) throws IOException;
  public abstract void finish(int numDocs) throws IOException;
  
  public int merge(MergeState mergeState) throws IOException {
    int numMergedDocs = 0;
    for (FieldInfo fi : mergeState.fieldInfos) {
      if (fi.isIndexed && !fi.omitNorms) {
        startField(fi);
        int numMergedDocsForField = 0;
        for (MergeState.IndexReaderAndLiveDocs reader : mergeState.readers) {
          final int maxDoc = reader.reader.maxDoc();
          byte normBuffer[] = reader.reader.norms(fi.name);
          if (normBuffer == null) {
            // Can be null if this segment doesn't have
            // any docs with this field
            normBuffer = new byte[maxDoc];
            Arrays.fill(normBuffer, (byte)0);
          }
          // this segment has deleted docs, so we have to
          // check for every doc if it is deleted or not
          final Bits liveDocs = reader.liveDocs;
          for (int k = 0; k < maxDoc; k++) {
            if (liveDocs == null || liveDocs.get(k)) {
              writeNorm(normBuffer[k]);
              numMergedDocsForField++;
            }
          }
          mergeState.checkAbort.work(maxDoc);
        }
        assert numMergedDocs == 0 || numMergedDocs == numMergedDocsForField;
        numMergedDocs = numMergedDocsForField;
      }
    }
    finish(numMergedDocs);
    return numMergedDocs;
  }
}
