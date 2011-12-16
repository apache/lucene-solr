package org.apache.lucene.codecs.lucene40;

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
import java.util.Arrays;

import org.apache.lucene.codecs.NormsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

public class Lucene40NormsWriter extends NormsWriter {
  private IndexOutput out;
  private int normCount = 0;
  
  /** norms header placeholder */
  static final byte[] NORMS_HEADER = new byte[]{'N','R','M',-1};
  
  /** Extension of norms file */
  static final String NORMS_EXTENSION = "nrm";
  
  /** Extension of separate norms file
   * @deprecated */
  @Deprecated
  static final String SEPARATE_NORMS_EXTENSION = "s";
  
  public Lucene40NormsWriter(Directory directory, String segment, IOContext context) throws IOException {
    final String normsFileName = IndexFileNames.segmentFileName(segment, "", NORMS_EXTENSION);
    boolean success = false;
    try {
      out = directory.createOutput(normsFileName, context);
      out.writeBytes(NORMS_HEADER, 0, NORMS_HEADER.length);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }

  @Override
  public void startField(FieldInfo info) throws IOException {
    assert info.omitNorms == false;
    normCount++;
  }
  
  @Override
  public void writeNorm(byte norm) throws IOException {
    out.writeByte(norm);
  }
  
  @Override
  public void finish(int numDocs) throws IOException {
    if (4+normCount*(long)numDocs != out.getFilePointer()) {
      throw new RuntimeException(".nrm file size mismatch: expected=" + (4+normCount*(long)numDocs) + " actual=" + out.getFilePointer());
    }
  }
  
  /** we override merge and bulk-merge norms when there are no deletions */
  @Override
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
          if (reader.liveDocs == null) {
            //optimized case for segments without deleted docs
            out.writeBytes(normBuffer, maxDoc);
            numMergedDocsForField += maxDoc;
          } else {
            // this segment has deleted docs, so we have to
            // check for every doc if it is deleted or not
            final Bits liveDocs = reader.liveDocs;
            for (int k = 0; k < maxDoc; k++) {
              if (liveDocs.get(k)) {
                numMergedDocsForField++;
                out.writeByte(normBuffer[k]);
              }
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

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(out);
    } finally {
      out = null;
    }
  }
}
