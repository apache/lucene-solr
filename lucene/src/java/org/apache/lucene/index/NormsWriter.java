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
import java.util.Collection;
import java.util.Map;

import org.apache.lucene.store.IndexOutput;

// TODO FI: norms could actually be stored as doc store

/** Writes norms.  Each thread X field accumulates the norms
 *  for the doc/fields it saw, then the flush method below
 *  merges all of these together into a single _X.nrm file.
 */

final class NormsWriter extends InvertedDocEndConsumer {


  @Override
  public void abort() {}

  // We only write the _X.nrm file at flush
  void files(Collection<String> files) {}

  /** Produce _X.nrm if any document had a field with norms
   *  not disabled */
  @Override
  public void flush(Map<FieldInfo,InvertedDocEndConsumerPerField> fieldsToFlush, SegmentWriteState state) throws IOException {
    if (!state.fieldInfos.hasNorms()) {
      return;
    }

    final String normsFileName = IndexFileNames.segmentFileName(state.segmentName, "", IndexFileNames.NORMS_EXTENSION);
    IndexOutput normsOut = state.directory.createOutput(normsFileName);

    try {
      normsOut.writeBytes(SegmentNorms.NORMS_HEADER, 0, SegmentNorms.NORMS_HEADER.length);

      int normCount = 0;

      for (FieldInfo fi : state.fieldInfos) {
        final NormsWriterPerField toWrite = (NormsWriterPerField) fieldsToFlush.get(fi);
        int upto = 0;
        if (toWrite != null && toWrite.upto > 0) {
          normCount++;

          int docID = 0;
          for (; docID < state.numDocs; docID++) {
            if (upto < toWrite.upto && toWrite.docIDs[upto] == docID) {
              normsOut.writeByte(toWrite.norms[upto]);
              upto++;
            } else {
              normsOut.writeByte((byte) 0);
            }
          }

          // we should have consumed every norm
          assert upto == toWrite.upto;

          toWrite.reset();
        } else if (fi.isIndexed && !fi.omitNorms) {
          normCount++;
          // Fill entire field with default norm:
          for(;upto<state.numDocs;upto++)
            normsOut.writeByte((byte) 0);
        }

        assert 4+normCount*state.numDocs == normsOut.getFilePointer() : ".nrm file size mismatch: expected=" + (4+normCount*state.numDocs) + " actual=" + normsOut.getFilePointer();
      }

    } finally {
      normsOut.close();
    }
  }

  @Override
  void finishDocument() throws IOException {}

  @Override
  void startDocument() throws IOException {}

  @Override
  InvertedDocEndConsumerPerField addField(DocInverterPerField docInverterPerField,
      FieldInfo fieldInfo) {
    return new NormsWriterPerField(docInverterPerField, fieldInfo);
  }
}
