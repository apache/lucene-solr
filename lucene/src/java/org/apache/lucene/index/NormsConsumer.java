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

import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsWriter;
import org.apache.lucene.util.IOUtils;

// TODO FI: norms could actually be stored as doc store

/** Writes norms.  Each thread X field accumulates the norms
 *  for the doc/fields it saw, then the flush method below
 *  merges all of these together into a single _X.nrm file.
 */

final class NormsConsumer extends InvertedDocEndConsumer {
  final NormsFormat normsFormat;
  
  public NormsConsumer(DocumentsWriterPerThread dwpt) {
    normsFormat = dwpt.codec.normsFormat();
  }

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

    NormsWriter normsOut = null;
    boolean success = false;
    try {
      normsOut = normsFormat.normsWriter(state);

      for (FieldInfo fi : state.fieldInfos) {
        final NormsConsumerPerField toWrite = (NormsConsumerPerField) fieldsToFlush.get(fi);
        int upto = 0;
        // we must check the final value of omitNorms for the fieldinfo, it could have 
        // changed for this field since the first time we added it.
        if (!fi.omitNorms && toWrite != null && toWrite.upto > 0) {
          normsOut.startField(fi);
          int docID = 0;
          for (; docID < state.numDocs; docID++) {
            if (upto < toWrite.upto && toWrite.docIDs[upto] == docID) {
              normsOut.writeNorm(toWrite.norms[upto]);
              upto++;
            } else {
              normsOut.writeNorm((byte) 0);
            }
          }

          // we should have consumed every norm
          assert upto == toWrite.upto;

          toWrite.reset();
        } else if (fi.isIndexed && !fi.omitNorms) {
          // Fill entire field with default norm:
          normsOut.startField(fi);
          for(;upto<state.numDocs;upto++)
            normsOut.writeNorm((byte) 0);
        }
      }
      normsOut.finish(state.numDocs);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(normsOut);
      } else {
        IOUtils.closeWhileHandlingException(normsOut);
      }
    }
  }

  @Override
  void finishDocument() throws IOException {}

  @Override
  void startDocument() throws IOException {}

  @Override
  InvertedDocEndConsumerPerField addField(DocInverterPerField docInverterPerField,
      FieldInfo fieldInfo) {
    return new NormsConsumerPerField(docInverterPerField, fieldInfo);
  }
}
