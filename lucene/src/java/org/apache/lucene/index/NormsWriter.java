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
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.util.IOUtils;

// TODO FI: norms could actually be stored as doc store

/** Writes norms.  Each thread X field accumulates the norms
 *  for the doc/fields it saw, then the flush method below
 *  merges all of these together into a single _X.nrm file.
 */

final class NormsWriter extends InvertedDocEndConsumer {

  private final byte defaultNorm = Similarity.getDefault().encodeNormValue(1.0f);
  private FieldInfos fieldInfos;
  @Override
  public InvertedDocEndConsumerPerThread addThread(DocInverterPerThread docInverterPerThread) {
    return new NormsWriterPerThread(docInverterPerThread, this);
  }

  @Override
  public void abort() {}

  // We only write the _X.nrm file at flush
  void files(Collection<String> files) {}

  @Override
  void setFieldInfos(FieldInfos fieldInfos) {
    this.fieldInfos = fieldInfos;
  }

  /** Produce _X.nrm if any document had a field with norms
   *  not disabled */
  @Override
  public void flush(Map<InvertedDocEndConsumerPerThread,Collection<InvertedDocEndConsumerPerField>> threadsAndFields, SegmentWriteState state) throws IOException {

    final Map<FieldInfo,List<NormsWriterPerField>> byField = new HashMap<FieldInfo,List<NormsWriterPerField>>();

    // Typically, each thread will have encountered the same
    // field.  So first we collate by field, ie, all
    // per-thread field instances that correspond to the
    // same FieldInfo
    for (final Map.Entry<InvertedDocEndConsumerPerThread,Collection<InvertedDocEndConsumerPerField>> entry : threadsAndFields.entrySet()) {
      final Collection<InvertedDocEndConsumerPerField> fields = entry.getValue();
      final Iterator<InvertedDocEndConsumerPerField> fieldsIt = fields.iterator();

      while (fieldsIt.hasNext()) {
        final NormsWriterPerField perField = (NormsWriterPerField) fieldsIt.next();

        if (perField.upto > 0) {
          // It has some norms
          List<NormsWriterPerField> l = byField.get(perField.fieldInfo);
          if (l == null) {
            l = new ArrayList<NormsWriterPerField>();
            byField.put(perField.fieldInfo, l);
          }
          l.add(perField);
        } else
          // Remove this field since we haven't seen it
          // since the previous flush
          fieldsIt.remove();
      }
    }

    final String normsFileName = IndexFileNames.segmentFileName(state.segmentName, IndexFileNames.NORMS_EXTENSION);
    IndexOutput normsOut = state.directory.createOutput(normsFileName);
    boolean success = false;
    try {
      normsOut.writeBytes(SegmentNorms.NORMS_HEADER, 0, SegmentNorms.NORMS_HEADER.length);

      final int numField = fieldInfos.size();

      int normCount = 0;

      for(int fieldNumber=0;fieldNumber<numField;fieldNumber++) {

        final FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);

        List<NormsWriterPerField> toMerge = byField.get(fieldInfo);
        int upto = 0;
        if (toMerge != null) {

          final int numFields = toMerge.size();

          normCount++;

          final NormsWriterPerField[] fields = new NormsWriterPerField[numFields];
          int[] uptos = new int[numFields];

          for(int j=0;j<numFields;j++)
            fields[j] = toMerge.get(j);

          int numLeft = numFields;
              
          while(numLeft > 0) {

            assert uptos[0] < fields[0].docIDs.length : " uptos[0]=" + uptos[0] + " len=" + (fields[0].docIDs.length);

            int minLoc = 0;
            int minDocID = fields[0].docIDs[uptos[0]];

            for(int j=1;j<numLeft;j++) {
              final int docID = fields[j].docIDs[uptos[j]];
              if (docID < minDocID) {
                minDocID = docID;
                minLoc = j;
              }
            }

            assert minDocID < state.numDocs;

            // Fill hole
            for(;upto<minDocID;upto++)
              normsOut.writeByte(defaultNorm);

            normsOut.writeByte(fields[minLoc].norms[uptos[minLoc]]);
            (uptos[minLoc])++;
            upto++;

            if (uptos[minLoc] == fields[minLoc].upto) {
              fields[minLoc].reset();
              if (minLoc != numLeft-1) {
                fields[minLoc] = fields[numLeft-1];
                uptos[minLoc] = uptos[numLeft-1];
              }
              numLeft--;
            }
          }
          
          // Fill final hole with defaultNorm
          for(;upto<state.numDocs;upto++)
            normsOut.writeByte(defaultNorm);
        } else if (fieldInfo.isIndexed && !fieldInfo.omitNorms) {
          normCount++;
          // Fill entire field with default norm:
          for(;upto<state.numDocs;upto++)
            normsOut.writeByte(defaultNorm);
        }

        assert 4+normCount*state.numDocs == normsOut.getFilePointer() : ".nrm file size mismatch: expected=" + (4+normCount*state.numDocs) + " actual=" + normsOut.getFilePointer();
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(normsOut);
      } else {
        IOUtils.closeWhileHandlingException(normsOut);
      }
    }
  }
}
