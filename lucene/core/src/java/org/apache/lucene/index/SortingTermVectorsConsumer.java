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
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

final class SortingTermVectorsConsumer extends TermVectorsConsumer {
  TrackingTmpOutputDirectoryWrapper tmpDirectory;

  public SortingTermVectorsConsumer(DocumentsWriterPerThread docWriter) {
    super(docWriter);
  }

  @Override
  void flush(Map<String, TermsHashPerField> fieldsToFlush, final SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    super.flush(fieldsToFlush, state, sortMap);
    if (tmpDirectory != null) {
      if (sortMap == null) {
        // we're lucky the index is already sorted, just rename the temporary file and return
        for (Map.Entry<String, String> entry : tmpDirectory.getTemporaryFiles().entrySet()) {
          tmpDirectory.rename(entry.getValue(), entry.getKey());
        }
        return;
      }
      TermVectorsReader reader = docWriter.codec.termVectorsFormat()
          .vectorsReader(tmpDirectory, state.segmentInfo, state.fieldInfos, IOContext.DEFAULT);
      TermVectorsReader mergeReader = reader.getMergeInstance();
      TermVectorsWriter writer = docWriter.codec.termVectorsFormat()
          .vectorsWriter(state.directory, state.segmentInfo, IOContext.DEFAULT);
      try {
        reader.checkIntegrity();
        for (int docID = 0; docID < state.segmentInfo.maxDoc(); docID++) {
          Fields vectors = mergeReader.get(sortMap.newToOld(docID));
          writeTermVectors(writer, vectors, state.fieldInfos);
        }
        writer.finish(state.fieldInfos, state.segmentInfo.maxDoc());
      } finally {
        IOUtils.close(reader, writer);
        IOUtils.deleteFiles(tmpDirectory,
            tmpDirectory.getTemporaryFiles().values());
      }
    }
  }

  @Override
  void initTermVectorsWriter() throws IOException {
    if (writer == null) {
      IOContext context = new IOContext(new FlushInfo(docWriter.getNumDocsInRAM(), docWriter.bytesUsed()));
      tmpDirectory = new TrackingTmpOutputDirectoryWrapper(docWriter.directory);
      writer = docWriter.codec.termVectorsFormat().vectorsWriter(tmpDirectory, docWriter.getSegmentInfo(), context);
      lastDocID = 0;
    }
  }

  @Override
  public void abort() {
    try {
      super.abort();
    } finally {
      IOUtils.deleteFilesIgnoringExceptions(tmpDirectory,
          tmpDirectory.getTemporaryFiles().values());
    }
  }

  /** Safe (but, slowish) default method to copy every vector field in the provided {@link TermVectorsWriter}. */
  private static void writeTermVectors(TermVectorsWriter writer, Fields vectors, FieldInfos fieldInfos) throws IOException {
    if (vectors == null) {
      writer.startDocument(0);
      writer.finishDocument();
      return;
    }

    int numFields = vectors.size();
    if (numFields == -1) {
      // count manually! TODO: Maybe enforce that Fields.size() returns something valid?
      numFields = 0;
      for (final Iterator<String> it = vectors.iterator(); it.hasNext(); ) {
        it.next();
        numFields++;
      }
    }
    writer.startDocument(numFields);

    String lastFieldName = null;

    TermsEnum termsEnum = null;
    PostingsEnum docsAndPositionsEnum = null;

    int fieldCount = 0;
    for(String fieldName : vectors) {
      fieldCount++;
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldName);

      assert lastFieldName == null || fieldName.compareTo(lastFieldName) > 0: "lastFieldName=" + lastFieldName + " fieldName=" + fieldName;
      lastFieldName = fieldName;

      final Terms terms = vectors.terms(fieldName);
      if (terms == null) {
        // FieldsEnum shouldn't lie...
        continue;
      }

      final boolean hasPositions = terms.hasPositions();
      final boolean hasOffsets = terms.hasOffsets();
      final boolean hasPayloads = terms.hasPayloads();
      assert !hasPayloads || hasPositions;

      int numTerms = (int) terms.size();
      if (numTerms == -1) {
        // count manually. It is stupid, but needed, as Terms.size() is not a mandatory statistics function
        numTerms = 0;
        termsEnum = terms.iterator();
        while(termsEnum.next() != null) {
          numTerms++;
        }
      }

      writer.startField(fieldInfo, numTerms, hasPositions, hasOffsets, hasPayloads);
      termsEnum = terms.iterator();

      int termCount = 0;
      while(termsEnum.next() != null) {
        termCount++;

        final int freq = (int) termsEnum.totalTermFreq();

        writer.startTerm(termsEnum.term(), freq);

        if (hasPositions || hasOffsets) {
          docsAndPositionsEnum = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.OFFSETS | PostingsEnum.PAYLOADS);
          assert docsAndPositionsEnum != null;

          final int docID = docsAndPositionsEnum.nextDoc();
          assert docID != DocIdSetIterator.NO_MORE_DOCS;
          assert docsAndPositionsEnum.freq() == freq;

          for(int posUpto=0; posUpto<freq; posUpto++) {
            final int pos = docsAndPositionsEnum.nextPosition();
            final int startOffset = docsAndPositionsEnum.startOffset();
            final int endOffset = docsAndPositionsEnum.endOffset();

            final BytesRef payload = docsAndPositionsEnum.getPayload();

            assert !hasPositions || pos >= 0 ;
            writer.addPosition(pos, startOffset, endOffset, payload);
          }
        }
        writer.finishTerm();
      }
      assert termCount == numTerms;
      writer.finishField();
    }
    assert fieldCount == numFields;
    writer.finishDocument();
  }
}
