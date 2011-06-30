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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;

final class TermVectorsWriter {

  private IndexOutput tvx = null, tvd = null, tvf = null;
  private FieldInfos fieldInfos;

  public TermVectorsWriter(Directory directory, String segment,
                           FieldInfos fieldInfos, IOContext context) throws IOException {
    boolean success = false;
    try {
      // Open files for TermVector storage
      tvx = directory.createOutput(IndexFileNames.segmentFileName(segment, "", IndexFileNames.VECTORS_INDEX_EXTENSION), context);
      tvx.writeInt(TermVectorsReader.FORMAT_CURRENT);
      tvd = directory.createOutput(IndexFileNames.segmentFileName(segment, "", IndexFileNames.VECTORS_DOCUMENTS_EXTENSION), context);
      tvd.writeInt(TermVectorsReader.FORMAT_CURRENT);
      tvf = directory.createOutput(IndexFileNames.segmentFileName(segment, "", IndexFileNames.VECTORS_FIELDS_EXTENSION), context);
      tvf.writeInt(TermVectorsReader.FORMAT_CURRENT);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeSafely(true, tvx, tvd, tvf);
      }
    }

    this.fieldInfos = fieldInfos;
  }

  /**
   * Add a complete document specified by all its term vectors. If document has no
   * term vectors, add value for tvx.
   *
   * @param vectors
   * @throws IOException
   */
  public final void addAllDocVectors(TermFreqVector[] vectors) throws IOException {

    tvx.writeLong(tvd.getFilePointer());
    tvx.writeLong(tvf.getFilePointer());

    if (vectors != null) {
      final int numFields = vectors.length;
      tvd.writeVInt(numFields);

      long[] fieldPointers = new long[numFields];

      for (int i=0; i<numFields; i++) {
        fieldPointers[i] = tvf.getFilePointer();

        final int fieldNumber = fieldInfos.fieldNumber(vectors[i].getField());

        // 1st pass: write field numbers to tvd
        tvd.writeVInt(fieldNumber);

        final int numTerms = vectors[i].size();
        tvf.writeVInt(numTerms);

        final TermPositionVector tpVector;

        final byte bits;
        final boolean storePositions;
        final boolean storeOffsets;

        if (vectors[i] instanceof TermPositionVector) {
          // May have positions & offsets
          tpVector = (TermPositionVector) vectors[i];
          storePositions = tpVector.size() > 0 && tpVector.getTermPositions(0) != null;
          storeOffsets = tpVector.size() > 0 && tpVector.getOffsets(0) != null;
          bits = (byte) ((storePositions ? TermVectorsReader.STORE_POSITIONS_WITH_TERMVECTOR : 0) +
                         (storeOffsets ? TermVectorsReader.STORE_OFFSET_WITH_TERMVECTOR : 0));
        } else {
          tpVector = null;
          bits = 0;
          storePositions = false;
          storeOffsets = false;
        }

        tvf.writeVInt(bits);

        final BytesRef[] terms = vectors[i].getTerms();
        final int[] freqs = vectors[i].getTermFrequencies();

        for (int j=0; j<numTerms; j++) {

          int start = j == 0 ? 0 : StringHelper.bytesDifference(terms[j-1].bytes,
                                                   terms[j-1].length,
                                                   terms[j].bytes,
                                                   terms[j].length);
          int length = terms[j].length - start;
          tvf.writeVInt(start);       // write shared prefix length
          tvf.writeVInt(length);        // write delta length
          tvf.writeBytes(terms[j].bytes, start, length);  // write delta bytes

          final int termFreq = freqs[j];

          tvf.writeVInt(termFreq);

          if (storePositions) {
            final int[] positions = tpVector.getTermPositions(j);
            if (positions == null)
              throw new IllegalStateException("Trying to write positions that are null!");
            assert positions.length == termFreq;

            // use delta encoding for positions
            int lastPosition = 0;
            for(int k=0;k<positions.length;k++) {
              final int position = positions[k];
              tvf.writeVInt(position-lastPosition);
              lastPosition = position;
            }
          }

          if (storeOffsets) {
            final TermVectorOffsetInfo[] offsets = tpVector.getOffsets(j);
            if (offsets == null)
              throw new IllegalStateException("Trying to write offsets that are null!");
            assert offsets.length == termFreq;

            // use delta encoding for offsets
            int lastEndOffset = 0;
            for(int k=0;k<offsets.length;k++) {
              final int startOffset = offsets[k].getStartOffset();
              final int endOffset = offsets[k].getEndOffset();
              tvf.writeVInt(startOffset-lastEndOffset);
              tvf.writeVInt(endOffset-startOffset);
              lastEndOffset = endOffset;
            }
          }
        }
      }

      // 2nd pass: write field pointers to tvd
      if (numFields > 1) {
        long lastFieldPointer = fieldPointers[0];
        for (int i=1; i<numFields; i++) {
          final long fieldPointer = fieldPointers[i];
          tvd.writeVLong(fieldPointer-lastFieldPointer);
          lastFieldPointer = fieldPointer;
        }
      }
    } else
      tvd.writeVInt(0);
  }

  /**
   * Do a bulk copy of numDocs documents from reader to our
   * streams.  This is used to expedite merging, if the
   * field numbers are congruent.
   */
  final void addRawDocuments(TermVectorsReader reader, int[] tvdLengths, int[] tvfLengths, int numDocs) throws IOException {
    long tvdPosition = tvd.getFilePointer();
    long tvfPosition = tvf.getFilePointer();
    long tvdStart = tvdPosition;
    long tvfStart = tvfPosition;
    for(int i=0;i<numDocs;i++) {
      tvx.writeLong(tvdPosition);
      tvdPosition += tvdLengths[i];
      tvx.writeLong(tvfPosition);
      tvfPosition += tvfLengths[i];
    }
    tvd.copyBytes(reader.getTvdStream(), tvdPosition-tvdStart);
    tvf.copyBytes(reader.getTvfStream(), tvfPosition-tvfStart);
    assert tvd.getFilePointer() == tvdPosition;
    assert tvf.getFilePointer() == tvfPosition;
  }

  /** Close all streams. */
  final void close() throws IOException {
    // make an effort to close all streams we can but remember and re-throw
    // the first exception encountered in this process
    IOUtils.closeSafely(false, tvx, tvd, tvf);
  }
}
