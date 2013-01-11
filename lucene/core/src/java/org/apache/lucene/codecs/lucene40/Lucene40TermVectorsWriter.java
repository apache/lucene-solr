package org.apache.lucene.codecs.lucene40;

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
import java.util.Comparator;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

import static org.apache.lucene.codecs.lucene40.Lucene40TermVectorsReader.*;


// TODO: make a new 4.0 TV format that encodes better
//   - use startOffset (not endOffset) as base for delta on
//     next startOffset because today for syns or ngrams or
//     WDF or shingles etc. we are encoding negative vints
//     (= slow, 5 bytes per)
//   - if doc has no term vectors, write 0 into the tvx
//     file; saves a seek to tvd only to read a 0 vint (and
//     saves a byte in tvd)

/**
 * Lucene 4.0 Term Vectors writer.
 * <p>
 * It writes .tvd, .tvf, and .tvx files.
 * 
 * @see Lucene40TermVectorsFormat
 */
public final class Lucene40TermVectorsWriter extends TermVectorsWriter {
  private final Directory directory;
  private final String segment;
  private IndexOutput tvx = null, tvd = null, tvf = null;
  
  /** Sole constructor. */
  public Lucene40TermVectorsWriter(Directory directory, String segment, IOContext context) throws IOException {
    this.directory = directory;
    this.segment = segment;
    boolean success = false;
    try {
      // Open files for TermVector storage
      tvx = directory.createOutput(IndexFileNames.segmentFileName(segment, "", Lucene40TermVectorsReader.VECTORS_INDEX_EXTENSION), context);
      CodecUtil.writeHeader(tvx, CODEC_NAME_INDEX, VERSION_CURRENT);
      tvd = directory.createOutput(IndexFileNames.segmentFileName(segment, "", Lucene40TermVectorsReader.VECTORS_DOCUMENTS_EXTENSION), context);
      CodecUtil.writeHeader(tvd, CODEC_NAME_DOCS, VERSION_CURRENT);
      tvf = directory.createOutput(IndexFileNames.segmentFileName(segment, "", Lucene40TermVectorsReader.VECTORS_FIELDS_EXTENSION), context);
      CodecUtil.writeHeader(tvf, CODEC_NAME_FIELDS, VERSION_CURRENT);
      assert HEADER_LENGTH_INDEX == tvx.getFilePointer();
      assert HEADER_LENGTH_DOCS == tvd.getFilePointer();
      assert HEADER_LENGTH_FIELDS == tvf.getFilePointer();
      success = true;
    } finally {
      if (!success) {
        abort();
      }
    }
  }
 
  @Override
  public void startDocument(int numVectorFields) throws IOException {
    lastFieldName = null;
    this.numVectorFields = numVectorFields;
    tvx.writeLong(tvd.getFilePointer());
    tvx.writeLong(tvf.getFilePointer());
    tvd.writeVInt(numVectorFields);
    fieldCount = 0;
    fps = ArrayUtil.grow(fps, numVectorFields);
  }
  
  private long fps[] = new long[10]; // pointers to the tvf before writing each field 
  private int fieldCount = 0;        // number of fields we have written so far for this document
  private int numVectorFields = 0;   // total number of fields we will write for this document
  private String lastFieldName;

  @Override
  public void startField(FieldInfo info, int numTerms, boolean positions, boolean offsets, boolean payloads) throws IOException {
    assert lastFieldName == null || info.name.compareTo(lastFieldName) > 0: "fieldName=" + info.name + " lastFieldName=" + lastFieldName;
    lastFieldName = info.name;
    this.positions = positions;
    this.offsets = offsets;
    this.payloads = payloads;
    lastTerm.length = 0;
    lastPayloadLength = -1; // force first payload to write its length
    fps[fieldCount++] = tvf.getFilePointer();
    tvd.writeVInt(info.number);
    tvf.writeVInt(numTerms);
    byte bits = 0x0;
    if (positions)
      bits |= Lucene40TermVectorsReader.STORE_POSITIONS_WITH_TERMVECTOR;
    if (offsets)
      bits |= Lucene40TermVectorsReader.STORE_OFFSET_WITH_TERMVECTOR;
    if (payloads)
      bits |= Lucene40TermVectorsReader.STORE_PAYLOAD_WITH_TERMVECTOR;
    tvf.writeByte(bits);
  }
  
  @Override
  public void finishDocument() throws IOException {
    assert fieldCount == numVectorFields;
    for (int i = 1; i < fieldCount; i++) {
      tvd.writeVLong(fps[i] - fps[i-1]);
    }
  }

  private final BytesRef lastTerm = new BytesRef(10);

  // NOTE: we override addProx, so we don't need to buffer when indexing.
  // we also don't buffer during bulk merges.
  private int offsetStartBuffer[] = new int[10];
  private int offsetEndBuffer[] = new int[10];
  private BytesRef payloadData = new BytesRef(10);
  private int bufferedIndex = 0;
  private int bufferedFreq = 0;
  private boolean positions = false;
  private boolean offsets = false;
  private boolean payloads = false;

  @Override
  public void startTerm(BytesRef term, int freq) throws IOException {
    final int prefix = StringHelper.bytesDifference(lastTerm, term);
    final int suffix = term.length - prefix;
    tvf.writeVInt(prefix);
    tvf.writeVInt(suffix);
    tvf.writeBytes(term.bytes, term.offset + prefix, suffix);
    tvf.writeVInt(freq);
    lastTerm.copyBytes(term);
    lastPosition = lastOffset = 0;
    
    if (offsets && positions) {
      // we might need to buffer if its a non-bulk merge
      offsetStartBuffer = ArrayUtil.grow(offsetStartBuffer, freq);
      offsetEndBuffer = ArrayUtil.grow(offsetEndBuffer, freq);
    }
    bufferedIndex = 0;
    bufferedFreq = freq;
    payloadData.length = 0;
  }

  int lastPosition = 0;
  int lastOffset = 0;
  int lastPayloadLength = -1; // force first payload to write its length

  BytesRef scratch = new BytesRef(); // used only by this optimized flush below

  @Override
  public void addProx(int numProx, DataInput positions, DataInput offsets) throws IOException {
    if (payloads) {
      // TODO, maybe overkill and just call super.addProx() in this case?
      // we do avoid buffering the offsets in RAM though.
      for (int i = 0; i < numProx; i++) {
        int code = positions.readVInt();
        if ((code & 1) == 1) {
          int length = positions.readVInt();
          scratch.grow(length);
          scratch.length = length;
          positions.readBytes(scratch.bytes, scratch.offset, scratch.length);
          writePosition(code >>> 1, scratch);
        } else {
          writePosition(code >>> 1, null);
        }
      }
      tvf.writeBytes(payloadData.bytes, payloadData.offset, payloadData.length);
    } else if (positions != null) {
      // pure positions, no payloads
      for (int i = 0; i < numProx; i++) {
        tvf.writeVInt(positions.readVInt() >>> 1);
      }
    }
    
    if (offsets != null) {
      for (int i = 0; i < numProx; i++) {
        tvf.writeVInt(offsets.readVInt());
        tvf.writeVInt(offsets.readVInt());
      }
    }
  }

  @Override
  public void addPosition(int position, int startOffset, int endOffset, BytesRef payload) throws IOException {
    if (positions && (offsets || payloads)) {
      // write position delta
      writePosition(position - lastPosition, payload);
      lastPosition = position;
      
      // buffer offsets
      if (offsets) {
        offsetStartBuffer[bufferedIndex] = startOffset;
        offsetEndBuffer[bufferedIndex] = endOffset;
      }
      
      bufferedIndex++;
    } else if (positions) {
      // write position delta
      writePosition(position - lastPosition, payload);
      lastPosition = position;
    } else if (offsets) {
      // write offset deltas
      tvf.writeVInt(startOffset - lastOffset);
      tvf.writeVInt(endOffset - startOffset);
      lastOffset = endOffset;
    }
  }
  
  @Override
  public void finishTerm() throws IOException {
    if (bufferedIndex > 0) {
      // dump buffer
      assert positions && (offsets || payloads);
      assert bufferedIndex == bufferedFreq;
      if (payloads) {
        tvf.writeBytes(payloadData.bytes, payloadData.offset, payloadData.length);
      }
      for (int i = 0; i < bufferedIndex; i++) {
        if (offsets) {
          tvf.writeVInt(offsetStartBuffer[i] - lastOffset);
          tvf.writeVInt(offsetEndBuffer[i] - offsetStartBuffer[i]);
          lastOffset = offsetEndBuffer[i];
        }
      }
    }
  }

  private void writePosition(int delta, BytesRef payload) throws IOException {
    if (payloads) {
      int payloadLength = payload == null ? 0 : payload.length;

      if (payloadLength != lastPayloadLength) {
        lastPayloadLength = payloadLength;
        tvf.writeVInt((delta<<1)|1);
        tvf.writeVInt(payloadLength);
      } else {
        tvf.writeVInt(delta << 1);
      }
      if (payloadLength > 0) {
        if (payloadLength + payloadData.length < 0) {
          // we overflowed the payload buffer, just throw UOE
          // having > Integer.MAX_VALUE bytes of payload for a single term in a single doc is nuts.
          throw new UnsupportedOperationException("A term cannot have more than Integer.MAX_VALUE bytes of payload data in a single document");
        }
        payloadData.append(payload);
      }
    } else {
      tvf.writeVInt(delta);
    }
  }

  @Override
  public void abort() {
    try {
      close();
    } catch (Throwable ignored) {}
    IOUtils.deleteFilesIgnoringExceptions(directory, IndexFileNames.segmentFileName(segment, "", Lucene40TermVectorsReader.VECTORS_INDEX_EXTENSION),
        IndexFileNames.segmentFileName(segment, "", Lucene40TermVectorsReader.VECTORS_DOCUMENTS_EXTENSION),
        IndexFileNames.segmentFileName(segment, "", Lucene40TermVectorsReader.VECTORS_FIELDS_EXTENSION));
  }

  /**
   * Do a bulk copy of numDocs documents from reader to our
   * streams.  This is used to expedite merging, if the
   * field numbers are congruent.
   */
  private void addRawDocuments(Lucene40TermVectorsReader reader, int[] tvdLengths, int[] tvfLengths, int numDocs) throws IOException {
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

  @Override
  public final int merge(MergeState mergeState) throws IOException {
    // Used for bulk-reading raw bytes for term vectors
    int rawDocLengths[] = new int[MAX_RAW_MERGE_DOCS];
    int rawDocLengths2[] = new int[MAX_RAW_MERGE_DOCS];

    int idx = 0;
    int numDocs = 0;
    for (int i = 0; i < mergeState.readers.size(); i++) {
      final AtomicReader reader = mergeState.readers.get(i);

      final SegmentReader matchingSegmentReader = mergeState.matchingSegmentReaders[idx++];
      Lucene40TermVectorsReader matchingVectorsReader = null;
      if (matchingSegmentReader != null) {
        TermVectorsReader vectorsReader = matchingSegmentReader.getTermVectorsReader();

        if (vectorsReader != null && vectorsReader instanceof Lucene40TermVectorsReader) {
            matchingVectorsReader = (Lucene40TermVectorsReader) vectorsReader;
        }
      }
      if (reader.getLiveDocs() != null) {
        numDocs += copyVectorsWithDeletions(mergeState, matchingVectorsReader, reader, rawDocLengths, rawDocLengths2);
      } else {
        numDocs += copyVectorsNoDeletions(mergeState, matchingVectorsReader, reader, rawDocLengths, rawDocLengths2);
      }
    }
    finish(mergeState.fieldInfos, numDocs);
    return numDocs;
  }

  /** Maximum number of contiguous documents to bulk-copy
      when merging term vectors */
  private final static int MAX_RAW_MERGE_DOCS = 4192;

  private int copyVectorsWithDeletions(MergeState mergeState,
                                        final Lucene40TermVectorsReader matchingVectorsReader,
                                        final AtomicReader reader,
                                        int rawDocLengths[],
                                        int rawDocLengths2[])
          throws IOException {
    final int maxDoc = reader.maxDoc();
    final Bits liveDocs = reader.getLiveDocs();
    int totalNumDocs = 0;
    if (matchingVectorsReader != null) {
      // We can bulk-copy because the fieldInfos are "congruent"
      for (int docNum = 0; docNum < maxDoc;) {
        if (!liveDocs.get(docNum)) {
          // skip deleted docs
          ++docNum;
          continue;
        }
        // We can optimize this case (doing a bulk byte copy) since the field
        // numbers are identical
        int start = docNum, numDocs = 0;
        do {
          docNum++;
          numDocs++;
          if (docNum >= maxDoc) break;
          if (!liveDocs.get(docNum)) {
            docNum++;
            break;
          }
        } while(numDocs < MAX_RAW_MERGE_DOCS);
        
        matchingVectorsReader.rawDocs(rawDocLengths, rawDocLengths2, start, numDocs);
        addRawDocuments(matchingVectorsReader, rawDocLengths, rawDocLengths2, numDocs);
        totalNumDocs += numDocs;
        mergeState.checkAbort.work(300 * numDocs);
      }
    } else {
      for (int docNum = 0; docNum < maxDoc; docNum++) {
        if (!liveDocs.get(docNum)) {
          // skip deleted docs
          continue;
        }
        
        // NOTE: it's very important to first assign to vectors then pass it to
        // termVectorsWriter.addAllDocVectors; see LUCENE-1282
        Fields vectors = reader.getTermVectors(docNum);
        addAllDocVectors(vectors, mergeState);
        totalNumDocs++;
        mergeState.checkAbort.work(300);
      }
    }
    return totalNumDocs;
  }
  
  private int copyVectorsNoDeletions(MergeState mergeState,
                                      final Lucene40TermVectorsReader matchingVectorsReader,
                                      final AtomicReader reader,
                                      int rawDocLengths[],
                                      int rawDocLengths2[])
          throws IOException {
    final int maxDoc = reader.maxDoc();
    if (matchingVectorsReader != null) {
      // We can bulk-copy because the fieldInfos are "congruent"
      int docCount = 0;
      while (docCount < maxDoc) {
        int len = Math.min(MAX_RAW_MERGE_DOCS, maxDoc - docCount);
        matchingVectorsReader.rawDocs(rawDocLengths, rawDocLengths2, docCount, len);
        addRawDocuments(matchingVectorsReader, rawDocLengths, rawDocLengths2, len);
        docCount += len;
        mergeState.checkAbort.work(300 * len);
      }
    } else {
      for (int docNum = 0; docNum < maxDoc; docNum++) {
        // NOTE: it's very important to first assign to vectors then pass it to
        // termVectorsWriter.addAllDocVectors; see LUCENE-1282
        Fields vectors = reader.getTermVectors(docNum);
        addAllDocVectors(vectors, mergeState);
        mergeState.checkAbort.work(300);
      }
    }
    return maxDoc;
  }
  
  @Override
  public void finish(FieldInfos fis, int numDocs) {
    if (HEADER_LENGTH_INDEX+((long) numDocs)*16 != tvx.getFilePointer())
      // This is most likely a bug in Sun JRE 1.6.0_04/_05;
      // we detect that the bug has struck, here, and
      // throw an exception to prevent the corruption from
      // entering the index.  See LUCENE-1282 for
      // details.
      throw new RuntimeException("tvx size mismatch: mergedDocs is " + numDocs + " but tvx size is " + tvx.getFilePointer() + " file=" + tvx.toString() + "; now aborting this merge to prevent index corruption");
  }

  /** Close all streams. */
  @Override
  public void close() throws IOException {
    // make an effort to close all streams we can but remember and re-throw
    // the first exception encountered in this process
    IOUtils.close(tvx, tvd, tvf);
    tvx = tvd = tvf = null;
  }

  @Override
  public Comparator<BytesRef> getComparator() {
    return BytesRef.getUTF8SortedAsUnicodeComparator();
  }
}
