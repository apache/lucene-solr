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
import java.util.Comparator;

import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergePolicy.MergeAbortedException;
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

// TODO: make a new 4.0 TV format that encodes better
//   - use startOffset (not endOffset) as base for delta on
//     next startOffset because today for syns or ngrams or
//     WDF or shingles etc. we are encoding negative vints
//     (= slow, 5 bytes per)
//   - if doc has no term vectors, write 0 into the tvx
//     file; saves a seek to tvd only to read a 0 vint (and
//     saves a byte in tvd)

public final class Lucene40TermVectorsWriter extends TermVectorsWriter {
  private final Directory directory;
  private final String segment;
  private IndexOutput tvx = null, tvd = null, tvf = null;

  public Lucene40TermVectorsWriter(Directory directory, String segment, IOContext context) throws IOException {
    this.directory = directory;
    this.segment = segment;
    boolean success = false;
    try {
      // Open files for TermVector storage
      tvx = directory.createOutput(IndexFileNames.segmentFileName(segment, "", Lucene40TermVectorsReader.VECTORS_INDEX_EXTENSION), context);
      tvx.writeInt(Lucene40TermVectorsReader.FORMAT_CURRENT);
      tvd = directory.createOutput(IndexFileNames.segmentFileName(segment, "", Lucene40TermVectorsReader.VECTORS_DOCUMENTS_EXTENSION), context);
      tvd.writeInt(Lucene40TermVectorsReader.FORMAT_CURRENT);
      tvf = directory.createOutput(IndexFileNames.segmentFileName(segment, "", Lucene40TermVectorsReader.VECTORS_FIELDS_EXTENSION), context);
      tvf.writeInt(Lucene40TermVectorsReader.FORMAT_CURRENT);
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
  public void startField(FieldInfo info, int numTerms, boolean positions, boolean offsets) throws IOException {
    assert lastFieldName == null || info.name.compareTo(lastFieldName) > 0: "fieldName=" + info.name + " lastFieldName=" + lastFieldName;
    lastFieldName = info.name;
    this.positions = positions;
    this.offsets = offsets;
    lastTerm.length = 0;
    fps[fieldCount++] = tvf.getFilePointer();
    tvd.writeVInt(info.number);
    tvf.writeVInt(numTerms);
    byte bits = 0x0;
    if (positions)
      bits |= Lucene40TermVectorsReader.STORE_POSITIONS_WITH_TERMVECTOR;
    if (offsets)
      bits |= Lucene40TermVectorsReader.STORE_OFFSET_WITH_TERMVECTOR;
    tvf.writeByte(bits);
    
    assert fieldCount <= numVectorFields;
    if (fieldCount == numVectorFields) {
      // last field of the document
      // this is crazy because the file format is crazy!
      for (int i = 1; i < fieldCount; i++) {
        tvd.writeVLong(fps[i] - fps[i-1]);
      }
    }
  }
  
  private final BytesRef lastTerm = new BytesRef(10);

  // NOTE: we override addProx, so we don't need to buffer when indexing.
  // we also don't buffer during bulk merges.
  private int offsetStartBuffer[] = new int[10];
  private int offsetEndBuffer[] = new int[10];
  private int offsetIndex = 0;
  private int offsetFreq = 0;
  private boolean positions = false;
  private boolean offsets = false;

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
      offsetIndex = 0;
      offsetFreq = freq;
    }
  }

  int lastPosition = 0;
  int lastOffset = 0;

  @Override
  public void addProx(int numProx, DataInput positions, DataInput offsets) throws IOException {
    // TODO: technically we could just copy bytes and not re-encode if we knew the length...
    if (positions != null) {
      for (int i = 0; i < numProx; i++) {
        tvf.writeVInt(positions.readVInt());
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
  public void addPosition(int position, int startOffset, int endOffset) throws IOException {
    if (positions && offsets) {
      // write position delta
      tvf.writeVInt(position - lastPosition);
      lastPosition = position;
      
      // buffer offsets
      offsetStartBuffer[offsetIndex] = startOffset;
      offsetEndBuffer[offsetIndex] = endOffset;
      offsetIndex++;
      
      // dump buffer if we are done
      if (offsetIndex == offsetFreq) {
        for (int i = 0; i < offsetIndex; i++) {
          tvf.writeVInt(offsetStartBuffer[i] - lastOffset);
          tvf.writeVInt(offsetEndBuffer[i] - offsetStartBuffer[i]);
          lastOffset = offsetEndBuffer[i];
        }
      }
    } else if (positions) {
      // write position delta
      tvf.writeVInt(position - lastPosition);
      lastPosition = position;
    } else if (offsets) {
      // write offset deltas
      tvf.writeVInt(startOffset - lastOffset);
      tvf.writeVInt(endOffset - startOffset);
      lastOffset = endOffset;
    }
  }

  @Override
  public void abort() {
    try {
      close();
    } catch (IOException ignored) {}
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
    for (final MergeState.IndexReaderAndLiveDocs reader : mergeState.readers) {
      final SegmentReader matchingSegmentReader = mergeState.matchingSegmentReaders[idx++];
      Lucene40TermVectorsReader matchingVectorsReader = null;
      if (matchingSegmentReader != null) {
        TermVectorsReader vectorsReader = matchingSegmentReader.getTermVectorsReader();

        if (vectorsReader != null && vectorsReader instanceof Lucene40TermVectorsReader) {
          // If the TV* files are an older format then they cannot read raw docs:
          if (((Lucene40TermVectorsReader)vectorsReader).canReadRawDocs()) {
            matchingVectorsReader = (Lucene40TermVectorsReader) vectorsReader;
          }
        }
      }
      if (reader.liveDocs != null) {
        numDocs += copyVectorsWithDeletions(mergeState, matchingVectorsReader, reader, rawDocLengths, rawDocLengths2);
      } else {
        numDocs += copyVectorsNoDeletions(mergeState, matchingVectorsReader, reader, rawDocLengths, rawDocLengths2);
      }
    }
    finish(numDocs);
    return numDocs;
  }

  /** Maximum number of contiguous documents to bulk-copy
      when merging term vectors */
  private final static int MAX_RAW_MERGE_DOCS = 4192;

  private int copyVectorsWithDeletions(MergeState mergeState,
                                        final Lucene40TermVectorsReader matchingVectorsReader,
                                        final MergeState.IndexReaderAndLiveDocs reader,
                                        int rawDocLengths[],
                                        int rawDocLengths2[])
          throws IOException, MergeAbortedException {
    final int maxDoc = reader.reader.maxDoc();
    final Bits liveDocs = reader.liveDocs;
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
        Fields vectors = reader.reader.getTermVectors(docNum);
        addAllDocVectors(vectors, mergeState.fieldInfos);
        totalNumDocs++;
        mergeState.checkAbort.work(300);
      }
    }
    return totalNumDocs;
  }
  
  private int copyVectorsNoDeletions(MergeState mergeState,
                                      final Lucene40TermVectorsReader matchingVectorsReader,
                                      final MergeState.IndexReaderAndLiveDocs reader,
                                      int rawDocLengths[],
                                      int rawDocLengths2[])
          throws IOException, MergeAbortedException {
    final int maxDoc = reader.reader.maxDoc();
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
        Fields vectors = reader.reader.getTermVectors(docNum);
        addAllDocVectors(vectors, mergeState.fieldInfos);
        mergeState.checkAbort.work(300);
      }
    }
    return maxDoc;
  }
  
  @Override
  public void finish(int numDocs) throws IOException {
    if (4+((long) numDocs)*16 != tvx.getFilePointer())
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
  public Comparator<BytesRef> getComparator() throws IOException {
    return BytesRef.getUTF8SortedAsUnicodeComparator();
  }
}
