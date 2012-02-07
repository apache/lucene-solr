package org.apache.lucene.codecs.lucene3x;

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

import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

final class PreFlexRWTermVectorsWriter extends TermVectorsWriter {
  private final Directory directory;
  private final String segment;
  private IndexOutput tvx = null, tvd = null, tvf = null;

  public PreFlexRWTermVectorsWriter(Directory directory, String segment, IOContext context) throws IOException {
    this.directory = directory;
    this.segment = segment;
    boolean success = false;
    try {
      // Open files for TermVector storage
      tvx = directory.createOutput(IndexFileNames.segmentFileName(segment, "", Lucene3xTermVectorsReader.VECTORS_INDEX_EXTENSION), context);
      tvx.writeInt(Lucene3xTermVectorsReader.FORMAT_CURRENT);
      tvd = directory.createOutput(IndexFileNames.segmentFileName(segment, "", Lucene3xTermVectorsReader.VECTORS_DOCUMENTS_EXTENSION), context);
      tvd.writeInt(Lucene3xTermVectorsReader.FORMAT_CURRENT);
      tvf = directory.createOutput(IndexFileNames.segmentFileName(segment, "", Lucene3xTermVectorsReader.VECTORS_FIELDS_EXTENSION), context);
      tvf.writeInt(Lucene3xTermVectorsReader.FORMAT_CURRENT);
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
      bits |= Lucene3xTermVectorsReader.STORE_POSITIONS_WITH_TERMVECTOR;
    if (offsets)
      bits |= Lucene3xTermVectorsReader.STORE_OFFSET_WITH_TERMVECTOR;
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
    IOUtils.deleteFilesIgnoringExceptions(directory, IndexFileNames.segmentFileName(segment, "", Lucene3xTermVectorsReader.VECTORS_INDEX_EXTENSION),
        IndexFileNames.segmentFileName(segment, "", Lucene3xTermVectorsReader.VECTORS_DOCUMENTS_EXTENSION),
        IndexFileNames.segmentFileName(segment, "", Lucene3xTermVectorsReader.VECTORS_FIELDS_EXTENSION));
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
    return BytesRef.getUTF8SortedAsUTF16Comparator();
  }
}
