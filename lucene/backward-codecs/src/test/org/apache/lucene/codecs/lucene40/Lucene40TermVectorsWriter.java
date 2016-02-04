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
package org.apache.lucene.codecs.lucene40;

import java.io.IOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

import static org.apache.lucene.codecs.lucene40.Lucene40TermVectorsReader.*;

/**
 * Writer for 4.0 term vectors format for testing
 * @deprecated for test purposes only
 */
@Deprecated
final class Lucene40TermVectorsWriter extends TermVectorsWriter {
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
        IOUtils.closeWhileHandlingException(this);
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
    lastTerm.clear();
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

  private final BytesRefBuilder lastTerm = new BytesRefBuilder();

  // NOTE: we override addProx, so we don't need to buffer when indexing.
  // we also don't buffer during bulk merges.
  private int offsetStartBuffer[] = new int[10];
  private int offsetEndBuffer[] = new int[10];
  private BytesRefBuilder payloadData = new BytesRefBuilder();
  private int bufferedIndex = 0;
  private int bufferedFreq = 0;
  private boolean positions = false;
  private boolean offsets = false;
  private boolean payloads = false;

  @Override
  public void startTerm(BytesRef term, int freq) throws IOException {
    final int prefix = StringHelper.bytesDifference(lastTerm.get(), term);
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
    payloadData.clear();
  }

  int lastPosition = 0;
  int lastOffset = 0;
  int lastPayloadLength = -1; // force first payload to write its length

  BytesRefBuilder scratch = new BytesRefBuilder(); // used only by this optimized flush below

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
          scratch.setLength(length);
          positions.readBytes(scratch.bytes(), 0, scratch.length());
          writePosition(code >>> 1, scratch.get());
        } else {
          writePosition(code >>> 1, null);
        }
      }
      tvf.writeBytes(payloadData.bytes(), 0, payloadData.length());
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
        tvf.writeBytes(payloadData.bytes(), 0, payloadData.length());
      }
      if (offsets) {
        for (int i = 0; i < bufferedIndex; i++) {
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
        if (payloadLength + payloadData.length() < 0) {
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
  public void finish(FieldInfos fis, int numDocs) {
    long indexFP = tvx.getFilePointer();
    if (HEADER_LENGTH_INDEX+((long) numDocs)*16 != indexFP)
      // This is most likely a bug in Sun JRE 1.6.0_04/_05;
      // we detect that the bug has struck, here, and
      // throw an exception to prevent the corruption from
      // entering the index.  See LUCENE-1282 for
      // details.
      throw new RuntimeException("tvx size mismatch: mergedDocs is " + numDocs + " but tvx size is " + indexFP + " (wrote numDocs=" + ((indexFP - HEADER_LENGTH_INDEX)/16.0) + " file=" + tvx.toString() + "; now aborting this merge to prevent index corruption");
  }

  /** Close all streams. */
  @Override
  public void close() throws IOException {
    // make an effort to close all streams we can but remember and re-throw
    // the first exception encountered in this process
    IOUtils.close(tvx, tvd, tvf);
    tvx = tvd = tvf = null;
  }
}
