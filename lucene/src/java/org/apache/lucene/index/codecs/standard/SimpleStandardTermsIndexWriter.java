package org.apache.lucene.index.codecs.standard;

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

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.packed.PackedInts;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

/** @lucene.experimental */
public class SimpleStandardTermsIndexWriter extends StandardTermsIndexWriter {
  protected final IndexOutput out;

  final static String CODEC_NAME = "SIMPLE_STANDARD_TERMS_INDEX";
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  final private int termIndexInterval;

  private final List<SimpleFieldWriter> fields = new ArrayList<SimpleFieldWriter>();
  private final FieldInfos fieldInfos; // unread
  private IndexOutput termsOut;

  public SimpleStandardTermsIndexWriter(SegmentWriteState state) throws IOException {
    final String indexFileName = IndexFileNames.segmentFileName(state.segmentName, "", StandardCodec.TERMS_INDEX_EXTENSION);
    state.flushedFiles.add(indexFileName);
    termIndexInterval = state.termIndexInterval;
    out = state.directory.createOutput(indexFileName);
    fieldInfos = state.fieldInfos;
    writeHeader(out);
    out.writeInt(termIndexInterval);
  }
  
  protected void writeHeader(IndexOutput out) throws IOException {
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
    // Placeholder for dir offset
    out.writeLong(0);
  }

  @Override
  public void setTermsOutput(IndexOutput termsOut) {
    this.termsOut = termsOut;
  }
  
  @Override
  public FieldWriter addField(FieldInfo field) {
    SimpleFieldWriter writer = new SimpleFieldWriter(field);
    fields.add(writer);
    return writer;
  }

  private class SimpleFieldWriter extends FieldWriter {
    final FieldInfo fieldInfo;
    int numIndexTerms;
    final long indexStart;
    final long termsStart;
    long packedIndexStart;
    long packedOffsetsStart;
    private int numTerms;

    // TODO: we could conceivably make a PackedInts wrapper
    // that auto-grows... then we wouldn't force 6 bytes RAM
    // per index term:
    private short[] termLengths;
    private int[] termsPointerDeltas;
    private long lastTermsPointer;
    private long totTermLength;

    SimpleFieldWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      indexStart = out.getFilePointer();
      termsStart = lastTermsPointer = termsOut.getFilePointer();
      termLengths = new short[0];
      termsPointerDeltas = new int[0];
    }

    @Override
    public boolean checkIndexTerm(BytesRef text, int docFreq) throws IOException {
      // First term is first indexed term:
      if (0 == (numTerms++ % termIndexInterval)) {

        // write full bytes
        out.writeBytes(text.bytes, text.offset, text.length);

        if (termLengths.length == numIndexTerms) {
          termLengths = ArrayUtil.grow(termLengths);
        }
        if (termsPointerDeltas.length == numIndexTerms) {
          termsPointerDeltas = ArrayUtil.grow(termsPointerDeltas);
        }

        // save delta terms pointer
        final long fp = termsOut.getFilePointer();
        termsPointerDeltas[numIndexTerms] = (int) (fp - lastTermsPointer);
        lastTermsPointer = fp;

        // save term length (in bytes)
        assert text.length <= Short.MAX_VALUE;
        termLengths[numIndexTerms] = (short) text.length;

        totTermLength += text.length;

        numIndexTerms++;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void finish() throws IOException {

      // write primary terms dict offsets
      packedIndexStart = out.getFilePointer();

      final long maxValue = termsOut.getFilePointer();
      PackedInts.Writer w = PackedInts.getWriter(out, numIndexTerms, PackedInts.bitsRequired(maxValue));

      // relative to our indexStart
      long upto = 0;
      for(int i=0;i<numIndexTerms;i++) {
        upto += termsPointerDeltas[i];
        w.add(upto);
      }
      w.finish();

      packedOffsetsStart = out.getFilePointer();

      // write offsets into the byte[] terms
      w = PackedInts.getWriter(out, 1+numIndexTerms, PackedInts.bitsRequired(totTermLength));
      upto = 0;
      for(int i=0;i<numIndexTerms;i++) {
        w.add(upto);
        upto += termLengths[i];
      }
      w.add(upto);
      w.finish();

      // our referrer holds onto us, while other fields are
      // being written, so don't tie up this RAM:
      termLengths = null;
      termsPointerDeltas = null;
    }
  }

  @Override
  public void close() throws IOException {
    final long dirStart = out.getFilePointer();
    final int fieldCount = fields.size();

    out.writeInt(fieldCount);
    for(int i=0;i<fieldCount;i++) {
      SimpleFieldWriter field = fields.get(i);
      out.writeInt(field.fieldInfo.number);
      out.writeInt(field.numIndexTerms);
      out.writeLong(field.termsStart);
      out.writeLong(field.indexStart);
      out.writeLong(field.packedIndexStart);
      out.writeLong(field.packedOffsetsStart);
    }
    writeTrailer(dirStart);
    out.close();
  }

  protected void writeTrailer(long dirStart) throws IOException {
    out.seek(CodecUtil.headerLength(CODEC_NAME));
    out.writeLong(dirStart);
  }
}
