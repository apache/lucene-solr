package org.apache.lucene.codecs.memory;

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
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

// TODO: would be nice to somehow allow this to act like
// InstantiatedIndex, by never writing to disk; ie you write
// to this Codec in RAM only and then when you open a reader
// it pulls the FST directly from what you wrote w/o going
// to disk.

/** Stores terms & postings (docs, positions, payloads) in
 *  RAM, using an FST.
 *
 * <p>Note that this codec implements advance as a linear
 * scan!  This means if you store large fields in here,
 * queries that rely on advance will (AND BooleanQuery,
 * PhraseQuery) will be relatively slow!
 *
 * <p><b>NOTE</b>: this codec cannot address more than ~2.1 GB
 * of postings, because the underlying FST uses an int
 * to address the underlying byte[].
 *
 * @lucene.experimental */

// TODO: Maybe name this 'Cached' or something to reflect
// the reality that it is actually written to disk, but
// loads itself in ram?
public class MemoryPostingsFormat extends PostingsFormat {

  private final boolean doPackFST;

  public MemoryPostingsFormat() {
    this(false);
  }

  public MemoryPostingsFormat(boolean doPackFST) {
    super("Memory");
    this.doPackFST = doPackFST;
  }
  
  @Override
  public String toString() {
    return "PostingsFormat(name=" + getName() + " doPackFST= " + doPackFST + ")";
  }

  private final static class TermsWriter extends TermsConsumer {
    private final IndexOutput out;
    private final FieldInfo field;
    private final Builder<BytesRef> builder;
    private final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
    private final boolean doPackFST;
    private int termCount;

    public TermsWriter(IndexOutput out, FieldInfo field, boolean doPackFST) {
      this.out = out;
      this.field = field;
      this.doPackFST = doPackFST;
      builder = new Builder<BytesRef>(FST.INPUT_TYPE.BYTE1, 0, 0, true, true, Integer.MAX_VALUE, outputs, null, doPackFST);
    }

    private class PostingsWriter extends PostingsConsumer {
      private int lastDocID;
      private int lastPos;
      private int lastPayloadLen;

      // NOTE: not private so we don't pay access check at runtime:
      int docCount;
      RAMOutputStream buffer = new RAMOutputStream();
      
      int lastOffsetLength;
      int lastOffset;

      @Override
      public void startDoc(int docID, int termDocFreq) throws IOException {
        //System.out.println("    startDoc docID=" + docID + " freq=" + termDocFreq);
        final int delta = docID - lastDocID;
        assert docID == 0 || delta > 0;
        lastDocID = docID;
        docCount++;

        if (field.indexOptions == IndexOptions.DOCS_ONLY) {
          buffer.writeVInt(delta);
        } else if (termDocFreq == 1) {
          buffer.writeVInt((delta<<1) | 1);
        } else {
          buffer.writeVInt(delta<<1);
          assert termDocFreq > 0;
          buffer.writeVInt(termDocFreq);
        }

        lastPos = 0;
        lastOffset = 0;
      }

      @Override
      public void addPosition(int pos, BytesRef payload, int startOffset, int endOffset) throws IOException {
        assert payload == null || field.storePayloads;

        //System.out.println("      addPos pos=" + pos + " payload=" + payload);

        final int delta = pos - lastPos;
        assert delta >= 0;
        lastPos = pos;
        
        int payloadLen = 0;
        
        if (field.storePayloads) {
          payloadLen = payload == null ? 0 : payload.length;
          if (payloadLen != lastPayloadLen) {
            lastPayloadLen = payloadLen;
            buffer.writeVInt((delta<<1)|1);
            buffer.writeVInt(payloadLen);
          } else {
            buffer.writeVInt(delta<<1);
          }
        } else {
          buffer.writeVInt(delta);
        }
        
        if (field.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
          // don't use startOffset - lastEndOffset, because this creates lots of negative vints for synonyms,
          // and the numbers aren't that much smaller anyways.
          int offsetDelta = startOffset - lastOffset;
          int offsetLength = endOffset - startOffset;
          if (offsetLength != lastOffsetLength) {
            buffer.writeVInt(offsetDelta << 1 | 1);
            buffer.writeVInt(offsetLength);
          } else {
            buffer.writeVInt(offsetDelta << 1);
          }
          lastOffset = startOffset;
          lastOffsetLength = offsetLength;
        }
        
        if (payloadLen > 0) {
          buffer.writeBytes(payload.bytes, payload.offset, payloadLen);
        }
      }

      @Override
      public void finishDoc() {
      }

      public PostingsWriter reset() {
        assert buffer.getFilePointer() == 0;
        lastDocID = 0;
        docCount = 0;
        lastPayloadLen = 0;
        // force first offset to write its length
        lastOffsetLength = -1;
        return this;
      }
    }

    private final PostingsWriter postingsWriter = new PostingsWriter();

    @Override
    public PostingsConsumer startTerm(BytesRef text) {
      //System.out.println("  startTerm term=" + text.utf8ToString());
      return postingsWriter.reset();
    }

    private final RAMOutputStream buffer2 = new RAMOutputStream();
    private final BytesRef spare = new BytesRef();
    private byte[] finalBuffer = new byte[128];

    private final IntsRef scratchIntsRef = new IntsRef();

    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {

      assert postingsWriter.docCount == stats.docFreq;

      assert buffer2.getFilePointer() == 0;

      buffer2.writeVInt(stats.docFreq);
      if (field.indexOptions != IndexOptions.DOCS_ONLY) {
        buffer2.writeVLong(stats.totalTermFreq-stats.docFreq);
      }
      int pos = (int) buffer2.getFilePointer();
      buffer2.writeTo(finalBuffer, 0);
      buffer2.reset();

      final int totalBytes = pos + (int) postingsWriter.buffer.getFilePointer();
      if (totalBytes > finalBuffer.length) {
        finalBuffer = ArrayUtil.grow(finalBuffer, totalBytes);
      }
      postingsWriter.buffer.writeTo(finalBuffer, pos);
      postingsWriter.buffer.reset();

      spare.bytes = finalBuffer;
      spare.length = totalBytes;

      //System.out.println("    finishTerm term=" + text.utf8ToString() + " " + totalBytes + " bytes totalTF=" + stats.totalTermFreq);
      //for(int i=0;i<totalBytes;i++) {
      //  System.out.println("      " + Integer.toHexString(finalBuffer[i]&0xFF));
      //}

      builder.add(Util.toIntsRef(text, scratchIntsRef), BytesRef.deepCopyOf(spare));
      termCount++;
    }

    @Override
    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
      if (termCount > 0) {
        out.writeVInt(termCount);
        out.writeVInt(field.number);
        if (field.indexOptions != IndexOptions.DOCS_ONLY) {
          out.writeVLong(sumTotalTermFreq);
        }
        out.writeVLong(sumDocFreq);
        out.writeVInt(docCount);
        FST<BytesRef> fst = builder.finish();
        if (doPackFST) {
          fst = fst.pack(3, Math.max(10, fst.getNodeCount()/4));
        }
        fst.save(out);
        //System.out.println("finish field=" + field.name + " fp=" + out.getFilePointer());
      }
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }
  }

  private static String EXTENSION = "ram";

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {

    final String fileName = IndexFileNames.segmentFileName(state.segmentName, state.segmentSuffix, EXTENSION);
    final IndexOutput out = state.directory.createOutput(fileName, state.context);
    
    return new FieldsConsumer() {
      @Override
      public TermsConsumer addField(FieldInfo field) {
        //System.out.println("\naddField field=" + field.name);
        return new TermsWriter(out, field, doPackFST);
      }

      @Override
      public void close() throws IOException {
        // EOF marker:
        try {
          out.writeVInt(0);
        } finally {
          out.close();
        }
      }
    };
  }

  private final static class FSTDocsEnum extends DocsEnum {
    private final IndexOptions indexOptions;
    private final boolean storePayloads;
    private byte[] buffer = new byte[16];
    private final ByteArrayDataInput in = new ByteArrayDataInput(buffer);

    private Bits liveDocs;
    private int docUpto;
    private int docID = -1;
    private int accum;
    private int freq;
    private int payloadLen;
    private int numDocs;

    public FSTDocsEnum(IndexOptions indexOptions, boolean storePayloads) {
      this.indexOptions = indexOptions;
      this.storePayloads = storePayloads;
    }

    public boolean canReuse(IndexOptions indexOptions, boolean storePayloads) {
      return indexOptions == this.indexOptions && storePayloads == this.storePayloads;
    }
    
    public FSTDocsEnum reset(BytesRef bufferIn, Bits liveDocs, int numDocs) {
      assert numDocs > 0;
      if (buffer.length < bufferIn.length - bufferIn.offset) {
        buffer = ArrayUtil.grow(buffer, bufferIn.length - bufferIn.offset);
      }
      in.reset(buffer, 0, bufferIn.length - bufferIn.offset);
      System.arraycopy(bufferIn.bytes, bufferIn.offset, buffer, 0, bufferIn.length - bufferIn.offset);
      this.liveDocs = liveDocs;
      docID = -1;
      accum = 0;
      docUpto = 0;
      payloadLen = 0;
      this.numDocs = numDocs;
      return this;
    }

    @Override
    public int nextDoc() {
      while(true) {
        //System.out.println("  nextDoc cycle docUpto=" + docUpto + " numDocs=" + numDocs + " fp=" + in.getPosition() + " this=" + this);
        if (docUpto == numDocs) {
          // System.out.println("    END");
          return docID = NO_MORE_DOCS;
        }
        docUpto++;
        if (indexOptions == IndexOptions.DOCS_ONLY) {
          accum += in.readVInt();
        } else {
          final int code = in.readVInt();
          accum += code >>> 1;
          //System.out.println("  docID=" + accum + " code=" + code);
          if ((code & 1) != 0) {
            freq = 1;
          } else {
            freq = in.readVInt();
            assert freq > 0;
          }

          if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
            // Skip positions/payloads
            for(int posUpto=0;posUpto<freq;posUpto++) {
              if (!storePayloads) {
                in.readVInt();
              } else {
                final int posCode = in.readVInt();
                if ((posCode & 1) != 0) {
                  payloadLen = in.readVInt();
                }
                in.skipBytes(payloadLen);
              }
            }
          } else if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
            // Skip positions/offsets/payloads
            for(int posUpto=0;posUpto<freq;posUpto++) {
              int posCode = in.readVInt();
              if (storePayloads && ((posCode & 1) != 0)) {
                payloadLen = in.readVInt();
              }
              if ((in.readVInt() & 1) != 0) {
                // new offset length
                in.readVInt();
              }
              if (storePayloads) {
                in.skipBytes(payloadLen);
              }
            }
          }
        }

        if (liveDocs == null || liveDocs.get(accum)) {
          //System.out.println("    return docID=" + accum + " freq=" + freq);
          return (docID = accum);
        }
      }
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int advance(int target) {
      // TODO: we could make more efficient version, but, it
      // should be rare that this will matter in practice
      // since usually apps will not store "big" fields in
      // this codec!
      //System.out.println("advance start docID=" + docID + " target=" + target);
      while(nextDoc() < target) {
      }
      return docID;
    }

    @Override
    public int freq() {
      assert indexOptions != IndexOptions.DOCS_ONLY;
      return freq;
    }
  }

  private final static class FSTDocsAndPositionsEnum extends DocsAndPositionsEnum {
    private final boolean storePayloads;
    private byte[] buffer = new byte[16];
    private final ByteArrayDataInput in = new ByteArrayDataInput(buffer);

    private Bits liveDocs;
    private int docUpto;
    private int docID = -1;
    private int accum;
    private int freq;
    private int numDocs;
    private int posPending;
    private int payloadLength;
    private boolean payloadRetrieved;
    final boolean storeOffsets;
    int offsetLength;
    int startOffset;

    private int pos;
    private final BytesRef payload = new BytesRef();

    public FSTDocsAndPositionsEnum(boolean storePayloads, boolean storeOffsets) {
      this.storePayloads = storePayloads;
      this.storeOffsets = storeOffsets;
    }

    public boolean canReuse(boolean storePayloads, boolean storeOffsets) {
      return storePayloads == this.storePayloads && storeOffsets == this.storeOffsets;
    }
    
    public FSTDocsAndPositionsEnum reset(BytesRef bufferIn, Bits liveDocs, int numDocs) {
      assert numDocs > 0;

      // System.out.println("D&P reset bytes this=" + this);
      // for(int i=bufferIn.offset;i<bufferIn.length;i++) {
      //   System.out.println("  " + Integer.toHexString(bufferIn.bytes[i]&0xFF));
      // }

      if (buffer.length < bufferIn.length - bufferIn.offset) {
        buffer = ArrayUtil.grow(buffer, bufferIn.length - bufferIn.offset);
      }
      in.reset(buffer, 0, bufferIn.length - bufferIn.offset);
      System.arraycopy(bufferIn.bytes, bufferIn.offset, buffer, 0, bufferIn.length - bufferIn.offset);
      this.liveDocs = liveDocs;
      docID = -1;
      accum = 0;
      docUpto = 0;
      payload.bytes = buffer;
      payloadLength = 0;
      this.numDocs = numDocs;
      posPending = 0;
      payloadRetrieved = false;
      startOffset = storeOffsets ? 0 : -1; // always return -1 if no offsets are stored
      offsetLength = 0;
      return this;
    }

    @Override
    public int nextDoc() {
      while (posPending > 0) {
        nextPosition();
      }
      while(true) {
        //System.out.println("  nextDoc cycle docUpto=" + docUpto + " numDocs=" + numDocs + " fp=" + in.getPosition() + " this=" + this);
        if (docUpto == numDocs) {
          //System.out.println("    END");
          return docID = NO_MORE_DOCS;
        }
        docUpto++;
        
        final int code = in.readVInt();
        accum += code >>> 1;
        if ((code & 1) != 0) {
          freq = 1;
        } else {
          freq = in.readVInt();
          assert freq > 0;
        }

        if (liveDocs == null || liveDocs.get(accum)) {
          pos = 0;
          startOffset = storeOffsets ? 0 : -1;
          posPending = freq;
          //System.out.println("    return docID=" + accum + " freq=" + freq);
          return (docID = accum);
        }

        // Skip positions
        for(int posUpto=0;posUpto<freq;posUpto++) {
          if (!storePayloads) {
            in.readVInt();
          } else {
            final int skipCode = in.readVInt();
            if ((skipCode & 1) != 0) {
              payloadLength = in.readVInt();
              //System.out.println("    new payloadLen=" + payloadLength);
            }
          }
          
          if (storeOffsets) {
            if ((in.readVInt() & 1) != 0) {
              // new offset length
              offsetLength = in.readVInt();
            }
          }
          
          if (storePayloads) {
            in.skipBytes(payloadLength);
          }
        }
      }
    }

    @Override
    public int nextPosition() {
      //System.out.println("    nextPos storePayloads=" + storePayloads + " this=" + this);
      assert posPending > 0;
      posPending--;
      if (!storePayloads) {
        pos += in.readVInt();
      } else {
        final int code = in.readVInt();
        pos += code >>> 1;
        if ((code & 1) != 0) {
          payloadLength = in.readVInt();
          //System.out.println("      new payloadLen=" + payloadLength);
          //} else {
          //System.out.println("      same payloadLen=" + payloadLength);
        }
      }
      
      if (storeOffsets) {
        int offsetCode = in.readVInt();
        if ((offsetCode & 1) != 0) {
          // new offset length
          offsetLength = in.readVInt();
        }
        startOffset += offsetCode >>> 1;
      }
      
      if (storePayloads) {
        payload.offset = in.getPosition();
        in.skipBytes(payloadLength);
        payload.length = payloadLength;
        // Necessary, in case caller changed the
        // payload.bytes from prior call:
        payload.bytes = buffer;
        payloadRetrieved = false;
      }

      //System.out.println("      pos=" + pos + " payload=" + payload + " fp=" + in.getPosition());
      return pos;
    }

    @Override
    public int startOffset() {
      return startOffset;
    }

    @Override
    public int endOffset() {
      return startOffset + offsetLength;
    }

    @Override
    public BytesRef getPayload() {
      payloadRetrieved = true;
      return payload;
    }

    @Override
    public boolean hasPayload() {
      return !payloadRetrieved && payload.length > 0;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int advance(int target) {
      // TODO: we could make more efficient version, but, it
      // should be rare that this will matter in practice
      // since usually apps will not store "big" fields in
      // this codec!
      //System.out.println("advance target=" + target);
      while(nextDoc() < target) {
      }
      //System.out.println("  return " + docID);
      return docID;
    }

    @Override
    public int freq() {
      return freq;
    }
  }

  private final static class FSTTermsEnum extends TermsEnum {
    private final FieldInfo field;
    private final BytesRefFSTEnum<BytesRef> fstEnum;
    private final ByteArrayDataInput buffer = new ByteArrayDataInput();
    private boolean didDecode;

    private int docFreq;
    private long totalTermFreq;
    private BytesRefFSTEnum.InputOutput<BytesRef> current;

    public FSTTermsEnum(FieldInfo field, FST<BytesRef> fst) {
      this.field = field;
      fstEnum = new BytesRefFSTEnum<BytesRef>(fst);
    }

    private void decodeMetaData() throws IOException {
      if (!didDecode) {
        buffer.reset(current.output.bytes, 0, current.output.length);
        docFreq = buffer.readVInt();
        if (field.indexOptions != IndexOptions.DOCS_ONLY) {
          totalTermFreq = docFreq + buffer.readVLong();
        } else {
          totalTermFreq = -1;
        }
        current.output.offset = buffer.getPosition();
        //System.out.println("  df=" + docFreq + " totTF=" + totalTermFreq + " offset=" + buffer.getPosition() + " len=" + current.output.length);
        didDecode = true;
      }
    }

    @Override
    public boolean seekExact(BytesRef text, boolean useCache /* ignored */) throws IOException {
      //System.out.println("te.seekExact text=" + field.name + ":" + text.utf8ToString() + " this=" + this);
      current = fstEnum.seekExact(text);
      didDecode = false;
      return current != null;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text, boolean useCache /* ignored */) throws IOException {
      //System.out.println("te.seek text=" + field.name + ":" + text.utf8ToString() + " this=" + this);
      current = fstEnum.seekCeil(text);
      if (current == null) {
        return SeekStatus.END;
      } else {

        // System.out.println("  got term=" + current.input.utf8ToString());
        // for(int i=0;i<current.output.length;i++) {
        //   System.out.println("    " + Integer.toHexString(current.output.bytes[i]&0xFF));
        // }

        didDecode = false;

        if (text.equals(current.input)) {
          //System.out.println("  found!");
          return SeekStatus.FOUND;
        } else {
          //System.out.println("  not found: " + current.input.utf8ToString());
          return SeekStatus.NOT_FOUND;
        }
      }
    }
    
    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, boolean needsFreqs) throws IOException {
      decodeMetaData();
      FSTDocsEnum docsEnum;

      if (needsFreqs && field.indexOptions == IndexOptions.DOCS_ONLY) {
        return null;
      } else if (reuse == null || !(reuse instanceof FSTDocsEnum)) {
        docsEnum = new FSTDocsEnum(field.indexOptions, field.storePayloads);
      } else {
        docsEnum = (FSTDocsEnum) reuse;        
        if (!docsEnum.canReuse(field.indexOptions, field.storePayloads)) {
          docsEnum = new FSTDocsEnum(field.indexOptions, field.storePayloads);
        }
      }
      return docsEnum.reset(current.output, liveDocs, docFreq);
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, boolean needsOffsets) throws IOException {

      boolean hasOffsets = field.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      if (needsOffsets && !hasOffsets) {
        return null; // not available
      }
      
      if (field.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
        return null;
      }
      decodeMetaData();
      FSTDocsAndPositionsEnum docsAndPositionsEnum;
      if (reuse == null || !(reuse instanceof FSTDocsAndPositionsEnum)) {
        docsAndPositionsEnum = new FSTDocsAndPositionsEnum(field.storePayloads, hasOffsets);
      } else {
        docsAndPositionsEnum = (FSTDocsAndPositionsEnum) reuse;        
        if (!docsAndPositionsEnum.canReuse(field.storePayloads, hasOffsets)) {
          docsAndPositionsEnum = new FSTDocsAndPositionsEnum(field.storePayloads, hasOffsets);
        }
      }
      //System.out.println("D&P reset this=" + this);
      return docsAndPositionsEnum.reset(current.output, liveDocs, docFreq);
    }

    @Override
    public BytesRef term() {
      return current.input;
    }

    @Override
    public BytesRef next() throws IOException {
      //System.out.println("te.next");
      current = fstEnum.next();
      if (current == null) {
        //System.out.println("  END");
        return null;
      }
      didDecode = false;
      //System.out.println("  term=" + field.name + ":" + current.input.utf8ToString());
      return current.input;
    }

    @Override
    public int docFreq() throws IOException {
      decodeMetaData();
      return docFreq;
    }

    @Override
    public long totalTermFreq() throws IOException {
      decodeMetaData();
      return totalTermFreq;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    @Override
    public void seekExact(long ord) {
      // NOTE: we could add this...
      throw new UnsupportedOperationException();
    }

    @Override
    public long ord() {
      // NOTE: we could add this...
      throw new UnsupportedOperationException();
    }
  }

  private final static class TermsReader extends Terms {

    private final long sumTotalTermFreq;
    private final long sumDocFreq;
    private final int docCount;
    private final int termCount;
    private FST<BytesRef> fst;
    private final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
    private final FieldInfo field;

    public TermsReader(FieldInfos fieldInfos, IndexInput in, int termCount) throws IOException {
      this.termCount = termCount;
      final int fieldNumber = in.readVInt();
      field = fieldInfos.fieldInfo(fieldNumber);
      if (field.indexOptions != IndexOptions.DOCS_ONLY) {
        sumTotalTermFreq = in.readVLong();
      } else {
        sumTotalTermFreq = -1;
      }
      sumDocFreq = in.readVLong();
      docCount = in.readVInt();
      
      fst = new FST<BytesRef>(in, outputs);
    }

    @Override
    public long getSumTotalTermFreq() {
      return sumTotalTermFreq;
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return sumDocFreq;
    }

    @Override
    public int getDocCount() throws IOException {
      return docCount;
    }

    @Override
    public long size() throws IOException {
      return termCount;
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) {
      return new FSTTermsEnum(field, fst);
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
    final IndexInput in = state.dir.openInput(fileName, IOContext.READONCE);

    final SortedMap<String,TermsReader> fields = new TreeMap<String,TermsReader>();

    try {
      while(true) {
        final int termCount = in.readVInt();
        if (termCount == 0) {
          break;
        }
        final TermsReader termsReader = new TermsReader(state.fieldInfos, in, termCount);
        // System.out.println("load field=" + termsReader.field.name);
        fields.put(termsReader.field.name, termsReader);
      }
    } finally {
      in.close();
    }

    return new FieldsProducer() {
      @Override
      public FieldsEnum iterator() {
        final Iterator<TermsReader> iter = fields.values().iterator();

        return new FieldsEnum() {

          private TermsReader current;

          @Override
          public String next() {
            current = iter.next();
            return current.field.name;
          }

          @Override
          public Terms terms() {
            return current;
          }
        };
      }

      @Override
      public Terms terms(String field) {
        return fields.get(field);
      }
      
      @Override
      public int size() {
        return fields.size();
      }

      @Override
      public void close() {
        // Drop ref to FST:
        for(TermsReader termsReader : fields.values()) {
          termsReader.fst = null;
        }
      }
    };
  }

  @Override
  public void files(SegmentInfo segmentInfo, String segmentSuffix, Set<String> files) throws IOException {
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, EXTENSION));
  }
}
