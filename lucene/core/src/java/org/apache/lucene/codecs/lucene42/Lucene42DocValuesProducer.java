package org.apache.lucene.codecs.lucene42;

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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.BytesRefFSTEnum.InputOutput;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.packed.BlockPackedReader;
import org.apache.lucene.util.packed.MonotonicBlockPackedReader;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Reader for {@link Lucene42DocValuesFormat}
 */
class Lucene42DocValuesProducer extends DocValuesProducer {
  // metadata maps (just file pointers and minimal stuff)
  private final Map<Integer,NumericEntry> numerics;
  private final Map<Integer,BinaryEntry> binaries;
  private final Map<Integer,FSTEntry> fsts;
  private final IndexInput data;
  
  // ram instances we have already loaded
  private final Map<Integer,NumericDocValues> numericInstances = 
      new HashMap<Integer,NumericDocValues>();
  private final Map<Integer,BinaryDocValues> binaryInstances =
      new HashMap<Integer,BinaryDocValues>();
  private final Map<Integer,FST<Long>> fstInstances =
      new HashMap<Integer,FST<Long>>();
  
  private final int maxDoc;
    
  Lucene42DocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    maxDoc = state.segmentInfo.getDocCount();
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    // read in the entries from the metadata file.
    IndexInput in = state.directory.openInput(metaName, state.context);
    boolean success = false;
    try {
      CodecUtil.checkHeader(in, metaCodec, 
                                Lucene42DocValuesConsumer.VERSION_START,
                                Lucene42DocValuesConsumer.VERSION_START);
      numerics = new HashMap<Integer,NumericEntry>();
      binaries = new HashMap<Integer,BinaryEntry>();
      fsts = new HashMap<Integer,FSTEntry>();
      readFields(in, state.fieldInfos);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(in);
      } else {
        IOUtils.closeWhileHandlingException(in);
      }
    }
    
    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    data = state.directory.openInput(dataName, state.context);
    CodecUtil.checkHeader(data, dataCodec, 
                                Lucene42DocValuesConsumer.VERSION_START,
                                Lucene42DocValuesConsumer.VERSION_START);
  }
  
  private void readFields(IndexInput meta, FieldInfos infos) throws IOException {
    int fieldNumber = meta.readVInt();
    while (fieldNumber != -1) {
      int fieldType = meta.readByte();
      if (fieldType == Lucene42DocValuesConsumer.NUMBER) {
        NumericEntry entry = new NumericEntry();
        entry.offset = meta.readLong();
        entry.format = meta.readByte();
        if (entry.format != Lucene42DocValuesConsumer.UNCOMPRESSED) {
          entry.packedIntsVersion = meta.readVInt();
        }
        numerics.put(fieldNumber, entry);
      } else if (fieldType == Lucene42DocValuesConsumer.BYTES) {
        BinaryEntry entry = new BinaryEntry();
        entry.offset = meta.readLong();
        entry.numBytes = meta.readLong();
        entry.minLength = meta.readVInt();
        entry.maxLength = meta.readVInt();
        if (entry.minLength != entry.maxLength) {
          entry.packedIntsVersion = meta.readVInt();
          entry.blockSize = meta.readVInt();
        }
        binaries.put(fieldNumber, entry);
      } else if (fieldType == Lucene42DocValuesConsumer.FST) {
        FSTEntry entry = new FSTEntry();
        entry.offset = meta.readLong();
        entry.numOrds = meta.readVLong();
        fsts.put(fieldNumber, entry);
      } else {
        throw new CorruptIndexException("invalid entry type: " + fieldType + ", input=" + meta);
      }
      fieldNumber = meta.readVInt();
    }
  }

  @Override
  public synchronized NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericDocValues instance = numericInstances.get(field.number);
    if (instance == null) {
      instance = loadNumeric(field);
      numericInstances.put(field.number, instance);
    }
    return instance;
  }
  
  private NumericDocValues loadNumeric(FieldInfo field) throws IOException {
    NumericEntry entry = numerics.get(field.number);
    data.seek(entry.offset);
    if (entry.format == Lucene42DocValuesConsumer.TABLE_COMPRESSED) {
      int size = data.readVInt();
      final long decode[] = new long[size];
      for (int i = 0; i < decode.length; i++) {
        decode[i] = data.readLong();
      }
      final int formatID = data.readVInt();
      final int bitsPerValue = data.readVInt();
      final PackedInts.Reader reader = PackedInts.getReaderNoHeader(data, PackedInts.Format.byId(formatID), entry.packedIntsVersion, maxDoc, bitsPerValue);
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return decode[(int)reader.get(docID)];
        }
      };
    } else if (entry.format == Lucene42DocValuesConsumer.DELTA_COMPRESSED) {
      final int blockSize = data.readVInt();
      final BlockPackedReader reader = new BlockPackedReader(data, entry.packedIntsVersion, blockSize, maxDoc, false);
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return reader.get(docID);
        }
      };
    } else if (entry.format == Lucene42DocValuesConsumer.UNCOMPRESSED) {
      final byte bytes[] = new byte[maxDoc];
      data.readBytes(bytes, 0, bytes.length);
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return bytes[docID];
        }
      };
    } else {
      throw new IllegalStateException();
    }
  }

  @Override
  public synchronized BinaryDocValues getBinary(FieldInfo field) throws IOException {
    BinaryDocValues instance = binaryInstances.get(field.number);
    if (instance == null) {
      instance = loadBinary(field);
      binaryInstances.put(field.number, instance);
    }
    return instance;
  }
  
  private BinaryDocValues loadBinary(FieldInfo field) throws IOException {
    BinaryEntry entry = binaries.get(field.number);
    data.seek(entry.offset);
    PagedBytes bytes = new PagedBytes(16);
    bytes.copy(data, entry.numBytes);
    final PagedBytes.Reader bytesReader = bytes.freeze(true);
    if (entry.minLength == entry.maxLength) {
      final int fixedLength = entry.minLength;
      return new BinaryDocValues() {
        @Override
        public void get(int docID, BytesRef result) {
          bytesReader.fillSlice(result, fixedLength * (long)docID, fixedLength);
        }
      };
    } else {
      final MonotonicBlockPackedReader addresses = new MonotonicBlockPackedReader(data, entry.packedIntsVersion, entry.blockSize, maxDoc, false);
      return new BinaryDocValues() {
        @Override
        public void get(int docID, BytesRef result) {
          long startAddress = docID == 0 ? 0 : addresses.get(docID-1);
          long endAddress = addresses.get(docID); 
          bytesReader.fillSlice(result, startAddress, (int) (endAddress - startAddress));
        }
      };
    }
  }
  
  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    final FSTEntry entry = fsts.get(field.number);
    FST<Long> instance;
    synchronized(this) {
      instance = fstInstances.get(field.number);
      if (instance == null) {
        data.seek(entry.offset);
        instance = new FST<Long>(data, PositiveIntOutputs.getSingleton(true));
        fstInstances.put(field.number, instance);
      }
    }
    final NumericDocValues docToOrd = getNumeric(field);
    final FST<Long> fst = instance;
    
    // per-thread resources
    final BytesReader in = fst.getBytesReader();
    final Arc<Long> firstArc = new Arc<Long>();
    final Arc<Long> scratchArc = new Arc<Long>();
    final IntsRef scratchInts = new IntsRef();
    final BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<Long>(fst); 
    
    return new SortedDocValues() {
      @Override
      public int getOrd(int docID) {
        return (int) docToOrd.get(docID);
      }

      @Override
      public void lookupOrd(int ord, BytesRef result) {
        try {
          in.setPosition(0);
          fst.getFirstArc(firstArc);
          IntsRef output = Util.getByOutput(fst, ord, in, firstArc, scratchArc, scratchInts);
          result.bytes = new byte[output.length];
          result.offset = 0;
          result.length = 0;
          Util.toBytesRef(output, result);
        } catch (IOException bogus) {
          throw new RuntimeException(bogus);
        }
      }

      @Override
      public int lookupTerm(BytesRef key) {
        try {
          InputOutput<Long> o = fstEnum.seekCeil(key);
          if (o == null) {
            return -getValueCount()-1;
          } else if (o.input.equals(key)) {
            return o.output.intValue();
          } else {
            return (int) -o.output-1;
          }
        } catch (IOException bogus) {
          throw new RuntimeException(bogus);
        }
      }

      @Override
      public int getValueCount() {
        return (int)entry.numOrds;
      }

      @Override
      public TermsEnum termsEnum() {
        return new FSTTermsEnum(fst);
      }
    };
  }
  
  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    final FSTEntry entry = fsts.get(field.number);
    if (entry.numOrds == 0) {
      return SortedSetDocValues.EMPTY; // empty FST!
    }
    FST<Long> instance;
    synchronized(this) {
      instance = fstInstances.get(field.number);
      if (instance == null) {
        data.seek(entry.offset);
        instance = new FST<Long>(data, PositiveIntOutputs.getSingleton(true));
        fstInstances.put(field.number, instance);
      }
    }
    final BinaryDocValues docToOrds = getBinary(field);
    final FST<Long> fst = instance;
    
    // per-thread resources
    final BytesReader in = fst.getBytesReader();
    final Arc<Long> firstArc = new Arc<Long>();
    final Arc<Long> scratchArc = new Arc<Long>();
    final IntsRef scratchInts = new IntsRef();
    final BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<Long>(fst); 
    final BytesRef ref = new BytesRef();
    final ByteArrayDataInput input = new ByteArrayDataInput();
    return new SortedSetDocValues() {
      long currentOrd;

      @Override
      public long nextOrd() {
        if (input.eof()) {
          return NO_MORE_ORDS;
        } else {
          currentOrd += input.readVLong();
          return currentOrd;
        }
      }
      
      @Override
      public void setDocument(int docID) {
        docToOrds.get(docID, ref);
        input.reset(ref.bytes, ref.offset, ref.length);
        currentOrd = 0;
      }

      @Override
      public void lookupOrd(long ord, BytesRef result) {
        try {
          in.setPosition(0);
          fst.getFirstArc(firstArc);
          IntsRef output = Util.getByOutput(fst, ord, in, firstArc, scratchArc, scratchInts);
          result.bytes = new byte[output.length];
          result.offset = 0;
          result.length = 0;
          Util.toBytesRef(output, result);
        } catch (IOException bogus) {
          throw new RuntimeException(bogus);
        }
      }

      @Override
      public long lookupTerm(BytesRef key) {
        try {
          InputOutput<Long> o = fstEnum.seekCeil(key);
          if (o == null) {
            return -getValueCount()-1;
          } else if (o.input.equals(key)) {
            return o.output.intValue();
          } else {
            return -o.output-1;
          }
        } catch (IOException bogus) {
          throw new RuntimeException(bogus);
        }
      }

      @Override
      public long getValueCount() {
        return entry.numOrds;
      }

      @Override
      public TermsEnum termsEnum() {
        return new FSTTermsEnum(fst);
      }
    };
  }

  @Override
  public void close() throws IOException {
    data.close();
  }
  
  static class NumericEntry {
    long offset;
    byte format;
    int packedIntsVersion;
  }
  
  static class BinaryEntry {
    long offset;
    long numBytes;
    int minLength;
    int maxLength;
    int packedIntsVersion;
    int blockSize;
  }
  
  static class FSTEntry {
    long offset;
    long numOrds;
  }
  
  // exposes FSTEnum directly as a TermsEnum: avoids binary-search next()
  static class FSTTermsEnum extends TermsEnum {
    final BytesRefFSTEnum<Long> in;
    
    // this is all for the complicated seek(ord)...
    // maybe we should add a FSTEnum that supports this operation?
    final FST<Long> fst;
    final FST.BytesReader bytesReader;
    final Arc<Long> firstArc = new Arc<Long>();
    final Arc<Long> scratchArc = new Arc<Long>();
    final IntsRef scratchInts = new IntsRef();
    final BytesRef scratchBytes = new BytesRef();
    
    FSTTermsEnum(FST<Long> fst) {
      this.fst = fst;
      in = new BytesRefFSTEnum<Long>(fst);
      bytesReader = fst.getBytesReader();
    }

    @Override
    public BytesRef next() throws IOException {
      InputOutput<Long> io = in.next();
      if (io == null) {
        return null;
      } else {
        return io.input;
      }
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    @Override
    public SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException {
      if (in.seekCeil(text) == null) {
        return SeekStatus.END;
      } else if (term().equals(text)) {
        // TODO: add SeekStatus to FSTEnum like in https://issues.apache.org/jira/browse/LUCENE-3729
        // to remove this comparision?
        return SeekStatus.FOUND;
      } else {
        return SeekStatus.NOT_FOUND;
      }
    }

    @Override
    public boolean seekExact(BytesRef text, boolean useCache) throws IOException {
      if (in.seekExact(text) == null) {
        return false;
      } else {
        return true;
      }
    }

    @Override
    public void seekExact(long ord) throws IOException {
      // TODO: would be better to make this simpler and faster.
      // but we dont want to introduce a bug that corrupts our enum state!
      bytesReader.setPosition(0);
      fst.getFirstArc(firstArc);
      IntsRef output = Util.getByOutput(fst, ord, bytesReader, firstArc, scratchArc, scratchInts);
      scratchBytes.bytes = new byte[output.length];
      scratchBytes.offset = 0;
      scratchBytes.length = 0;
      Util.toBytesRef(output, scratchBytes);
      // TODO: we could do this lazily, better to try to push into FSTEnum though?
      in.seekExact(scratchBytes);
    }

    @Override
    public BytesRef term() throws IOException {
      return in.current().input;
    }

    @Override
    public long ord() throws IOException {
      return in.current().output;
    }

    @Override
    public int docFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long totalTermFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
