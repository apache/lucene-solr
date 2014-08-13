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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.RamUsageEstimator;
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
  private final int version;
  
  // ram instances we have already loaded
  private final Map<Integer,NumericDocValues> numericInstances = 
      new HashMap<>();
  private final Map<Integer,BinaryDocValues> binaryInstances =
      new HashMap<>();
  private final Map<Integer,FST<Long>> fstInstances =
      new HashMap<>();
  
  private final int maxDoc;
  private final AtomicLong ramBytesUsed;
  
  static final byte NUMBER = 0;
  static final byte BYTES = 1;
  static final byte FST = 2;

  static final int BLOCK_SIZE = 4096;
  
  static final byte DELTA_COMPRESSED = 0;
  static final byte TABLE_COMPRESSED = 1;
  static final byte UNCOMPRESSED = 2;
  static final byte GCD_COMPRESSED = 3;
  
  static final int VERSION_START = 0;
  static final int VERSION_GCD_COMPRESSION = 1;
  static final int VERSION_CHECKSUM = 2;
  static final int VERSION_CURRENT = VERSION_CHECKSUM;
    
  Lucene42DocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    maxDoc = state.segmentInfo.getDocCount();
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    // read in the entries from the metadata file.
    ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context);
    boolean success = false;
    ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOfInstance(getClass()));
    try {
      version = CodecUtil.checkHeader(in, metaCodec, 
                                      VERSION_START,
                                      VERSION_CURRENT);
      numerics = new HashMap<>();
      binaries = new HashMap<>();
      fsts = new HashMap<>();
      readFields(in, state.fieldInfos);

      if (version >= VERSION_CHECKSUM) {
        CodecUtil.checkFooter(in);
      } else {
        CodecUtil.checkEOF(in);
      }
      
      success = true;
    } finally {
      if (success) {
        IOUtils.close(in);
      } else {
        IOUtils.closeWhileHandlingException(in);
      }
    }

    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    this.data = state.directory.openInput(dataName, state.context);
    success = false;
    try {
      final int version2 = CodecUtil.checkHeader(data, dataCodec, 
                                                 VERSION_START,
                                                 VERSION_CURRENT);
      if (version != version2) {
        throw new CorruptIndexException("Format versions mismatch");
      }
      
      if (version >= VERSION_CHECKSUM) {
        // NOTE: data file is too costly to verify checksum against all the bytes on open,
        // but for now we at least verify proper structure of the checksum footer: which looks
        // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
        // such as file truncation.
        CodecUtil.retrieveChecksum(data);
      }

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this.data);
      }
    }
  }
  
  private void readFields(IndexInput meta, FieldInfos infos) throws IOException {
    int fieldNumber = meta.readVInt();
    while (fieldNumber != -1) {
      if (infos.fieldInfo(fieldNumber) == null) {
        // trickier to validate more: because we re-use for norms, because we use multiple entries
        // for "composite" types like sortedset, etc.
        throw new CorruptIndexException("Invalid field number: " + fieldNumber + " (resource=" + meta + ")");
      }
      int fieldType = meta.readByte();
      if (fieldType == NUMBER) {
        NumericEntry entry = new NumericEntry();
        entry.offset = meta.readLong();
        entry.format = meta.readByte();
        switch(entry.format) {
          case DELTA_COMPRESSED:
          case TABLE_COMPRESSED:
          case GCD_COMPRESSED:
          case UNCOMPRESSED:
               break;
          default:
               throw new CorruptIndexException("Unknown format: " + entry.format + ", input=" + meta);
        }
        if (entry.format != UNCOMPRESSED) {
          entry.packedIntsVersion = meta.readVInt();
        }
        numerics.put(fieldNumber, entry);
      } else if (fieldType == BYTES) {
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
      } else if (fieldType == FST) {
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
  
  @Override
  public long ramBytesUsed() {
    return ramBytesUsed.get();
  }
  
  @Override
  public void checkIntegrity() throws IOException {
    if (version >= VERSION_CHECKSUM) {
      CodecUtil.checksumEntireFile(data);
    }
  }

  private NumericDocValues loadNumeric(FieldInfo field) throws IOException {
    NumericEntry entry = numerics.get(field.number);
    data.seek(entry.offset);
    switch (entry.format) {
      case TABLE_COMPRESSED:
        int size = data.readVInt();
        if (size > 256) {
          throw new CorruptIndexException("TABLE_COMPRESSED cannot have more than 256 distinct values, input=" + data);
        }
        final long decode[] = new long[size];
        for (int i = 0; i < decode.length; i++) {
          decode[i] = data.readLong();
        }
        final int formatID = data.readVInt();
        final int bitsPerValue = data.readVInt();
        final PackedInts.Reader ordsReader = PackedInts.getReaderNoHeader(data, PackedInts.Format.byId(formatID), entry.packedIntsVersion, maxDoc, bitsPerValue);
        ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(decode) + ordsReader.ramBytesUsed());
        return new NumericDocValues() {
          @Override
          public long get(int docID) {
            return decode[(int)ordsReader.get(docID)];
          }
        };
      case DELTA_COMPRESSED:
        final int blockSize = data.readVInt();
        final BlockPackedReader reader = new BlockPackedReader(data, entry.packedIntsVersion, blockSize, maxDoc, false);
        ramBytesUsed.addAndGet(reader.ramBytesUsed());
        return reader;
      case UNCOMPRESSED:
        final byte bytes[] = new byte[maxDoc];
        data.readBytes(bytes, 0, bytes.length);
        ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(bytes));
        return new NumericDocValues() {
          @Override
          public long get(int docID) {
            return bytes[docID];
          }
        };
      case GCD_COMPRESSED:
        final long min = data.readLong();
        final long mult = data.readLong();
        final int quotientBlockSize = data.readVInt();
        final BlockPackedReader quotientReader = new BlockPackedReader(data, entry.packedIntsVersion, quotientBlockSize, maxDoc, false);
        ramBytesUsed.addAndGet(quotientReader.ramBytesUsed());
        return new NumericDocValues() {
          @Override
          public long get(int docID) {
            return min + mult * quotientReader.get(docID);
          }
        };
      default:
        throw new AssertionError();
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
      ramBytesUsed.addAndGet(bytesReader.ramBytesUsed());
      return new BinaryDocValues() {
        @Override
        public BytesRef get(int docID) {
          final BytesRef term = new BytesRef();
          bytesReader.fillSlice(term, fixedLength * (long)docID, fixedLength);
          return term;
        }
      };
    } else {
      final MonotonicBlockPackedReader addresses = MonotonicBlockPackedReader.of(data, entry.packedIntsVersion, entry.blockSize, maxDoc, false);
      ramBytesUsed.addAndGet(bytesReader.ramBytesUsed() + addresses.ramBytesUsed());
      return new BinaryDocValues() {

        @Override
        public BytesRef get(int docID) {
          long startAddress = docID == 0 ? 0 : addresses.get(docID-1);
          long endAddress = addresses.get(docID); 
          final BytesRef term = new BytesRef();
          bytesReader.fillSlice(term, startAddress, (int) (endAddress - startAddress));
          return term;
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
        instance = new FST<>(data, PositiveIntOutputs.getSingleton());
        ramBytesUsed.addAndGet(instance.ramBytesUsed());
        fstInstances.put(field.number, instance);
      }
    }
    final NumericDocValues docToOrd = getNumeric(field);
    final FST<Long> fst = instance;
    
    // per-thread resources
    final BytesReader in = fst.getBytesReader();
    final Arc<Long> firstArc = new Arc<>();
    final Arc<Long> scratchArc = new Arc<>();
    final IntsRefBuilder scratchInts = new IntsRefBuilder();
    final BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<>(fst);
    
    return new SortedDocValues() {

      final BytesRefBuilder term = new BytesRefBuilder();

      @Override
      public int getOrd(int docID) {
        return (int) docToOrd.get(docID);
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        try {
          in.setPosition(0);
          fst.getFirstArc(firstArc);
          IntsRef output = Util.getByOutput(fst, ord, in, firstArc, scratchArc, scratchInts);
          term.grow(output.length);
          term.clear();
          return Util.toBytesRef(output, term);
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
      return DocValues.emptySortedSet(); // empty FST!
    }
    FST<Long> instance;
    synchronized(this) {
      instance = fstInstances.get(field.number);
      if (instance == null) {
        data.seek(entry.offset);
        instance = new FST<>(data, PositiveIntOutputs.getSingleton());
        ramBytesUsed.addAndGet(instance.ramBytesUsed());
        fstInstances.put(field.number, instance);
      }
    }
    final BinaryDocValues docToOrds = getBinary(field);
    final FST<Long> fst = instance;
    
    // per-thread resources
    final BytesReader in = fst.getBytesReader();
    final Arc<Long> firstArc = new Arc<>();
    final Arc<Long> scratchArc = new Arc<>();
    final IntsRefBuilder scratchInts = new IntsRefBuilder();
    final BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<>(fst);
    final ByteArrayDataInput input = new ByteArrayDataInput();
    return new SortedSetDocValues() {
      final BytesRefBuilder term = new BytesRefBuilder();
      BytesRef ordsRef;
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
        ordsRef = docToOrds.get(docID);
        input.reset(ordsRef.bytes, ordsRef.offset, ordsRef.length);
        currentOrd = 0;
      }

      @Override
      public BytesRef lookupOrd(long ord) {
        try {
          in.setPosition(0);
          fst.getFirstArc(firstArc);
          IntsRef output = Util.getByOutput(fst, ord, in, firstArc, scratchArc, scratchInts);
          term.grow(output.length);
          term.clear();
          return Util.toBytesRef(output, term);
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
  public Bits getDocsWithField(FieldInfo field) throws IOException {
    if (field.getDocValuesType() == FieldInfo.DocValuesType.SORTED_SET) {
      return DocValues.docsWithValue(getSortedSet(field), maxDoc);
    } else {
      return new Bits.MatchAllBits(maxDoc);
    }
  }
  
  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    throw new IllegalStateException("Lucene 4.2 does not support SortedNumeric: how did you pull this off?");
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
    final Arc<Long> firstArc = new Arc<>();
    final Arc<Long> scratchArc = new Arc<>();
    final IntsRefBuilder scratchInts = new IntsRefBuilder();
    final BytesRefBuilder scratchBytes = new BytesRefBuilder();
    
    FSTTermsEnum(FST<Long> fst) {
      this.fst = fst;
      in = new BytesRefFSTEnum<>(fst);
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
    public SeekStatus seekCeil(BytesRef text) throws IOException {
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
    public boolean seekExact(BytesRef text) throws IOException {
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
      BytesRefBuilder scratchBytes = new BytesRefBuilder();
      scratchBytes.clear();
      Util.toBytesRef(output, scratchBytes);
      // TODO: we could do this lazily, better to try to push into FSTEnum though?
      in.seekExact(scratchBytes.get());
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
