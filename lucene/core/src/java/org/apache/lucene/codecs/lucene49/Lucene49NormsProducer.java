package org.apache.lucene.codecs.lucene49;

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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.BlockPackedReader;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.codecs.lucene49.Lucene49NormsFormat.VERSION_START;
import static org.apache.lucene.codecs.lucene49.Lucene49NormsFormat.VERSION_CURRENT;
import static org.apache.lucene.codecs.lucene49.Lucene49NormsConsumer.CONST_COMPRESSED;
import static org.apache.lucene.codecs.lucene49.Lucene49NormsConsumer.DELTA_COMPRESSED;
import static org.apache.lucene.codecs.lucene49.Lucene49NormsConsumer.TABLE_COMPRESSED;
import static org.apache.lucene.codecs.lucene49.Lucene49NormsConsumer.UNCOMPRESSED;

/**
 * Reader for {@link Lucene49NormsFormat}
 */
class Lucene49NormsProducer extends DocValuesProducer {
  // metadata maps (just file pointers and minimal stuff)
  private final Map<Integer,NormsEntry> norms = new HashMap<>();
  private final IndexInput data;
  private final int version;
  
  // ram instances we have already loaded
  final Map<Integer,NumericDocValues> instances = new HashMap<>();
  
  private final int maxDoc;
  private final AtomicLong ramBytesUsed;
    
  Lucene49NormsProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    maxDoc = state.segmentInfo.getDocCount();
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    // read in the entries from the metadata file.
    ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context);
    boolean success = false;
    ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOfInstance(getClass()));
    try {
      version = CodecUtil.checkHeader(in, metaCodec, VERSION_START, VERSION_CURRENT);
      readFields(in, state.fieldInfos);
      CodecUtil.checkFooter(in);
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
      final int version2 = CodecUtil.checkHeader(data, dataCodec, VERSION_START, VERSION_CURRENT);
      if (version != version2) {
        throw new CorruptIndexException("Format versions mismatch");
      }
      
      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(data);

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
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber + " (resource=" + meta + ")");
      } else if (!info.hasNorms()) {
        throw new CorruptIndexException("Invalid field: " + info.name + " (resource=" + meta + ")");
      }
      NormsEntry entry = new NormsEntry();
      entry.format = meta.readByte();
      entry.offset = meta.readLong();
      switch(entry.format) {
        case CONST_COMPRESSED:
        case UNCOMPRESSED:
        case TABLE_COMPRESSED:
        case DELTA_COMPRESSED:
          break;
        default:
          throw new CorruptIndexException("Unknown format: " + entry.format + ", input=" + meta);
      }
      norms.put(fieldNumber, entry);
      fieldNumber = meta.readVInt();
    }
  }

  @Override
  public synchronized NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericDocValues instance = instances.get(field.number);
    if (instance == null) {
      instance = loadNorms(field);
      instances.put(field.number, instance);
    }
    return instance;
  }
  
  @Override
  public long ramBytesUsed() {
    return ramBytesUsed.get();
  }
  
  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
  }

  private NumericDocValues loadNorms(FieldInfo field) throws IOException {
    NormsEntry entry = norms.get(field.number);
    switch(entry.format) {
      case CONST_COMPRESSED:
        final long v = entry.offset;
        return new NumericDocValues() {
          @Override
          public long get(int docID) {
            return v;
          }
        };
      case UNCOMPRESSED:
        data.seek(entry.offset);
        final byte bytes[] = new byte[maxDoc];
        data.readBytes(bytes, 0, bytes.length);
        ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(bytes));
        return new NumericDocValues() {
          @Override
          public long get(int docID) {
            return bytes[docID];
          }
        };
      case DELTA_COMPRESSED:
        data.seek(entry.offset);
        int packedIntsVersion = data.readVInt();
        int blockSize = data.readVInt();
        final BlockPackedReader reader = new BlockPackedReader(data, packedIntsVersion, blockSize, maxDoc, false);
        ramBytesUsed.addAndGet(reader.ramBytesUsed());
        return reader;
      case TABLE_COMPRESSED:
        data.seek(entry.offset);
        int packedVersion = data.readVInt();
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
        final PackedInts.Reader ordsReader = PackedInts.getReaderNoHeader(data, PackedInts.Format.byId(formatID), packedVersion, maxDoc, bitsPerValue);
        ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(decode) + ordsReader.ramBytesUsed());
        return new NumericDocValues() {
          @Override
          public long get(int docID) {
            return decode[(int)ordsReader.get(docID)];
          }
        };
      default:
        throw new AssertionError();
    }
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    throw new IllegalStateException();
  }
  
  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    throw new IllegalStateException();
  }
  
  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    throw new IllegalStateException();
  }
  
  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    throw new IllegalStateException();
  }

  @Override
  public Bits getDocsWithField(FieldInfo field) throws IOException {
    throw new IllegalStateException();
  }

  @Override
  public void close() throws IOException {
    data.close();
  }
  
  static class NormsEntry {
    byte format;
    long offset;
  }
}
