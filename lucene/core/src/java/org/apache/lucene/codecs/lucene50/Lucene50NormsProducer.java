package org.apache.lucene.codecs.lucene50;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.packed.BlockPackedReader;
import org.apache.lucene.util.packed.MonotonicBlockPackedReader;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.codecs.lucene50.Lucene50NormsFormat.VERSION_START;
import static org.apache.lucene.codecs.lucene50.Lucene50NormsFormat.VERSION_CURRENT;
import static org.apache.lucene.codecs.lucene50.Lucene50NormsConsumer.CONST_COMPRESSED;
import static org.apache.lucene.codecs.lucene50.Lucene50NormsConsumer.DELTA_COMPRESSED;
import static org.apache.lucene.codecs.lucene50.Lucene50NormsConsumer.TABLE_COMPRESSED;
import static org.apache.lucene.codecs.lucene50.Lucene50NormsConsumer.UNCOMPRESSED;
import static org.apache.lucene.codecs.lucene50.Lucene50NormsConsumer.INDIRECT;
import static org.apache.lucene.codecs.lucene50.Lucene50NormsConsumer.PATCHED_BITSET;
import static org.apache.lucene.codecs.lucene50.Lucene50NormsConsumer.PATCHED_TABLE;

/**
 * Reader for {@link Lucene50NormsFormat}
 */
class Lucene50NormsProducer extends NormsProducer {
  // metadata maps (just file pointers and minimal stuff)
  private final Map<String,NormsEntry> norms = new HashMap<>();
  private final IndexInput data;
  
  // ram instances we have already loaded
  final Map<String,NumericDocValues> instances = new HashMap<>();
  final Map<String,Accountable> instancesInfo = new HashMap<>();
  
  private final AtomicLong ramBytesUsed;
  private final AtomicInteger activeCount = new AtomicInteger();
  private final int maxDoc;
  
  private final boolean merging;
  
  // clone for merge: when merging we don't do any instances.put()s
  Lucene50NormsProducer(Lucene50NormsProducer original) {
    assert Thread.holdsLock(original);
    norms.putAll(original.norms);
    data = original.data.clone();
    instances.putAll(original.instances);
    instancesInfo.putAll(original.instancesInfo);
    ramBytesUsed = new AtomicLong(original.ramBytesUsed.get());
    activeCount.set(original.activeCount.get());
    maxDoc = original.maxDoc;
    merging = true;
  }
    
  Lucene50NormsProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    merging = false;
    maxDoc = state.segmentInfo.getDocCount();
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOfInstance(getClass()));
    int version = -1;
    
    // read in the entries from the metadata file.
    try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context)) {
      Throwable priorE = null;
      try {
        version = CodecUtil.checkIndexHeader(in, metaCodec, VERSION_START, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
        readFields(in, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(in, priorE);
      }
    }

    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    this.data = state.directory.openInput(dataName, state.context);
    boolean success = false;
    try {
      final int version2 = CodecUtil.checkIndexHeader(data, dataCodec, VERSION_START, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      if (version != version2) {
        throw new CorruptIndexException("Format versions mismatch: meta=" + version + ",data=" + version2, data);
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
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      } else if (!info.hasNorms()) {
        throw new CorruptIndexException("Invalid field: " + info.name, meta);
      }
      NormsEntry entry = readEntry(info, meta);
      norms.put(info.name, entry);
      fieldNumber = meta.readVInt();
    }
  }
  
  private NormsEntry readEntry(FieldInfo info, IndexInput meta) throws IOException {
    NormsEntry entry = new NormsEntry();
    entry.count = meta.readVInt();
    entry.format = meta.readByte();
    entry.offset = meta.readLong();
    switch(entry.format) {
      case CONST_COMPRESSED:
      case UNCOMPRESSED:
      case TABLE_COMPRESSED:
      case DELTA_COMPRESSED:
        break;
      case PATCHED_BITSET:
      case PATCHED_TABLE:
      case INDIRECT:
        if (meta.readVInt() != info.number) {
          throw new CorruptIndexException("indirect norms entry for field: " + info.name + " is corrupt", meta);
        }
        entry.nested = readEntry(info, meta);
        break;
      default:
        throw new CorruptIndexException("Unknown format: " + entry.format, meta);
    }
    return entry;
  }

  @Override
  public synchronized NumericDocValues getNorms(FieldInfo field) throws IOException {
    NumericDocValues instance = instances.get(field.name);
    if (instance == null) {
      LoadedNorms loaded = loadNorms(norms.get(field.name));
      instance = loaded.norms;
      if (!merging) {
        instances.put(field.name, instance);
        activeCount.incrementAndGet();
        ramBytesUsed.addAndGet(loaded.ramBytesUsed);
        instancesInfo.put(field.name, loaded.info);
      }
    }
    return instance;
  }
  
  @Override
  public long ramBytesUsed() {
    return ramBytesUsed.get();
  }
  
  @Override
  public synchronized Iterable<? extends Accountable> getChildResources() {
    return Accountables.namedAccountables("field", instancesInfo);
  }
  
  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
  }

  private LoadedNorms loadNorms(NormsEntry entry) throws IOException {
    LoadedNorms instance = new LoadedNorms();
    switch(entry.format) {
      case CONST_COMPRESSED: {
        final long v = entry.offset;
        instance.info = Accountables.namedAccountable("constant", 8);
        instance.ramBytesUsed = 8;
        instance.norms = new NumericDocValues() {
          @Override
          public long get(int docID) {
            return v;
          }
        };
        break;
      }
      case UNCOMPRESSED: {
        data.seek(entry.offset);
        final byte bytes[] = new byte[entry.count];
        data.readBytes(bytes, 0, bytes.length);
        instance.info = Accountables.namedAccountable("byte array", bytes.length);
        instance.ramBytesUsed = RamUsageEstimator.sizeOf(bytes);
        instance.norms = new NumericDocValues() {
          @Override
          public long get(int docID) {
            return bytes[docID];
          }
        };
        break;
      }
      case DELTA_COMPRESSED: {
        data.seek(entry.offset);
        int packedIntsVersion = data.readVInt();
        int blockSize = data.readVInt();
        final BlockPackedReader reader = new BlockPackedReader(data, packedIntsVersion, blockSize, entry.count, false);
        instance.info = Accountables.namedAccountable("delta compressed", reader);
        instance.ramBytesUsed = reader.ramBytesUsed();
        instance.norms = reader;
        break;
      }
      case TABLE_COMPRESSED: {
        data.seek(entry.offset);
        int packedIntsVersion = data.readVInt();
        final int formatID = data.readVInt();
        final int bitsPerValue = data.readVInt();
        
        if (bitsPerValue != 1 && bitsPerValue != 2 && bitsPerValue != 4) {
          throw new CorruptIndexException("TABLE_COMPRESSED only supports bpv=1, bpv=2 and bpv=4, got=" + bitsPerValue, data);
        }
        int size = 1 << bitsPerValue;
        final byte decode[] = new byte[size];
        final int ordsSize = data.readVInt();
        for (int i = 0; i < ordsSize; ++i) {
          decode[i] = data.readByte();
        }
        for (int i = ordsSize; i < size; ++i) {
          decode[i] = 0;
        }

        final PackedInts.Reader ordsReader = PackedInts.getReaderNoHeader(data, PackedInts.Format.byId(formatID), packedIntsVersion, entry.count, bitsPerValue);
        instance.info = Accountables.namedAccountable("table compressed", ordsReader);
        instance.ramBytesUsed = RamUsageEstimator.sizeOf(decode) + ordsReader.ramBytesUsed();
        instance.norms = new NumericDocValues() {
          @Override
          public long get(int docID) {
            return decode[(int)ordsReader.get(docID)];
          }
        };
        break;
      }
      case INDIRECT: {
        data.seek(entry.offset);
        final long common = data.readLong();
        int packedIntsVersion = data.readVInt();
        int blockSize = data.readVInt();
        final MonotonicBlockPackedReader live = MonotonicBlockPackedReader.of(data, packedIntsVersion, blockSize, entry.count, false);
        LoadedNorms nestedInstance = loadNorms(entry.nested);
        instance.ramBytesUsed = live.ramBytesUsed() + nestedInstance.ramBytesUsed;
        instance.info = Accountables.namedAccountable("indirect -> " + nestedInstance.info, instance.ramBytesUsed);
        final NumericDocValues values = nestedInstance.norms;
        final int upperBound = entry.count-1;
        instance.norms = new NumericDocValues() {
          @Override
          public long get(int docID) {
            int low = 0;
            int high = upperBound;
            while (low <= high) {
              int mid = (low + high) >>> 1;
              long doc = live.get(mid);
              
              if (doc < docID) {
                low = mid + 1;
              } else if (doc > docID) {
                high = mid - 1;
              } else {
                return values.get(mid);
              }
            }
            return common;
          }
        };
        break;
      }
      case PATCHED_BITSET: {
        data.seek(entry.offset);
        final long common = data.readLong();
        int packedIntsVersion = data.readVInt();
        int blockSize = data.readVInt();
        MonotonicBlockPackedReader live = MonotonicBlockPackedReader.of(data, packedIntsVersion, blockSize, entry.count, true);
        final SparseFixedBitSet set = new SparseFixedBitSet(maxDoc);
        for (int i = 0; i < live.size(); i++) {
          int doc = (int) live.get(i);
          set.set(doc);
        }
        LoadedNorms nestedInstance = loadNorms(entry.nested);
        instance.ramBytesUsed = set.ramBytesUsed() + nestedInstance.ramBytesUsed;
        instance.info = Accountables.namedAccountable("patched bitset -> " + nestedInstance.info, instance.ramBytesUsed);
        final NumericDocValues values = nestedInstance.norms;
        instance.norms = new NumericDocValues() {
          @Override
          public long get(int docID) {
            if (set.get(docID)) {
              return values.get(docID);
            } else {
              return common;
            }
          }
        };
        break;
      }
      case PATCHED_TABLE: {
        data.seek(entry.offset);
        int packedIntsVersion = data.readVInt();
        final int formatID = data.readVInt();
        final int bitsPerValue = data.readVInt();

        if (bitsPerValue != 2 && bitsPerValue != 4) {
          throw new CorruptIndexException("PATCHED_TABLE only supports bpv=2 and bpv=4, got=" + bitsPerValue, data);
        }
        final int size = 1 << bitsPerValue;
        final int ordsSize = data.readVInt();
        final byte decode[] = new byte[ordsSize];
        assert ordsSize + 1 == size;
        for (int i = 0; i < ordsSize; ++i) {
          decode[i] = data.readByte();
        }
        
        final PackedInts.Reader ordsReader = PackedInts.getReaderNoHeader(data, PackedInts.Format.byId(formatID), packedIntsVersion, entry.count, bitsPerValue);
        final LoadedNorms nestedInstance = loadNorms(entry.nested);
        instance.ramBytesUsed = RamUsageEstimator.sizeOf(decode) + ordsReader.ramBytesUsed() + nestedInstance.ramBytesUsed;
        instance.info = Accountables.namedAccountable("patched table -> " + nestedInstance.info, instance.ramBytesUsed);
        final NumericDocValues values = nestedInstance.norms;
        instance.norms = new NumericDocValues() {
          @Override
          public long get(int docID) {
            int ord = (int)ordsReader.get(docID);
            try {
              // doing a try/catch here eliminates a seemingly unavoidable branch in hotspot...
              return decode[ord];
            } catch (IndexOutOfBoundsException e) {
              return values.get(docID);
            }
          }
        };
        break;
      }
      default:
        throw new AssertionError();
    }
    return instance;
  }

  @Override
  public void close() throws IOException {
    data.close();
  }
  
  static class NormsEntry {
    byte format;
    long offset;
    int count;
    NormsEntry nested;
  }
  
  static class LoadedNorms {
    NumericDocValues norms;
    long ramBytesUsed;
    Accountable info;
  }

  @Override
  public synchronized NormsProducer getMergeInstance() throws IOException {
    return new Lucene50NormsProducer(this);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + norms.size() + ",active=" + activeCount.get() + ")";
  }
}
