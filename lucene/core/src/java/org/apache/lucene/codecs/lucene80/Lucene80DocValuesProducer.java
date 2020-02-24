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
package org.apache.lucene.codecs.lucene80;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

/** reader for {@link Lucene80DocValuesFormat} */
final class Lucene80DocValuesProducer extends DocValuesProducer implements Closeable {
  private static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOfInstance(Lucene80DocValuesProducer.class);

  private final Map<String, Lucene80NumericProducer.NumericEntry> numerics = new HashMap<>();
  private final Map<String, Lucene80BinaryProducer.BinaryEntry> binaries = new HashMap<>();
  private final Map<String, Lucene80SortedSetProducer.SortedEntry> sorted = new HashMap<>();
  private final Map<String, Lucene80SortedSetProducer.SortedSetEntry> sortedSets = new HashMap<>();
  private final Map<String, Lucene80NumericProducer.SortedNumericEntry> sortedNumerics = new HashMap<>();

  private final IndexInput data;
  private int version = -1;

  private final Lucene80BinaryProducer binaryProducer;
  private final Lucene80NumericProducer numericProducer;
  private final Lucene80SortedSetProducer sortedSetProducer;

  /** expert: instantiates a new reader */
  Lucene80DocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    int maxDoc = state.segmentInfo.maxDoc();
    binaryProducer = new Lucene80BinaryProducer(maxDoc);
    numericProducer = new Lucene80NumericProducer(maxDoc);
    sortedSetProducer = new Lucene80SortedSetProducer(maxDoc);
    // read in the entries from the metadata file.
    try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context)) {
      Throwable priorE = null;

      try {
        version = CodecUtil.checkIndexHeader(in, metaCodec,
                                        Lucene80DocValuesFormat.VERSION_START,
                                        Lucene80DocValuesFormat.VERSION_CURRENT,
                                        state.segmentInfo.getId(),
                                        state.segmentSuffix);
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
      final int version2 = CodecUtil.checkIndexHeader(data, dataCodec,
                                                 Lucene80DocValuesFormat.VERSION_START,
                                                 Lucene80DocValuesFormat.VERSION_CURRENT,
                                                 state.segmentInfo.getId(),
                                                 state.segmentSuffix);
      if (version != version2) {
        throw new CorruptIndexException("Format versions mismatch: meta=" + version + ", data=" + version2, data);
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

  private void readFields(ChecksumIndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      byte type = meta.readByte();
      if (type == Lucene80DocValuesFormat.NUMERIC) {
        numerics.put(info.name, Lucene80NumericProducer.readNumeric(meta));
      } else if (type == Lucene80DocValuesFormat.BINARY) {
        binaries.put(info.name, Lucene80BinaryProducer.readBinary(meta, version));
      } else if (type == Lucene80DocValuesFormat.SORTED) {
        sorted.put(info.name, Lucene80SortedSetProducer.readSorted(meta));
      } else if (type == Lucene80DocValuesFormat.SORTED_SET) {
        sortedSets.put(info.name, Lucene80SortedSetProducer.readSortedSet(meta));
      } else if (type == Lucene80DocValuesFormat.SORTED_NUMERIC) {
        sortedNumerics.put(info.name, Lucene80NumericProducer.readSortedNumeric(meta));
      } else {
        throw new CorruptIndexException("invalid type: " + type, meta);
      }
    }
  }

  @Override
  public void close() throws IOException {
    data.close();
  }

  @Override
  public long ramBytesUsed() {
    long ramBytesUsed = BASE_RAM_USAGE;
    for (Accountable accountable : numerics.values()) {
      ramBytesUsed += accountable.ramBytesUsed();
    }
    for (Accountable accountable : sortedNumerics.values()) {
      ramBytesUsed += accountable.ramBytesUsed();
    }
    for (Accountable accountable : binaries.values()) {
      ramBytesUsed += accountable.ramBytesUsed();
    }
    for (Accountable accountable : sorted.values()) {
      ramBytesUsed += accountable.ramBytesUsed();
    }
    for (Accountable accountable : sortedSets.values()) {
      ramBytesUsed += accountable.ramBytesUsed();
    }
    return ramBytesUsed;
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    Lucene80NumericProducer.NumericEntry entry = numerics.get(field.name);
    return numericProducer.getNumeric(entry, data);
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    Lucene80BinaryProducer.BinaryEntry binaryEntry = binaries.get(field.name);
    return binaryProducer.getBinaryFromEntry(binaryEntry, data, version);
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    Lucene80SortedSetProducer.SortedEntry entry = sorted.get(field.name);
    return sortedSetProducer.getSorted(entry, data);
  }

  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    Lucene80NumericProducer.SortedNumericEntry entry = sortedNumerics.get(field.name);
    return numericProducer.getSortedNumeric(entry, data);
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    Lucene80SortedSetProducer.SortedSetEntry entry = sortedSets.get(field.name);
    return sortedSetProducer.getSortedSet(entry, data);
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
  }

}