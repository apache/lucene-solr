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
package org.apache.lucene.codecs.lucene53;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.codecs.lucene53.Lucene53NormsFormat.VERSION_CURRENT;
import static org.apache.lucene.codecs.lucene53.Lucene53NormsFormat.VERSION_START;

/**
 * Reader for {@link Lucene53NormsFormat}
 */
class Lucene53NormsProducer extends NormsProducer {
  // metadata maps (just file pointers and minimal stuff)
  private final Map<Integer,NormsEntry> norms = new HashMap<>();
  private final IndexInput data;
  private final int maxDoc;

  Lucene53NormsProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    maxDoc = state.segmentInfo.maxDoc();
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
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
    data = state.directory.openInput(dataName, state.context);
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
      NormsEntry entry = new NormsEntry();
      entry.bytesPerValue = meta.readByte();
      switch (entry.bytesPerValue) {
        case 0: case 1: case 2: case 4: case 8:
          break;
        default:
          throw new CorruptIndexException("Invalid bytesPerValue: " + entry.bytesPerValue + ", field: " + info.name, meta);
      }
      entry.offset = meta.readLong();
      norms.put(info.number, entry);
      fieldNumber = meta.readVInt();
    }
  }

  @Override
  public NumericDocValues getNorms(FieldInfo field) throws IOException {
    final NormsEntry entry = norms.get(field.number);

    if (entry.bytesPerValue == 0) {
      final long value = entry.offset;
      return new NormsIterator(maxDoc) {
          @Override
          public long longValue() {
            return value;
          }
        };
    } else {
      RandomAccessInput slice;
      synchronized (data) {
        switch (entry.bytesPerValue) {
        case 1: 
          slice = data.randomAccessSlice(entry.offset, maxDoc);
          return new NormsIterator(maxDoc) {
            @Override
            public long longValue() throws IOException {
              return slice.readByte(docID);
            }
          };
        case 2: 
          slice = data.randomAccessSlice(entry.offset, maxDoc * 2L);
          return new NormsIterator(maxDoc) {
            @Override
            public long longValue() throws IOException {
              return slice.readShort(((long)docID) << 1L);
            }
          };
        case 4: 
          slice = data.randomAccessSlice(entry.offset, maxDoc * 4L);
          return new NormsIterator(maxDoc) {
            @Override
            public long longValue() throws IOException {
              return slice.readInt(((long)docID) << 2L);
            }
          };
        case 8: 
          slice = data.randomAccessSlice(entry.offset, maxDoc * 8L);
          return new NormsIterator(maxDoc) {
            @Override
            public long longValue() throws IOException {
              return slice.readLong(((long)docID) << 3L);
            }
          };
        default:
          throw new AssertionError();
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    data.close();
  }

  @Override
  public long ramBytesUsed() {
    return 64L * norms.size(); // good enough
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
  }

  static class NormsEntry {
    byte bytesPerValue;
    long offset;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + norms.size() + ")";
  }

  private static abstract class NormsIterator extends NumericDocValues {
    private final int maxDoc;
    protected int docID = -1;
  
    public NormsIterator(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        docID = NO_MORE_DOCS;
      }
      return docID;
    }

    @Override
    public int advance(int target) {
      docID = target;
      if (docID >= maxDoc) {
        docID = NO_MORE_DOCS;
      }
      return docID;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      docID = target;
      return true;
    }

    @Override
    public long cost() {
      // TODO
      return 0;
    }
  }
}
