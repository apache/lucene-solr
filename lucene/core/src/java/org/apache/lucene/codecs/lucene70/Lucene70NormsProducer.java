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
package org.apache.lucene.codecs.lucene70;

import static org.apache.lucene.codecs.lucene70.Lucene70NormsFormat.VERSION_CURRENT;
import static org.apache.lucene.codecs.lucene70.Lucene70NormsFormat.VERSION_START;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.IOUtils;

/**
 * Reader for {@link Lucene70NormsFormat}
 */
final class Lucene70NormsProducer extends NormsProducer {
  // metadata maps (just file pointers and minimal stuff)
  private final Map<Integer,NormsEntry> norms = new HashMap<>();
  private final IndexInput data;
  private final int maxDoc;

  Lucene70NormsProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
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

  static class NormsEntry {
    byte bytesPerNorm;
    long docsWithFieldOffset;
    long docsWithFieldLength;
    int numDocsWithField;
    long normsOffset;
  }

  static abstract class DenseNormsIterator extends NumericDocValues {

    final int maxDoc;
    int doc = -1;

    DenseNormsIterator(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= maxDoc) {
        return doc = NO_MORE_DOCS;
      }
      return doc = target;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      this.doc = target;
      return true;
    }

    @Override
    public long cost() {
      return maxDoc;
    }

  }

  static abstract class SparseNormsIterator extends NumericDocValues {

    final IndexedDISI disi;

    SparseNormsIterator(IndexedDISI disi) {
      this.disi = disi;
    }

    @Override
    public int docID() {
      return disi.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return disi.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return disi.advance(target);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      return disi.advanceExact(target);
    }

    @Override
    public long cost() {
      return disi.cost();
    }
  }

  private void readFields(IndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      } else if (!info.hasNorms()) {
        throw new CorruptIndexException("Invalid field: " + info.name, meta);
      }
      NormsEntry entry = new NormsEntry();
      entry.docsWithFieldOffset = meta.readLong();
      entry.docsWithFieldLength = meta.readLong();
      entry.numDocsWithField = meta.readInt();
      entry.bytesPerNorm = meta.readByte();
      switch (entry.bytesPerNorm) {
        case 0: case 1: case 2: case 4: case 8:
          break;
        default:
          throw new CorruptIndexException("Invalid bytesPerValue: " + entry.bytesPerNorm + ", field: " + info.name, meta);
      }
      entry.normsOffset = meta.readLong();
      norms.put(info.number, entry);
    }
  }

  @Override
  public NumericDocValues getNorms(FieldInfo field) throws IOException {
    final NormsEntry entry = norms.get(field.number);
    if (entry.docsWithFieldOffset == -2) {
      // empty
      return DocValues.emptyNumeric();
    } else if (entry.docsWithFieldOffset == -1) {
      // dense
      if (entry.bytesPerNorm == 0) {
        return new DenseNormsIterator(maxDoc) {
          @Override
          public long longValue() throws IOException {
            return entry.normsOffset;
          }
        };
      }
      final RandomAccessInput slice = data.randomAccessSlice(entry.normsOffset, entry.numDocsWithField * (long) entry.bytesPerNorm);
      switch (entry.bytesPerNorm) {
        case 1:
          return new DenseNormsIterator(maxDoc) {
            @Override
            public long longValue() throws IOException {
              return slice.readByte(doc);
            }
          };
        case 2:
          return new DenseNormsIterator(maxDoc) {
            @Override
            public long longValue() throws IOException {
              return slice.readShort(((long) doc) << 1);
            }
          };
        case 4:
          return new DenseNormsIterator(maxDoc) {
            @Override
            public long longValue() throws IOException {
              return slice.readInt(((long) doc) << 2);
            }
          };
        case 8:
          return new DenseNormsIterator(maxDoc) {
            @Override
            public long longValue() throws IOException {
              return slice.readLong(((long) doc) << 3);
            }
          };
        default:
          // should not happen, we already validate bytesPerNorm in readFields
          throw new AssertionError();
      }
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength, entry.numDocsWithField);
      if (entry.bytesPerNorm == 0) {
        return new SparseNormsIterator(disi) {
          @Override
          public long longValue() throws IOException {
            return entry.normsOffset;
          }
        };
      }
      final RandomAccessInput slice = data.randomAccessSlice(entry.normsOffset, entry.numDocsWithField * (long) entry.bytesPerNorm);
      switch (entry.bytesPerNorm) {
        case 1:
          return new SparseNormsIterator(disi) {
            @Override
            public long longValue() throws IOException {
              return slice.readByte(disi.index());
            }
          };
        case 2:
          return new SparseNormsIterator(disi) {
            @Override
            public long longValue() throws IOException {
              return slice.readShort(((long) disi.index()) << 1);
            }
          };
        case 4:
          return new SparseNormsIterator(disi) {
            @Override
            public long longValue() throws IOException {
              return slice.readInt(((long) disi.index()) << 2);
            }
          };
        case 8:
          return new SparseNormsIterator(disi) {
            @Override
            public long longValue() throws IOException {
              return slice.readLong(((long) disi.index()) << 3);
            }
          };
        default:
          // should not happen, we already validate bytesPerNorm in readFields
          throw new AssertionError();
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

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + norms.size() + ")";
  }
}
