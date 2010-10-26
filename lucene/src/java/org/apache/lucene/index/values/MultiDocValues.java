package org.apache.lucene.index.values;
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
import java.util.List;

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.MultiTermsEnum.TermsEnumIndex;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FloatsRef;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.ReaderUtil.Slice;

public class MultiDocValues extends DocValues {

  public static class DocValuesIndex {
    public final static DocValuesIndex[] EMPTY_ARRAY = new DocValuesIndex[0];
    final int subIndex;
    final DocValues docValues;

    public DocValuesIndex(DocValues docValues, int subIndex) {
      this.docValues = docValues;
      this.subIndex = subIndex;
    }
  }

  private DocValuesIndex[] docValuesIdx;
  private Slice[] subSlices;

  public MultiDocValues(Slice[] subSlices) {
    this.subSlices = subSlices;
  }

  public MultiDocValues(DocValuesIndex[] docValuesIdx, Slice[] subSlices) {
    this(subSlices);
    reset(docValuesIdx);
  }

  @Override
  public ValuesEnum getEnum(AttributeSource source) throws IOException {
    return new MultiValuesEnum(subSlices, docValuesIdx, docValuesIdx[0].docValues.type());
  }

  @Override
  public Source load() throws IOException {
    return new MultiSource(subSlices, docValuesIdx);
  }

  public void close() throws IOException {
    //      
  }

  public DocValues reset(DocValuesIndex[] docValuesIdx) {
    this.docValuesIdx = docValuesIdx;
    return this;
  }

  private static class MultiValuesEnum extends ValuesEnum {
    private int numDocs_ = 0;
    private int pos = -1;
    private int start = 0;
    private ValuesEnum current;
    private Slice[] subSlices;
    private DocValuesIndex[] docValuesIdx;
    private final int maxDoc;

    public MultiValuesEnum(Slice[] subSlices, DocValuesIndex[] docValuesIdx, Values type) {
      super(type);
      this.subSlices = subSlices;
      this.docValuesIdx = docValuesIdx;
      Slice slice = subSlices[subSlices.length-1];
      maxDoc = slice.start + slice.length;
    }

    @Override
    public void close() throws IOException {
      
    }

    @Override
    public int advance(int target) throws IOException {
//      int n = target - start;
//      do {
//        if (target >= maxDoc)
//          return pos = NO_MORE_DOCS;
//        if (n >= numDocs_) {
//          int idx = readerIndex(target);
//          if (enumCache[idx] == null) {
//            try {
//              DocValues indexValues = subReaders[idx].docValues(id);
//              if (indexValues != null) // nocommit does that work with default
//                // values?
//                enumCache[idx] = indexValues.getEnum(this.attributes());
//              else
//                enumCache[idx] = new DummyEnum(this.attributes(),
//                    subSlices[idx].length, attr.type());
//            } catch (IOException ex) {
//              // nocommit what to do here?
//              throw new RuntimeException(ex);
//            }
//          }
//          current = enumCache[idx];
//          start = subSlices[idx].start;
//          numDocs_ = subSlices[idx].length;
//          n = target - start;
//        }
//        target = start + numDocs_;
//      } while ((n = current.advance(n)) == NO_MORE_DOCS);
      return pos = start + current.docID();
    }

    @Override
    public int docID() {
      return pos;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(pos + 1);
    }
  }

  private class MultiSource extends Source {
    private int numDocs_ = 0;
    private int start = 0;
    private Source current;
    private Slice[] subSlices;
    private DocValuesIndex[] docVAluesIdx;

    public MultiSource(Slice[] subSlices, DocValuesIndex[] docValuesIdx) {
      this.subSlices = subSlices;
      this.docVAluesIdx = docValuesIdx;
    }

    public long ints(int docID) {
//      int n = docID - start;
//      if (n >= numDocs_) {
//        int idx = readerIndex(docID);
//        try {
//          current = subReaders[idx].getIndexValuesCache().getInts(id);
//          if (current == null) // nocommit does that work with default values?
//            current = new DummySource();
//        } catch (IOException ex) {
//          // nocommit what to do here?
//          throw new RuntimeException(ex);
//        }
//        start = starts[idx];
//        numDocs_ = subReaders[idx].maxDoc();
//        n = docID - start;
//      }
//      return current.ints(n);
      return 0l;
    }

    public double floats(int docID) {
//      int n = docID - start;
//      if (n >= numDocs_) {
//        int idx = readerIndex(docID);
//        try {
//          current = subReaders[idx].getIndexValuesCache().getFloats(id);
//          if (current == null) // nocommit does that work with default values?
//            current = new DummySource();
//        } catch (IOException ex) {
//          // nocommit what to do here?
//          throw new RuntimeException(ex);
//        }
//        numDocs_ = subReaders[idx].maxDoc();
//
//        start = starts[idx];
//        n = docID - start;
//      }
//      return current.floats(n);
      return 0d;
    }

    public BytesRef bytes(int docID) {
//      int n = docID - start;
//      if (n >= numDocs_) {
//        int idx = readerIndex(docID);
//        try {
//          current = subReaders[idx].getIndexValuesCache().getBytes(id);
//          if (current == null) // nocommit does that work with default values?
//            current = new DummySource();
//        } catch (IOException ex) {
//          // nocommit what to do here?
//          throw new RuntimeException(ex);
//        }
//        numDocs_ = subReaders[idx].maxDoc();
//        start = starts[idx];
//        n = docID - start;
//      }
//      return current.bytes(n);
      return null;
    }

    public long ramBytesUsed() {
      return current.ramBytesUsed();
    }

  }

  private static class DummySource extends Source {
    private final BytesRef ref = new BytesRef();

    @Override
    public BytesRef bytes(int docID) {
      return ref;
    }

    @Override
    public double floats(int docID) {
      return 0.0d;
    }

    @Override
    public long ints(int docID) {
      return 0;
    }

    public long ramBytesUsed() {
      return 0;
    }
  }

  private static class DummyEnum extends ValuesEnum {
    private int pos = -1;
    private final int maxDoc;

    public DummyEnum(AttributeSource source, int maxDoc, Values type) {
      super(source, type);
      this.maxDoc = maxDoc;
      switch (type) {
      case BYTES_VAR_STRAIGHT:
      case BYTES_FIXED_STRAIGHT:
      case BYTES_FIXED_DEREF:
      case BYTES_FIXED_SORTED:
      case BYTES_VAR_DEREF:
      case BYTES_VAR_SORTED:
        // nocommit - this is not correct for Fixed_straight
        BytesRef bytes = attr.bytes();
        bytes.length = 0;
        bytes.offset = 0;
        break;
      case PACKED_INTS:
      case PACKED_INTS_FIXED:
        LongsRef ints = attr.ints();
        ints.set(0);
        break;

      case SIMPLE_FLOAT_4BYTE:
      case SIMPLE_FLOAT_8BYTE:
        FloatsRef floats = attr.floats();
        floats.set(0d);
        break;
      default:
        throw new IllegalArgumentException("unknown Values type: " + type);
      }
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public int advance(int target) throws IOException {
      return pos = (pos < maxDoc ? target : NO_MORE_DOCS);
    }

    @Override
    public int docID() {
      return pos;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(pos + 1);
    }
  }

  @Override
  public Values type() {
    return this.docValuesIdx[0].docValues.type();
  }

}
