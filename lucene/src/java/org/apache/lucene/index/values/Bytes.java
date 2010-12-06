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

/** Base class for specific Bytes Reader/Writer implementations */
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.values.DocValues.MissingValue;
import org.apache.lucene.index.values.DocValues.SortedSource;
import org.apache.lucene.index.values.DocValues.Source;
import org.apache.lucene.index.values.DocValues.SourceEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.PagedBytes;

/**
 * Provides concrete Writer/Reader impls for byte[] value per document. There
 * are 6 package-private impls of this, for all combinations of
 * STRAIGHT/DEREF/SORTED X fixed/not fixed.
 * 
 * <p>
 * NOTE: The total amount of byte[] data stored (across a single segment) cannot
 * exceed 2GB.
 * </p>
 * <p>
 * NOTE: Each byte[] must be <= 32768 bytes in length
 * </p>
 * @lucene.experimental
 */
// TODO - add bulk copy where possible
public final class Bytes {

  // don't instantiate!
  private Bytes() {
  }

  public static enum Mode {
    STRAIGHT, DEREF, SORTED
  };

  // TODO -- i shouldn't have to specify fixed? can
  // track itself & do the write thing at write time?
  public static Writer getWriter(Directory dir, String id, Mode mode,
      Comparator<BytesRef> comp, boolean fixedSize, AtomicLong bytesUsed) throws IOException {

    if (comp == null) {
      comp = BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    if (fixedSize) {
      if (mode == Mode.STRAIGHT) {
        return new FixedStraightBytesImpl.Writer(dir, id);
      } else if (mode == Mode.DEREF) {
        return new FixedDerefBytesImpl.Writer(dir, id, bytesUsed);
      } else if (mode == Mode.SORTED) {
        return new FixedSortedBytesImpl.Writer(dir, id, comp, bytesUsed);
      }
    } else {
      if (mode == Mode.STRAIGHT) {
        return new VarStraightBytesImpl.Writer(dir, id, bytesUsed);
      } else if (mode == Mode.DEREF) {
        return new VarDerefBytesImpl.Writer(dir, id, bytesUsed);
      } else if (mode == Mode.SORTED) {
        return new VarSortedBytesImpl.Writer(dir, id, comp, bytesUsed);
      }
    }

    throw new IllegalArgumentException("");
  }

  // TODO -- I can peek @ header to determing fixed/mode?
  public static DocValues getValues(Directory dir, String id, Mode mode,
      boolean fixedSize, int maxDoc) throws IOException {
    if (fixedSize) {
      if (mode == Mode.STRAIGHT) {
        try {
          return new FixedStraightBytesImpl.Reader(dir, id, maxDoc);
        } catch (IOException e) {
          throw e;
        }
      } else if (mode == Mode.DEREF) {
        try {
          return new FixedDerefBytesImpl.Reader(dir, id, maxDoc);
        } catch (IOException e) {
          throw e;
        }
      } else if (mode == Mode.SORTED) {
        return new FixedSortedBytesImpl.Reader(dir, id, maxDoc);
      }
    } else {
      if (mode == Mode.STRAIGHT) {
        return new VarStraightBytesImpl.Reader(dir, id, maxDoc);
      } else if (mode == Mode.DEREF) {
        return new VarDerefBytesImpl.Reader(dir, id, maxDoc);
      } else if (mode == Mode.SORTED) {
        return new VarSortedBytesImpl.Reader(dir, id, maxDoc);
      }
    }

    throw new IllegalArgumentException("");
  }

  static abstract class BytesBaseSource extends Source {
    protected final IndexInput datIn;
    protected final IndexInput idxIn;
    protected final static int PAGED_BYTES_BITS = 15;
    private final PagedBytes pagedBytes;
    protected final PagedBytes.Reader data;
    protected final long totalLengthInBytes;

    protected BytesBaseSource(IndexInput datIn, IndexInput idxIn,
        PagedBytes pagedBytes, long bytesToRead) throws IOException {
      assert bytesToRead <= datIn.length() : " file size is less than the expected size diff: "
          + (bytesToRead - datIn.length()) + " pos: " + datIn.getFilePointer();
      this.datIn = datIn;
      this.totalLengthInBytes = bytesToRead;
      this.pagedBytes = pagedBytes;
      this.pagedBytes.copy(datIn, bytesToRead);
      data = pagedBytes.freeze(true);
      this.idxIn = idxIn;
    }

    public void close() throws IOException {
      data.close();
      try {
        if (datIn != null)
          datIn.close();
      } finally {
        if (idxIn != null) // if straight - no index needed
          idxIn.close();
      }
    }
    
    protected abstract int maxDoc();

    public long ramBytesUsed() {
      return 0; // TODO
    }

    @Override
    public ValuesEnum getEnum(AttributeSource attrSource) throws IOException {
      final MissingValue missing = getMissing();
      return new SourceEnum(attrSource, type(), this, maxDoc()) {
        @Override
        public int advance(int target) throws IOException {
          if (target >= numDocs) {
            return pos = NO_MORE_DOCS;
          }
          while (source.getBytes(target, bytesRef) == missing.bytesValue) {
            if (++target >= numDocs) {
              return pos = NO_MORE_DOCS;
            }
          }
          return pos = target;
        }
      };
    }

  }

  static abstract class BytesBaseSortedSource extends SortedSource {
    protected final IndexInput datIn;
    protected final IndexInput idxIn;
    protected final BytesRef defaultValue = new BytesRef();
    protected final static int PAGED_BYTES_BITS = 15;
    private final PagedBytes pagedBytes;
    protected final PagedBytes.Reader data;
    protected final LookupResult lookupResult = new LookupResult();
    private final Comparator<BytesRef> comp;

    protected BytesBaseSortedSource(IndexInput datIn, IndexInput idxIn,
        Comparator<BytesRef> comp, PagedBytes pagedBytes, long bytesToRead)
        throws IOException {
      assert bytesToRead <= datIn.length() : " file size is less than the expected size diff: "
          + (bytesToRead - datIn.length()) + " pos: " + datIn.getFilePointer();
      this.datIn = datIn;
      this.pagedBytes = pagedBytes;
      this.pagedBytes.copy(datIn, bytesToRead);
      data = pagedBytes.freeze(true);
      this.idxIn = idxIn;
      this.comp = comp == null ? BytesRef.getUTF8SortedAsUnicodeComparator()
          : comp;

    }

    @Override
    public BytesRef getByOrd(int ord, BytesRef bytesRef) {
      return ord == 0 ? null : deref(--ord, bytesRef);
    }

    public void close() throws IOException {
      if (datIn != null)
        datIn.close();
      if (idxIn != null) // if straight
        idxIn.close();
    }

    protected abstract int maxDoc();

    protected abstract BytesRef deref(int ord, BytesRef bytesRef);

    protected LookupResult binarySearch(BytesRef b, BytesRef bytesRef, int low,
        int high) {
      while (low <= high) {
        int mid = (low + high) >>> 1;
        deref(mid, bytesRef);
        final int cmp = comp.compare(bytesRef, b);
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          lookupResult.ord = mid + 1;
          lookupResult.found = true;
          return lookupResult;
        }
      }
      assert comp.compare(bytesRef, b) != 0;
      lookupResult.ord = low;
      lookupResult.found = false;
      return lookupResult;
    }

    @Override
    public ValuesEnum getEnum(AttributeSource attrSource) throws IOException {
      final MissingValue missing = getMissing();
      return new SourceEnum(attrSource, type(), this, maxDoc()) {

        @Override
        public int advance(int target) throws IOException {
          if (target >= numDocs) {
            return pos = NO_MORE_DOCS;
          }
          while (source.getBytes(target, bytesRef) == missing.bytesValue) {
            if (++target >= numDocs) {
              return pos = NO_MORE_DOCS;
            }
          }
          return pos = target;
        }
      };
    }
  }

  static abstract class BytesWriterBase extends Writer {

    private final Directory dir;
    private final String id;
    protected IndexOutput idxOut;
    protected IndexOutput datOut;
    protected BytesRef bytesRef;
    private final String codecName;
    private final int version;
    protected final ByteBlockPool pool;

    protected BytesWriterBase(Directory dir, String id, String codecName,
        int version, boolean initIndex, boolean initData, ByteBlockPool pool,
        AtomicLong bytesUsed) throws IOException {
      super(bytesUsed);
      this.dir = dir;
      this.id = id;
      this.codecName = codecName;
      this.version = version;
      this.pool = pool;
      if (initData)
        initDataOut();
      if (initIndex)
        initIndexOut();
    }

    protected void initDataOut() throws IOException {
      datOut = dir.createOutput(IndexFileNames.segmentFileName(id, "",
          DATA_EXTENSION));
      CodecUtil.writeHeader(datOut, codecName, version);
    }

    protected void initIndexOut() throws IOException {
      idxOut = dir.createOutput(IndexFileNames.segmentFileName(id, "",
          INDEX_EXTENSION));
      CodecUtil.writeHeader(idxOut, codecName, version);
    }

    public long ramBytesUsed() {
      return bytesUsed.get();
    }

    /**
     * Must be called only with increasing docIDs. It's OK for some docIDs to be
     * skipped; they will be filled with 0 bytes.
     */
    @Override
    public abstract void add(int docID, BytesRef bytes) throws IOException;

    @Override
    public synchronized void finish(int docCount) throws IOException {
      if (datOut != null)
        datOut.close();
      if (idxOut != null)
        idxOut.close();
      if (pool != null)
        pool.reset();
    }

    @Override
    protected void add(int docID) throws IOException {
      add(docID, bytesRef);
    }


    @Override
    public void add(int docID, PerDocFieldValues docValues) throws IOException {
      final BytesRef ref;
      if ((ref = docValues.getBytes()) != null) {
        add(docID, ref);
      }
    }
    
    @Override
    protected void setNextEnum(ValuesEnum valuesEnum) {
      bytesRef = valuesEnum.bytes();
    }

    @Override
    public void files(Collection<String> files) throws IOException {
      assert datOut != null;
      files.add(IndexFileNames.segmentFileName(id, "", DATA_EXTENSION));
      if (idxOut != null) { // called after flush - so this must be initialized
        // if needed or present
        final String idxFile = IndexFileNames.segmentFileName(id, "",
            INDEX_EXTENSION);
        files.add(idxFile);
      }
    }
  }

  /**
   * Opens all necessary files, but does not read any data in until you call
   * {@link #load}.
   */
  static abstract class BytesReaderBase extends DocValues {
    protected final IndexInput idxIn;
    protected final IndexInput datIn;
    protected final int version;
    protected final String id;

    protected BytesReaderBase(Directory dir, String id, String codecName,
        int maxVersion, boolean doIndex) throws IOException {
      this.id = id;
      datIn = dir.openInput(IndexFileNames.segmentFileName(id, "",
          Writer.DATA_EXTENSION));
      version = CodecUtil.checkHeader(datIn, codecName, maxVersion, maxVersion);

      if (doIndex) {
        idxIn = dir.openInput(IndexFileNames.segmentFileName(id, "",
            Writer.INDEX_EXTENSION));
        final int version2 = CodecUtil.checkHeader(idxIn, codecName,
            maxVersion, maxVersion);
        assert version == version2;
      } else {
        idxIn = null;
      }

    }

    protected final IndexInput cloneData() {
      assert datIn != null;
      return (IndexInput) datIn.clone();
    }

    protected final IndexInput cloneIndex() { // TODO assert here for null
      // rather than return null
      return idxIn == null ? null : (IndexInput) idxIn.clone();
    }

    @Override
    public void close() throws IOException {
      super.close();
      if (datIn != null) {
        datIn.close();
      }
      if (idxIn != null) {
        idxIn.close();
      }
    }
  }

}