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
import org.apache.lucene.index.values.IndexDocValues.SortedSource;
import org.apache.lucene.index.values.IndexDocValues.Source;
import org.apache.lucene.index.values.IndexDocValues.SourceEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PagedBytes;

/**
 * Provides concrete Writer/Reader implementations for <tt>byte[]</tt> value per
 * document. There are 6 package-private default implementations of this, for
 * all combinations of {@link Mode#DEREF}/{@link Mode#STRAIGHT}/
 * {@link Mode#SORTED} x fixed-length/variable-length.
 * 
 * <p>
 * NOTE: Currently the total amount of byte[] data stored (across a single
 * segment) cannot exceed 2GB.
 * </p>
 * <p>
 * NOTE: Each byte[] must be <= 32768 bytes in length
 * </p>
 * 
 * @lucene.experimental
 */
public final class Bytes {
  // TODO - add bulk copy where possible
  private Bytes() { /* don't instantiate! */
  }

  /**
   * Defines the {@link Writer}s store mode. The writer will either store the
   * bytes sequentially ({@link #STRAIGHT}, dereferenced ({@link #DEREF}) or
   * sorted ({@link #SORTED})
   * 
   * @lucene.experimental
   */
  public static enum Mode {
    /**
     * Mode for sequentially stored bytes
     */
    STRAIGHT,
    /**
     * Mode for dereferenced stored bytes
     */
    DEREF,
    /**
     * Mode for sorted stored bytes
     */
    SORTED
  };

  /**
   * Creates a new <tt>byte[]</tt> {@link Writer} instances for the given
   * directory.
   * 
   * @param dir
   *          the directory to write the values to
   * @param id
   *          the id used to create a unique file name. Usually composed out of
   *          the segment name and a unique id per segment.
   * @param mode
   *          the writers store mode
   * @param comp
   *          a {@link BytesRef} comparator - only used with {@link Mode#SORTED}
   * @param fixedSize
   *          <code>true</code> if all bytes subsequently passed to the
   *          {@link Writer} will have the same length
   * @param bytesUsed
   *          an {@link AtomicLong} instance to track the used bytes within the
   *          {@link Writer}. A call to {@link Writer#finish(int)} will release
   *          all internally used resources and frees the memeory tracking
   *          reference.
   * @return a new {@link Writer} instance
   * @throws IOException
   *           if the files for the writer can not be created.
   */
  public static Writer getWriter(Directory dir, String id, Mode mode,
      Comparator<BytesRef> comp, boolean fixedSize, AtomicLong bytesUsed)
      throws IOException {
    // TODO -- i shouldn't have to specify fixed? can
    // track itself & do the write thing at write time?
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

  /**
   * Creates a new {@link IndexDocValues} instance that provides either memory
   * resident or iterative access to a per-document stored <tt>byte[]</tt>
   * value. The returned {@link IndexDocValues} instance will be initialized without
   * consuming a significant amount of memory.
   * 
   * @param dir
   *          the directory to load the {@link IndexDocValues} from.
   * @param id
   *          the file ID in the {@link Directory} to load the values from.
   * @param mode
   *          the mode used to store the values
   * @param fixedSize
   *          <code>true</code> iff the values are stored with fixed-size,
   *          otherwise <code>false</code>
   * @param maxDoc
   *          the number of document values stored for the given ID
   * @return an initialized {@link IndexDocValues} instance.
   * @throws IOException
   *           if an {@link IOException} occurs
   */
  public static IndexDocValues getValues(Directory dir, String id, Mode mode,
      boolean fixedSize, int maxDoc) throws IOException {
    // TODO -- I can peek @ header to determing fixed/mode?
    if (fixedSize) {
      if (mode == Mode.STRAIGHT) {
        return new FixedStraightBytesImpl.Reader(dir, id, maxDoc);
      } else if (mode == Mode.DEREF) {
        return new FixedDerefBytesImpl.Reader(dir, id, maxDoc);
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

    throw new IllegalArgumentException("Illegal Mode: " + mode);
  }

  // TODO open up this API?
  static abstract class BytesBaseSource extends Source {
    private final PagedBytes pagedBytes;
    protected final IndexInput datIn;
    protected final IndexInput idxIn;
    protected final static int PAGED_BYTES_BITS = 15;
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
      try {
        data.close(); // close data
      } finally {
        try {
          if (datIn != null) {
            datIn.close();
          }
        } finally {
          if (idxIn != null) {// if straight - no index needed
            idxIn.close();
          }
        }
      }
    }

    /**
     * Returns one greater than the largest possible document number.
     */
    protected abstract int maxDoc();

    @Override
    public ValuesEnum getEnum(AttributeSource attrSource) throws IOException {
      return new SourceEnum(attrSource, type(), this, maxDoc()) {
        @Override
        public int advance(int target) throws IOException {
          if (target >= numDocs) {
            return pos = NO_MORE_DOCS;
          }
          while (source.getBytes(target, bytesRef).length == 0) {
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
      assert ord >= 0;
      return deref(ord, bytesRef);
    }

    protected void closeIndexInput() throws IOException {
      try {
        if (datIn != null) {
          datIn.close();
        }
      } finally {
        if (idxIn != null) {// if straight
          idxIn.close();
        }
      }
    }

    /**
     * Returns the largest doc id + 1 in this doc values source
     */
    protected abstract int maxDoc();

    /**
     * Copies the value for the given ord to the given {@link BytesRef} and
     * returns it.
     */
    protected abstract BytesRef deref(int ord, BytesRef bytesRef);

    protected int binarySearch(BytesRef b, BytesRef bytesRef, int low,
        int high) {
      int mid = 0;
      while (low <= high) {
        mid = (low + high) >>> 1;
        deref(mid, bytesRef);
        final int cmp = comp.compare(bytesRef, b);
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          return mid;
        }
      }
      assert comp.compare(bytesRef, b) != 0;
      return -(low + 1);
    }

    @Override
    public ValuesEnum getEnum(AttributeSource attrSource) throws IOException {
      return new SourceEnum(attrSource, type(), this, maxDoc()) {

        @Override
        public int advance(int target) throws IOException {
          if (target >= numDocs) {
            return pos = NO_MORE_DOCS;
          }
          while (source.getBytes(target, bytesRef).length == 0) {
            if (++target >= numDocs) {
              return pos = NO_MORE_DOCS;
            }
          }
          return pos = target;
        }
      };
    }
  }

  // TODO: open up this API?!
  static abstract class BytesWriterBase extends Writer {
    private final String id;
    protected IndexOutput idxOut;
    protected IndexOutput datOut;
    protected BytesRef bytesRef;
    protected final ByteBlockPool pool;

    protected BytesWriterBase(Directory dir, String id, String codecName,
        int version, boolean initIndex, ByteBlockPool pool,
        AtomicLong bytesUsed) throws IOException {
      super(bytesUsed);
      this.id = id;
      this.pool = pool;
      datOut = dir.createOutput(IndexFileNames.segmentFileName(id, "",
            DATA_EXTENSION));
      boolean success = false;
      try {
        CodecUtil.writeHeader(datOut, codecName, version);
        if (initIndex) {
          idxOut = dir.createOutput(IndexFileNames.segmentFileName(id, "",
              INDEX_EXTENSION));
          CodecUtil.writeHeader(idxOut, codecName, version);
        } else {
          idxOut = null;
        }
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeSafely(true, datOut, idxOut);
        }
      }
    }

    /**
     * Must be called only with increasing docIDs. It's OK for some docIDs to be
     * skipped; they will be filled with 0 bytes.
     */
    @Override
    public abstract void add(int docID, BytesRef bytes) throws IOException;

    @Override
    public void finish(int docCount) throws IOException {
      try {
        IOUtils.closeSafely(false, datOut, idxOut);
      } finally {
        if (pool != null) {
          pool.reset();
        }
      }
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
  static abstract class BytesReaderBase extends IndexDocValues {
    protected final IndexInput idxIn;
    protected final IndexInput datIn;
    protected final int version;
    protected final String id;

    protected BytesReaderBase(Directory dir, String id, String codecName,
        int maxVersion, boolean doIndex) throws IOException {
      this.id = id;
      datIn = dir.openInput(IndexFileNames.segmentFileName(id, "",
          Writer.DATA_EXTENSION));
      boolean success = false;
      try {
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
      success = true;
      } finally {
        if (!success) {
          closeInternal();
        }
      }
    }

    /**
     * clones and returns the data {@link IndexInput}
     */
    protected final IndexInput cloneData() {
      assert datIn != null;
      return (IndexInput) datIn.clone();
    }

    /**
     * clones and returns the indexing {@link IndexInput}
     */
    protected final IndexInput cloneIndex() {
      assert idxIn != null;
      return (IndexInput) idxIn.clone();
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
         closeInternal();
      }
    }
    
    private void closeInternal() throws IOException {
      try {
        datIn.close();
      } finally {
        if (idxIn != null) {
          idxIn.close();
        }
      }
    }
  }

}