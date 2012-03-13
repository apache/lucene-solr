package org.apache.lucene.codecs.lucene40.values;

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
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.DocValues.SortedSource;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool.Allocator;
import org.apache.lucene.util.ByteBlockPool.DirectTrackingAllocator;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash.TrackingDirectBytesStartArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Provides concrete Writer/Reader implementations for <tt>byte[]</tt> value per
 * document. There are 6 package-private default implementations of this, for
 * all combinations of {@link Mode#DEREF}/{@link Mode#STRAIGHT} x fixed-length/variable-length.
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

  static final String DV_SEGMENT_SUFFIX = "dv";

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
   * @param fixedSize
   *          <code>true</code> if all bytes subsequently passed to the
   *          {@link Writer} will have the same length
   * @param sortComparator {@link BytesRef} comparator used by sorted variants. 
   *        If <code>null</code> {@link BytesRef#getUTF8SortedAsUnicodeComparator()}
   *        is used instead
   * @param bytesUsed
   *          an {@link AtomicLong} instance to track the used bytes within the
   *          {@link Writer}. A call to {@link Writer#finish(int)} will release
   *          all internally used resources and frees the memory tracking
   *          reference.
   * @param fasterButMoreRam whether packed ints for docvalues should be optimized for speed by rounding up the bytes
   *                         used for a value to either 8, 16, 32 or 64 bytes. This option is only applicable for
   *                         docvalues of type {@link Type#BYTES_FIXED_SORTED} and {@link Type#BYTES_VAR_SORTED}.
   * @param context I/O Context
   * @return a new {@link Writer} instance
   * @throws IOException
   *           if the files for the writer can not be created.
   */
  public static DocValuesConsumer getWriter(Directory dir, String id, Mode mode,
      boolean fixedSize, Comparator<BytesRef> sortComparator,
      Counter bytesUsed, IOContext context, boolean fasterButMoreRam)
      throws IOException {
    // TODO -- i shouldn't have to specify fixed? can
    // track itself & do the write thing at write time?
    if (sortComparator == null) {
      sortComparator = BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    if (fixedSize) {
      if (mode == Mode.STRAIGHT) {
        return new FixedStraightBytesImpl.Writer(dir, id, bytesUsed, context);
      } else if (mode == Mode.DEREF) {
        return new FixedDerefBytesImpl.Writer(dir, id, bytesUsed, context);
      } else if (mode == Mode.SORTED) {
        return new FixedSortedBytesImpl.Writer(dir, id, sortComparator, bytesUsed, context, fasterButMoreRam);
      }
    } else {
      if (mode == Mode.STRAIGHT) {
        return new VarStraightBytesImpl.Writer(dir, id, bytesUsed, context);
      } else if (mode == Mode.DEREF) {
        return new VarDerefBytesImpl.Writer(dir, id, bytesUsed, context);
      } else if (mode == Mode.SORTED) {
        return new VarSortedBytesImpl.Writer(dir, id, sortComparator, bytesUsed, context, fasterButMoreRam);
      }
    }

    throw new IllegalArgumentException("");
  }

  /**
   * Creates a new {@link DocValues} instance that provides either memory
   * resident or iterative access to a per-document stored <tt>byte[]</tt>
   * value. The returned {@link DocValues} instance will be initialized without
   * consuming a significant amount of memory.
   * 
   * @param dir
   *          the directory to load the {@link DocValues} from.
   * @param id
   *          the file ID in the {@link Directory} to load the values from.
   * @param mode
   *          the mode used to store the values
   * @param fixedSize
   *          <code>true</code> iff the values are stored with fixed-size,
   *          otherwise <code>false</code>
   * @param maxDoc
   *          the number of document values stored for the given ID
   * @param sortComparator {@link BytesRef} comparator used by sorted variants. 
   *        If <code>null</code> {@link BytesRef#getUTF8SortedAsUnicodeComparator()}
   *        is used instead
   * @return an initialized {@link DocValues} instance.
   * @throws IOException
   *           if an {@link IOException} occurs
   */
  public static DocValues getValues(Directory dir, String id, Mode mode,
      boolean fixedSize, int maxDoc, Comparator<BytesRef> sortComparator, IOContext context) throws IOException {
    if (sortComparator == null) {
      sortComparator = BytesRef.getUTF8SortedAsUnicodeComparator();
    }
    // TODO -- I can peek @ header to determing fixed/mode?
    if (fixedSize) {
      if (mode == Mode.STRAIGHT) {
        return new FixedStraightBytesImpl.FixedStraightReader(dir, id, maxDoc, context);
      } else if (mode == Mode.DEREF) {
        return new FixedDerefBytesImpl.FixedDerefReader(dir, id, maxDoc, context);
      } else if (mode == Mode.SORTED) {
        return new FixedSortedBytesImpl.Reader(dir, id, maxDoc, context, Type.BYTES_FIXED_SORTED, sortComparator);
      }
    } else {
      if (mode == Mode.STRAIGHT) {
        return new VarStraightBytesImpl.VarStraightReader(dir, id, maxDoc, context);
      } else if (mode == Mode.DEREF) {
        return new VarDerefBytesImpl.VarDerefReader(dir, id, maxDoc, context);
      } else if (mode == Mode.SORTED) {
        return new VarSortedBytesImpl.Reader(dir, id, maxDoc,context, Type.BYTES_VAR_SORTED, sortComparator);
      }
    }

    throw new IllegalArgumentException("Illegal Mode: " + mode);
  }

  // TODO open up this API?
  static abstract class BytesSourceBase extends Source {
    private final PagedBytes pagedBytes;
    protected final IndexInput datIn;
    protected final IndexInput idxIn;
    protected final static int PAGED_BYTES_BITS = 15;
    protected final PagedBytes.Reader data;
    protected final long totalLengthInBytes;
    

    protected BytesSourceBase(IndexInput datIn, IndexInput idxIn,
        PagedBytes pagedBytes, long bytesToRead, Type type) throws IOException {
      super(type);
      assert bytesToRead <= datIn.length() : " file size is less than the expected size diff: "
          + (bytesToRead - datIn.length()) + " pos: " + datIn.getFilePointer();
      this.datIn = datIn;
      this.totalLengthInBytes = bytesToRead;
      this.pagedBytes = pagedBytes;
      this.pagedBytes.copy(datIn, bytesToRead);
      data = pagedBytes.freeze(true);
      this.idxIn = idxIn;
    }
  }
  
  // TODO: open up this API?!
  static abstract class BytesWriterBase extends Writer {
    private final String id;
    private IndexOutput idxOut;
    private IndexOutput datOut;
    protected BytesRef bytesRef = new BytesRef();
    private final Directory dir;
    private final String codecName;
    private final int version;
    private final IOContext context;

    protected BytesWriterBase(Directory dir, String id, String codecName,
        int version, Counter bytesUsed, IOContext context, Type type) throws IOException {
      super(bytesUsed, type);
      this.id = id;
      this.dir = dir;
      this.codecName = codecName;
      this.version = version;
      this.context = context;
    }
    
    protected IndexOutput getOrCreateDataOut() throws IOException {
      if (datOut == null) {
        boolean success = false;
        try {
          datOut = dir.createOutput(IndexFileNames.segmentFileName(id, DV_SEGMENT_SUFFIX,
              DocValuesWriterBase.DATA_EXTENSION), context);
          CodecUtil.writeHeader(datOut, codecName, version);
          success = true;
        } finally {
          if (!success) {
            IOUtils.closeWhileHandlingException(datOut);
          }
        }
      }
      return datOut;
    }
    
    protected IndexOutput getIndexOut() {
      return idxOut;
    }
    
    protected IndexOutput getDataOut() {
      return datOut;
    }

    protected IndexOutput getOrCreateIndexOut() throws IOException {
      boolean success = false;
      try {
        if (idxOut == null) {
          idxOut = dir.createOutput(IndexFileNames.segmentFileName(id, DV_SEGMENT_SUFFIX,
              DocValuesWriterBase.INDEX_EXTENSION), context);
          CodecUtil.writeHeader(idxOut, codecName, version);
        }
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(idxOut);
        }
      }
      return idxOut;
    }


    @Override
    public abstract void finish(int docCount) throws IOException;

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
    protected final Type type;

    protected BytesReaderBase(Directory dir, String id, String codecName,
        int maxVersion, boolean doIndex, IOContext context, Type type) throws IOException {
      IndexInput dataIn = null;
      IndexInput indexIn = null;
      boolean success = false;
      try {
        dataIn = dir.openInput(IndexFileNames.segmentFileName(id, DV_SEGMENT_SUFFIX,
                                                              DocValuesWriterBase.DATA_EXTENSION), context);
        version = CodecUtil.checkHeader(dataIn, codecName, maxVersion, maxVersion);
        if (doIndex) {
          indexIn = dir.openInput(IndexFileNames.segmentFileName(id, DV_SEGMENT_SUFFIX,
                                                                 DocValuesWriterBase.INDEX_EXTENSION), context);
          final int version2 = CodecUtil.checkHeader(indexIn, codecName,
                                                     maxVersion, maxVersion);
          assert version == version2;
        }
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(dataIn, indexIn);
        }
      }
      datIn = dataIn;
      idxIn = indexIn;
      this.type = type;
      this.id = id;
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
         IOUtils.close(datIn, idxIn);
      }
    }

    @Override
    public Type getType() {
      return type;
    }
    
  }
  
  static abstract class DerefBytesWriterBase extends BytesWriterBase {
    protected int size = -1;
    protected int lastDocId = -1;
    protected int[] docToEntry;
    protected final BytesRefHash hash;
    protected final boolean fasterButMoreRam;
    protected long maxBytes = 0;
    
    protected DerefBytesWriterBase(Directory dir, String id, String codecName,
        int codecVersion, Counter bytesUsed, IOContext context, Type type)
        throws IOException {
      this(dir, id, codecName, codecVersion, new DirectTrackingAllocator(
          ByteBlockPool.BYTE_BLOCK_SIZE, bytesUsed), bytesUsed, context, false, type);
    }

    protected DerefBytesWriterBase(Directory dir, String id, String codecName,
                                   int codecVersion, Counter bytesUsed, IOContext context, boolean fasterButMoreRam, Type type)
        throws IOException {
      this(dir, id, codecName, codecVersion, new DirectTrackingAllocator(
          ByteBlockPool.BYTE_BLOCK_SIZE, bytesUsed), bytesUsed, context, fasterButMoreRam,type);
    }

    protected DerefBytesWriterBase(Directory dir, String id, String codecName, int codecVersion, Allocator allocator,
        Counter bytesUsed, IOContext context, boolean fasterButMoreRam, Type type) throws IOException {
      super(dir, id, codecName, codecVersion, bytesUsed, context, type);
      hash = new BytesRefHash(new ByteBlockPool(allocator),
          BytesRefHash.DEFAULT_CAPACITY, new TrackingDirectBytesStartArray(
              BytesRefHash.DEFAULT_CAPACITY, bytesUsed));
      docToEntry = new int[1];
      bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_INT);
      this.fasterButMoreRam = fasterButMoreRam;
    }
    
    protected static int writePrefixLength(DataOutput datOut, BytesRef bytes)
        throws IOException {
      if (bytes.length < 128) {
        datOut.writeByte((byte) bytes.length);
        return 1;
      } else {
        datOut.writeByte((byte) (0x80 | (bytes.length >> 8)));
        datOut.writeByte((byte) (bytes.length & 0xff));
        return 2;
      }
    }

    @Override
    public void add(int docID, IndexableField value) throws IOException {
      BytesRef bytes = value.binaryValue();
      assert bytes != null;
      if (bytes.length == 0) { // default value - skip it
        return;
      }
      checkSize(bytes);
      fillDefault(docID);
      int ord = hash.add(bytes);
      if (ord < 0) {
        ord = (-ord) - 1;
      } else {
        maxBytes += bytes.length;
      }
      
      
      docToEntry[docID] = ord;
      lastDocId = docID;
    }
    
    protected void fillDefault(int docID) {
      if (docID >= docToEntry.length) {
        final int size = docToEntry.length;
        docToEntry = ArrayUtil.grow(docToEntry, 1 + docID);
        bytesUsed.addAndGet((docToEntry.length - size)
            * RamUsageEstimator.NUM_BYTES_INT);
      }
      assert size >= 0;
      BytesRef ref = new BytesRef(size);
      ref.length = size;
      int ord = hash.add(ref);
      if (ord < 0) {
        ord = (-ord) - 1;
      }
      for (int i = lastDocId+1; i < docID; i++) {
        docToEntry[i] = ord;
      }
    }
    
    protected void checkSize(BytesRef bytes) {
      if (size == -1) {
        size = bytes.length;
      } else if (bytes.length != size) {
        throw new IllegalArgumentException("expected bytes size=" + size
            + " but got " + bytes.length);
      }
    }
    
    // Important that we get docCount, in case there were
    // some last docs that we didn't see
    @Override
    public void finish(int docCount) throws IOException {
      boolean success = false;
      try {
        finishInternal(docCount);
        success = true;
      } finally {
        releaseResources();
        if (success) {
          IOUtils.close(getIndexOut(), getDataOut());
        } else {
          IOUtils.closeWhileHandlingException(getIndexOut(), getDataOut());
        }
        
      }
    }
    
    protected abstract void finishInternal(int docCount) throws IOException;
    
    protected void releaseResources() {
      hash.close();
      bytesUsed.addAndGet((-docToEntry.length) * RamUsageEstimator.NUM_BYTES_INT);
      docToEntry = null;
    }
    
    protected void writeIndex(IndexOutput idxOut, int docCount,
        long maxValue, int[] toEntry) throws IOException {
      writeIndex(idxOut, docCount, maxValue, (int[])null, toEntry);
    }
    
    protected void writeIndex(IndexOutput idxOut, int docCount,
        long maxValue, int[] addresses, int[] toEntry) throws IOException {
      final PackedInts.Writer w = PackedInts.getWriter(idxOut, docCount,
          bitsRequired(maxValue));
      final int limit = docCount > docToEntry.length ? docToEntry.length
          : docCount;
      assert toEntry.length >= limit -1;
      if (addresses != null) {
        for (int i = 0; i < limit; i++) {
          assert addresses[toEntry[i]] >= 0;
          w.add(addresses[toEntry[i]]);
        }
      } else {
        for (int i = 0; i < limit; i++) {
          assert toEntry[i] >= 0;
          w.add(toEntry[i]);
        }
      }
      for (int i = limit; i < docCount; i++) {
        w.add(0);
      }
      w.finish();
    }
    
    protected void writeIndex(IndexOutput idxOut, int docCount,
        long maxValue, long[] addresses, int[] toEntry) throws IOException {
      final PackedInts.Writer w = PackedInts.getWriter(idxOut, docCount,
          bitsRequired(maxValue));
      final int limit = docCount > docToEntry.length ? docToEntry.length
          : docCount;
      assert toEntry.length >= limit -1;
      if (addresses != null) {
        for (int i = 0; i < limit; i++) {
          assert addresses[toEntry[i]] >= 0;
          w.add(addresses[toEntry[i]]);
        }
      } else {
        for (int i = 0; i < limit; i++) {
          assert toEntry[i] >= 0;
          w.add(toEntry[i]);
        }
      }
      for (int i = limit; i < docCount; i++) {
        w.add(0);
      }
      w.finish();
    }

    protected int bitsRequired(long maxValue){
      return fasterButMoreRam ?
          PackedInts.getNextFixedSize(PackedInts.bitsRequired(maxValue)) : PackedInts.bitsRequired(maxValue);
    }
    
  }
  
  static abstract class BytesSortedSourceBase extends SortedSource {
    private final PagedBytes pagedBytes;
    
    protected final PackedInts.Reader docToOrdIndex;
    protected final PackedInts.Reader ordToOffsetIndex;

    protected final IndexInput datIn;
    protected final IndexInput idxIn;
    protected final BytesRef defaultValue = new BytesRef();
    protected final static int PAGED_BYTES_BITS = 15;
    protected final PagedBytes.Reader data;

    protected BytesSortedSourceBase(IndexInput datIn, IndexInput idxIn,
        Comparator<BytesRef> comp, long bytesToRead, Type type, boolean hasOffsets) throws IOException {
      this(datIn, idxIn, comp, new PagedBytes(PAGED_BYTES_BITS), bytesToRead, type, hasOffsets);
    }
    
    protected BytesSortedSourceBase(IndexInput datIn, IndexInput idxIn,
        Comparator<BytesRef> comp, PagedBytes pagedBytes, long bytesToRead, Type type, boolean hasOffsets)
        throws IOException {
      super(type, comp);
      assert bytesToRead <= datIn.length() : " file size is less than the expected size diff: "
          + (bytesToRead - datIn.length()) + " pos: " + datIn.getFilePointer();
      this.datIn = datIn;
      this.pagedBytes = pagedBytes;
      this.pagedBytes.copy(datIn, bytesToRead);
      data = pagedBytes.freeze(true);
      this.idxIn = idxIn;
      ordToOffsetIndex = hasOffsets ? PackedInts.getReader(idxIn) : null; 
      docToOrdIndex = PackedInts.getReader(idxIn);
    }

    @Override
    public boolean hasPackedDocToOrd() {
      return true;
    }

    @Override
    public PackedInts.Reader getDocToOrd() {
      return docToOrdIndex;
    }
    
    @Override
    public int ord(int docID) {
      assert docToOrdIndex.get(docID) < getValueCount();
      return (int) docToOrdIndex.get(docID);
    }

    protected void closeIndexInput() throws IOException {
      IOUtils.close(datIn, idxIn);
    }
  }
}
