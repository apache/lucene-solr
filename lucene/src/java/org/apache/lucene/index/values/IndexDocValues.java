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
import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.document.IndexDocValuesField;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.codecs.DocValuesFormat;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;

/**
 * {@link IndexDocValues} provides a dense per-document typed storage for fast
 * value access based on the lucene internal document id. {@link IndexDocValues}
 * exposes two distinct APIs:
 * <ul>
 * <li>via {@link #getSource()} providing RAM resident random access</li>
 * <li>via {@link #getDirectSource()} providing on disk random access</li>
 * </ul> {@link IndexDocValues} are exposed via
 * {@link IndexReader#perDocValues()} on a per-segment basis. For best
 * performance {@link IndexDocValues} should be consumed per-segment just like
 * IndexReader.
 * <p>
 * {@link IndexDocValues} are fully integrated into the {@link DocValuesFormat} API.
 * 
 * @see ValueType for limitations and default implementation documentation
 * @see IndexDocValuesField for adding values to the index
 * @see DocValuesFormat#docsConsumer(org.apache.lucene.index.PerDocWriteState) for
 *      customization
 * @lucene.experimental
 */
public abstract class IndexDocValues implements Closeable {

  public static final IndexDocValues[] EMPTY_ARRAY = new IndexDocValues[0];

  private volatile SourceCache cache = new SourceCache.DirectSourceCache();
  private final Object cacheLock = new Object();
  
  /**
   * Loads a new {@link Source} instance for this {@link IndexDocValues} field
   * instance. Source instances returned from this method are not cached. It is
   * the callers responsibility to maintain the instance and release its
   * resources once the source is not needed anymore.
   * <p>
   * For managed {@link Source} instances see {@link #getSource()}.
   * 
   * @see #getSource()
   * @see #setCache(SourceCache)
   */
  public abstract Source load() throws IOException;

  /**
   * Returns a {@link Source} instance through the current {@link SourceCache}.
   * Iff no {@link Source} has been loaded into the cache so far the source will
   * be loaded through {@link #load()} and passed to the {@link SourceCache}.
   * The caller of this method should not close the obtained {@link Source}
   * instance unless it is not needed for the rest of its life time.
   * <p>
   * {@link Source} instances obtained from this method are closed / released
   * from the cache once this {@link IndexDocValues} instance is closed by the
   * {@link IndexReader}, {@link Fields} or {@link FieldsEnum} the
   * {@link IndexDocValues} was created from.
   */
  public Source getSource() throws IOException {
    return cache.load(this);
  }

  /**
   * Returns a disk resident {@link Source} instance. Direct Sources are not
   * cached in the {@link SourceCache} and should not be shared between threads.
   */
  public abstract Source getDirectSource() throws IOException;

  /**
   * Returns the {@link ValueType} of this {@link IndexDocValues} instance
   */
  public abstract ValueType type();

  /**
   * Closes this {@link IndexDocValues} instance. This method should only be called
   * by the creator of this {@link IndexDocValues} instance. API users should not
   * close {@link IndexDocValues} instances.
   */
  public void close() throws IOException {
    cache.close(this);
  }

  /**
   * Returns the size per value in bytes or <code>-1</code> iff size per value
   * is variable.
   * 
   * @return the size per value in bytes or <code>-1</code> iff size per value
   * is variable.
   */
  public int getValueSize() {
    return -1;
  }

  /**
   * Sets the {@link SourceCache} used by this {@link IndexDocValues} instance. This
   * method should be called before {@link #load()} is called. All {@link Source} instances in the currently used cache will be closed
   * before the new cache is installed.
   * <p>
   * Note: All instances previously obtained from {@link #load()} will be lost.
   * 
   * @throws IllegalArgumentException
   *           if the given cache is <code>null</code>
   * 
   */
  public void setCache(SourceCache cache) {
    if (cache == null)
      throw new IllegalArgumentException("cache must not be null");
    synchronized (cacheLock) {
      SourceCache toClose = this.cache;
      this.cache = cache;
      toClose.close(this);
    }
  }

  /**
   * Source of per document values like long, double or {@link BytesRef}
   * depending on the {@link IndexDocValues} fields {@link ValueType}. Source
   * implementations provide random access semantics similar to array lookups
   * <p>
   * @see IndexDocValues#getSource()
   * @see IndexDocValues#getDirectSource()
   */
  public static abstract class Source {
    
    protected final ValueType type;

    protected Source(ValueType type) {
      this.type = type;
    }
    /**
     * Returns a <tt>long</tt> for the given document id or throws an
     * {@link UnsupportedOperationException} if this source doesn't support
     * <tt>long</tt> values.
     * 
     * @throws UnsupportedOperationException
     *           if this source doesn't support <tt>long</tt> values.
     */
    public long getInt(int docID) {
      throw new UnsupportedOperationException("ints are not supported");
    }

    /**
     * Returns a <tt>double</tt> for the given document id or throws an
     * {@link UnsupportedOperationException} if this source doesn't support
     * <tt>double</tt> values.
     * 
     * @throws UnsupportedOperationException
     *           if this source doesn't support <tt>double</tt> values.
     */
    public double getFloat(int docID) {
      throw new UnsupportedOperationException("floats are not supported");
    }

    /**
     * Returns a {@link BytesRef} for the given document id or throws an
     * {@link UnsupportedOperationException} if this source doesn't support
     * <tt>byte[]</tt> values.
     * @throws IOException 
     * 
     * @throws UnsupportedOperationException
     *           if this source doesn't support <tt>byte[]</tt> values.
     */
    public BytesRef getBytes(int docID, BytesRef ref) {
      throw new UnsupportedOperationException("bytes are not supported");
    }

    /**
     * Returns the {@link ValueType} of this source.
     * 
     * @return the {@link ValueType} of this source.
     */
    public ValueType type() {
      return type;
    }

    /**
     * Returns <code>true</code> iff this {@link Source} exposes an array via
     * {@link #getArray()} otherwise <code>false</code>.
     * 
     * @return <code>true</code> iff this {@link Source} exposes an array via
     *         {@link #getArray()} otherwise <code>false</code>.
     */
    public boolean hasArray() {
      return false;
    }

    /**
     * Returns the internal array representation iff this {@link Source} uses an
     * array as its inner representation, otherwise <code>UOE</code>.
     */
    public Object getArray() {
      throw new UnsupportedOperationException("getArray is not supported");
    }
    
    /**
     * If this {@link Source} is sorted this method will return an instance of
     * {@link SortedSource} otherwise <code>UOE</code>
     */
    public SortedSource asSortedSource() {
      throw new UnsupportedOperationException("asSortedSource is not supported");
    }
  }

  /**
   * A sorted variant of {@link Source} for <tt>byte[]</tt> values per document.
   * <p>
   */
  public static abstract class SortedSource extends Source {

    private final Comparator<BytesRef> comparator;

    protected SortedSource(ValueType type, Comparator<BytesRef> comparator) {
      super(type);
      this.comparator = comparator;
    }

    @Override
    public BytesRef getBytes(int docID, BytesRef bytesRef) {
      final int ord = ord(docID);
      if (ord < 0) {
        bytesRef.length = 0;
      } else {
        getByOrd(ord , bytesRef);
      }
      return bytesRef;
    }

    /**
     * Returns ord for specified docID. Ord is dense, ie, starts at 0, then increments by 1
     * for the next (as defined by {@link Comparator} value.
     */
    public abstract int ord(int docID);

    /** Returns value for specified ord. */
    public abstract BytesRef getByOrd(int ord, BytesRef bytesRef);

    /**
     * Returns the PackedInts.Reader impl that maps document to ord.
     */
    public abstract PackedInts.Reader getDocToOrd();
    
    /**
     * Returns the comparator used to order the BytesRefs.
     */
    public Comparator<BytesRef> getComparator() {
      return comparator;
    }

    /**
     * Performs a lookup by value.
     * 
     * @param value
     *          the value to look up
     * @param spare
     *          a spare {@link BytesRef} instance used to compare internal
     *          values to the given value. Must not be <code>null</code>
     * @return the given values ordinal if found or otherwise
     *         <code>(-(ord)-1)</code>, defined as the ordinal of the first
     *         element that is greater than the given value. This guarantees
     *         that the return value will always be &gt;= 0 if the given value
     *         is found.
     */
    public int getByValue(BytesRef value, BytesRef spare) {
      return binarySearch(value, spare, 0, getValueCount() - 1);
    }    

    private int binarySearch(BytesRef b, BytesRef bytesRef, int low,
        int high) {
      int mid = 0;
      while (low <= high) {
        mid = (low + high) >>> 1;
        getByOrd(mid, bytesRef);
        final int cmp = comparator.compare(bytesRef, b);
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          return mid;
        }
      }
      assert comparator.compare(bytesRef, b) != 0;
      return -(low + 1);
    }
    
    @Override
    public SortedSource asSortedSource() {
      return this;
    }
    
    /**
     * Returns the number of unique values in this sorted source
     */
    public abstract int getValueCount();
  }

  /** Returns a Source that always returns default (missing)
   *  values for all documents. */
  public static Source getDefaultSource(final ValueType type) {
    return new Source(type) {
      @Override
      public long getInt(int docID) {
        return 0;
      }

      @Override
      public double getFloat(int docID) {
        return 0.0;
      }

      @Override
      public BytesRef getBytes(int docID, BytesRef ref) {
        ref.length = 0;
        return ref;
      }
    };
  }

  /** Returns a SortedSource that always returns default (missing)
   *  values for all documents. */
  public static SortedSource getDefaultSortedSource(final ValueType type, final int size) {

    final PackedInts.Reader docToOrd = new PackedInts.Reader() {
      @Override
      public long get(int index) {
        return 0;
      }

      @Override
      public int getBitsPerValue() {
        return 0;
      }

      @Override
      public int size() {
        return size;
      }

      @Override
      public boolean hasArray() {
        return false;
      }

      @Override
      public Object getArray() {
        return null;
      }
    };

    return new SortedSource(type, BytesRef.getUTF8SortedAsUnicodeComparator()) {

      @Override
      public BytesRef getBytes(int docID, BytesRef ref) {
        ref.length = 0;
        return ref;
      }

      @Override
      public int ord(int docID) {
        return 0;
      }

      @Override
      public BytesRef getByOrd(int ord, BytesRef bytesRef) {
        assert ord == 0;
        bytesRef.length = 0;
        return bytesRef;
      }

      @Override
      public PackedInts.Reader getDocToOrd() {
        return docToOrd;
      }

      @Override
      public int getByValue(BytesRef value, BytesRef spare) {
        if (value.length == 0) {
          return 0;
        } else {
          return -1;
        }
      }

      @Override
        public int getValueCount() {
        return 1;
      }
    };
  }
}
