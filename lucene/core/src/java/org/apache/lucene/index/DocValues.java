package org.apache.lucene.index;

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

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.DocValuesField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;

/**
 * {@link DocValues} provides a dense per-document typed storage for fast
 * value access based on the lucene internal document id. {@link DocValues}
 * exposes two distinct APIs:
 * <ul>
 * <li>via {@link #getSource()} providing RAM resident random access</li>
 * <li>via {@link #getDirectSource()} providing on disk random access</li>
 * </ul> {@link DocValues} are exposed via
 * {@link AtomicReader#docValues(String)} on a per-segment basis. For best
 * performance {@link DocValues} should be consumed per-segment just like
 * IndexReader.
 * <p>
 * {@link DocValues} are fully integrated into the {@link DocValuesFormat} API.
 * 
 * @see Type for limitations and default implementation documentation
 * @see DocValuesField for adding values to the index
 * @see DocValuesFormat#docsConsumer(org.apache.lucene.index.PerDocWriteState) for
 *      customization
 * @lucene.experimental
 */
public abstract class DocValues implements Closeable {

  public static final DocValues[] EMPTY_ARRAY = new DocValues[0];

  private volatile SourceCache cache = new SourceCache.DirectSourceCache();
  private final Object cacheLock = new Object();
  
  /**
   * Loads a new {@link Source} instance for this {@link DocValues} field
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
   * from the cache once this {@link DocValues} instance is closed by the
   * {@link IndexReader}, {@link Fields} or {@link FieldsEnum} the
   * {@link DocValues} was created from.
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
   * Returns the {@link Type} of this {@link DocValues} instance
   */
  public abstract Type getType();

  /**
   * Closes this {@link DocValues} instance. This method should only be called
   * by the creator of this {@link DocValues} instance. API users should not
   * close {@link DocValues} instances.
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
   * Sets the {@link SourceCache} used by this {@link DocValues} instance. This
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
   * depending on the {@link DocValues} fields {@link Type}. Source
   * implementations provide random access semantics similar to array lookups
   * <p>
   * @see DocValues#getSource()
   * @see DocValues#getDirectSource()
   */
  public static abstract class Source {
    
    protected final Type type;

    protected Source(Type type) {
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
     * Returns the {@link Type} of this source.
     * 
     * @return the {@link Type} of this source.
     */
    public Type getType() {
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

    protected SortedSource(Type type, Comparator<BytesRef> comparator) {
      super(type);
      this.comparator = comparator;
    }

    @Override
    public BytesRef getBytes(int docID, BytesRef bytesRef) {
      final int ord = ord(docID);
      if (ord < 0) {
        // Negative ord means doc was missing?
        bytesRef.length = 0;
      } else {
        getByOrd(ord, bytesRef);
      }
      return bytesRef;
    }

    /**
     * Returns ord for specified docID. Ord is dense, ie, starts at 0, then increments by 1
     * for the next (as defined by {@link Comparator} value.
     */
    public abstract int ord(int docID);

    /** Returns value for specified ord. */
    public abstract BytesRef getByOrd(int ord, BytesRef result);

    /** Return true if it's safe to call {@link
     *  #getDocToOrd}. */
    public boolean hasPackedDocToOrd() {
      return false;
    }

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
     * Lookup ord by value.
     * 
     * @param value
     *          the value to look up
     * @param spare
     *          a spare {@link BytesRef} instance used to compare internal
     *          values to the given value. Must not be <code>null</code>
     * @return the given values ordinal if found or otherwise
     *         <code>(-(ord)-1)</code>, defined as the ordinal of the first
     *         element that is greater than the given value (the insertion
     *         point). This guarantees that the return value will always be
     *         &gt;= 0 if the given value is found.
     */
    public int getOrdByValue(BytesRef value, BytesRef spare) {
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
  public static Source getDefaultSource(final Type type) {
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
  public static SortedSource getDefaultSortedSource(final Type type, final int size) {

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
      public boolean hasPackedDocToOrd() {
        return true;
      }

      @Override
      public PackedInts.Reader getDocToOrd() {
        return docToOrd;
      }

      @Override
      public int getOrdByValue(BytesRef value, BytesRef spare) {
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
  
  /**
   * <code>Type</code> specifies the {@link DocValues} type for a
   * certain field. A <code>Type</code> only defines the data type for a field
   * while the actual implementation used to encode and decode the values depends
   * on the the {@link DocValuesFormat#docsConsumer} and {@link DocValuesFormat#docsProducer} methods.
   * 
   * @lucene.experimental
   */
  public static enum Type {

    /**
     * A variable bit signed integer value. By default this type uses
     * {@link PackedInts} to compress the values, as an offset
     * from the minimum value, as long as the value range
     * fits into 2<sup>63</sup>-1. Otherwise,
     * the default implementation falls back to fixed size 64bit
     * integers ({@link #FIXED_INTS_64}).
     * <p>
     * NOTE: this type uses <tt>0</tt> as the default value without any
     * distinction between provided <tt>0</tt> values during indexing. All
     * documents without an explicit value will use <tt>0</tt> instead.
     * Custom default values must be assigned explicitly.
     * </p>
     */
    VAR_INTS,
    
    /**
     * A 8 bit signed integer value. {@link Source} instances of
     * this type return a <tt>byte</tt> array from {@link Source#getArray()}
     * <p>
     * NOTE: this type uses <tt>0</tt> as the default value without any
     * distinction between provided <tt>0</tt> values during indexing. All
     * documents without an explicit value will use <tt>0</tt> instead.
     * Custom default values must be assigned explicitly.
     * </p>
     */
    FIXED_INTS_8,
    
    /**
     * A 16 bit signed integer value. {@link Source} instances of
     * this type return a <tt>short</tt> array from {@link Source#getArray()}
     * <p>
     * NOTE: this type uses <tt>0</tt> as the default value without any
     * distinction between provided <tt>0</tt> values during indexing. All
     * documents without an explicit value will use <tt>0</tt> instead.
     * Custom default values must be assigned explicitly.
     * </p>
     */
    FIXED_INTS_16,
    
    /**
     * A 32 bit signed integer value. {@link Source} instances of
     * this type return a <tt>int</tt> array from {@link Source#getArray()}
     * <p>
     * NOTE: this type uses <tt>0</tt> as the default value without any
     * distinction between provided <tt>0</tt> values during indexing. All
     * documents without an explicit value will use <tt>0</tt> instead. 
     * Custom default values must be assigned explicitly.
     * </p>
     */
    FIXED_INTS_32,

    /**
     * A 64 bit signed integer value. {@link Source} instances of
     * this type return a <tt>long</tt> array from {@link Source#getArray()}
     * <p>
     * NOTE: this type uses <tt>0</tt> as the default value without any
     * distinction between provided <tt>0</tt> values during indexing. All
     * documents without an explicit value will use <tt>0</tt> instead.
     * Custom default values must be assigned explicitly.
     * </p>
     */
    FIXED_INTS_64,

    /**
     * A 32 bit floating point value. By default there is no compression
     * applied. To fit custom float values into less than 32bit either a custom
     * implementation is needed or values must be encoded into a
     * {@link #BYTES_FIXED_STRAIGHT} type. {@link Source} instances of
     * this type return a <tt>float</tt> array from {@link Source#getArray()}
     * <p>
     * NOTE: this type uses <tt>0.0f</tt> as the default value without any
     * distinction between provided <tt>0.0f</tt> values during indexing. All
     * documents without an explicit value will use <tt>0.0f</tt> instead.
     * Custom default values must be assigned explicitly.
     * </p>
     */
    FLOAT_32,

    /**
     * 
     * A 64 bit floating point value. By default there is no compression
     * applied. To fit custom float values into less than 64bit either a custom
     * implementation is needed or values must be encoded into a
     * {@link #BYTES_FIXED_STRAIGHT} type. {@link Source} instances of
     * this type return a <tt>double</tt> array from {@link Source#getArray()}
     * <p>
     * NOTE: this type uses <tt>0.0d</tt> as the default value without any
     * distinction between provided <tt>0.0d</tt> values during indexing. All
     * documents without an explicit value will use <tt>0.0d</tt> instead.
     * Custom default values must be assigned explicitly.
     * </p>
     */
    FLOAT_64,

    // TODO(simonw): -- shouldn't lucene decide/detect straight vs
    // deref, as well fixed vs var?
    /**
     * A fixed length straight byte[]. All values added to
     * such a field must be of the same length. All bytes are stored sequentially
     * for fast offset access.
     * <p>
     * NOTE: this type uses <tt>0 byte</tt> filled byte[] based on the length of the first seen
     * value as the default value without any distinction between explicitly
     * provided values during indexing. All documents without an explicit value
     * will use the default instead.Custom default values must be assigned explicitly.
     * </p>
     */
    BYTES_FIXED_STRAIGHT,

    /**
     * A fixed length dereferenced byte[] variant. Fields with
     * this type only store distinct byte values and store an additional offset
     * pointer per document to dereference the shared byte[].
     * Use this type if your documents may share the same byte[].
     * <p>
     * NOTE: Fields of this type will not store values for documents without and
     * explicitly provided value. If a documents value is accessed while no
     * explicit value is stored the returned {@link BytesRef} will be a 0-length
     * reference. Custom default values must be assigned explicitly.
     * </p>
     */
    BYTES_FIXED_DEREF,

    /**
     * Variable length straight stored byte[] variant. All bytes are
     * stored sequentially for compactness. Usage of this type via the
     * disk-resident API might yield performance degradation since no additional
     * index is used to advance by more than one document value at a time.
     * <p>
     * NOTE: Fields of this type will not store values for documents without an
     * explicitly provided value. If a documents value is accessed while no
     * explicit value is stored the returned {@link BytesRef} will be a 0-length
     * byte[] reference. Custom default values must be assigned explicitly.
     * </p>
     */
    BYTES_VAR_STRAIGHT,

    /**
     * A variable length dereferenced byte[]. Just like
     * {@link #BYTES_FIXED_DEREF}, but allowing each
     * document's value to be a different length.
     * <p>
     * NOTE: Fields of this type will not store values for documents without and
     * explicitly provided value. If a documents value is accessed while no
     * explicit value is stored the returned {@link BytesRef} will be a 0-length
     * reference. Custom default values must be assigned explicitly.
     * </p>
     */
    BYTES_VAR_DEREF,


    /**
     * A variable length pre-sorted byte[] variant. Just like
     * {@link #BYTES_FIXED_SORTED}, but allowing each
     * document's value to be a different length.
     * <p>
     * NOTE: Fields of this type will not store values for documents without and
     * explicitly provided value. If a documents value is accessed while no
     * explicit value is stored the returned {@link BytesRef} will be a 0-length
     * reference.Custom default values must be assigned explicitly.
     * </p>
     * 
     * @see SortedSource
     */
    BYTES_VAR_SORTED,
    
    /**
     * A fixed length pre-sorted byte[] variant. Fields with this type only
     * store distinct byte values and store an additional offset pointer per
     * document to dereference the shared byte[]. The stored
     * byte[] is presorted, by default by unsigned byte order,
     * and allows access via document id, ordinal and by-value.
     * Use this type if your documents may share the same byte[].
     * <p>
     * NOTE: Fields of this type will not store values for documents without and
     * explicitly provided value. If a documents value is accessed while no
     * explicit value is stored the returned {@link BytesRef} will be a 0-length
     * reference. Custom default values must be assigned
     * explicitly.
     * </p>
     * 
     * @see SortedSource
     */
    BYTES_FIXED_SORTED
  }
  
  /**
   * Abstract base class for {@link DocValues} {@link Source} cache.
   * <p>
   * {@link Source} instances loaded via {@link DocValues#load()} are entirely memory resident
   * and need to be maintained by the caller. Each call to
   * {@link DocValues#load()} will cause an entire reload of
   * the underlying data. Source instances obtained from
   * {@link DocValues#getSource()} and {@link DocValues#getSource()}
   * respectively are maintained by a {@link SourceCache} that is closed (
   * {@link #close(DocValues)}) once the {@link IndexReader} that created the
   * {@link DocValues} instance is closed.
   * <p>
   * Unless {@link Source} instances are managed by another entity it is
   * recommended to use the cached variants to obtain a source instance.
   * <p>
   * Implementation of this API must be thread-safe.
   * 
   * @see DocValues#setCache(SourceCache)
   * @see DocValues#getSource()
   * 
   * @lucene.experimental
   */
  public static abstract class SourceCache {

    /**
     * Atomically loads a {@link Source} into the cache from the given
     * {@link DocValues} and returns it iff no other {@link Source} has already
     * been cached. Otherwise the cached source is returned.
     * <p>
     * This method will not return <code>null</code>
     */
    public abstract Source load(DocValues values) throws IOException;

    /**
     * Atomically invalidates the cached {@link Source} 
     * instances if any and empties the cache.
     */
    public abstract void invalidate(DocValues values);

    /**
     * Atomically closes the cache and frees all resources.
     */
    public synchronized void close(DocValues values) {
      invalidate(values);
    }

    /**
     * Simple per {@link DocValues} instance cache implementation that holds a
     * {@link Source} a member variable.
     * <p>
     * If a {@link DirectSourceCache} instance is closed or invalidated the cached
     * reference are simply set to <code>null</code>
     */
    public static final class DirectSourceCache extends SourceCache {
      private Source ref;

      public synchronized Source load(DocValues values) throws IOException {
        if (ref == null) {
          ref = values.load();
        }
        return ref;
      }

      public synchronized void invalidate(DocValues values) {
        ref = null;
      }
    }
  }
}
