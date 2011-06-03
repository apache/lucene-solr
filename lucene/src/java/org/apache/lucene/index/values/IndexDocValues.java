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

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

/**
 * nocommit - javadoc 
 * 
 * @lucene.experimental
 */
public abstract class IndexDocValues implements Closeable {
  /*
   * TODO: it might be useful to add another Random Access enum for some
   * implementations like packed ints and only return such a random access enum
   * if the impl supports random access. For super large segments it might be
   * useful or even required in certain environements to have disc based random
   * access
   */
  public static final IndexDocValues[] EMPTY_ARRAY = new IndexDocValues[0];

  private SourceCache cache = new SourceCache.DirectSourceCache();

  /**
   * Returns an iterator that steps through all documents values for this
   * {@link IndexDocValues} field instance. {@link DocValuesEnum} will skip document
   * without a value if applicable.
   */
  public DocValuesEnum getEnum() throws IOException {
    return getEnum(null);
  }

  /**
   * Returns an iterator that steps through all documents values for this
   * {@link IndexDocValues} field instance. {@link DocValuesEnum} will skip document
   * without a value if applicable.
   * <p>
   * If an {@link AttributeSource} is supplied to this method the
   * {@link DocValuesEnum} will use the given source to access implementation
   * related attributes.
   */
  public abstract DocValuesEnum getEnum(AttributeSource attrSource)
      throws IOException;

  /**
   * Loads a new {@link Source} instance for this {@link IndexDocValues} field
   * instance. Source instances returned from this method are not cached. It is
   * the callers responsibility to maintain the instance and release its
   * resources once the source is not needed anymore.
   * <p>
   * This method will return null iff this {@link IndexDocValues} represent a
   * {@link SortedSource}.
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
   * <p>
   * This method will return null iff this {@link IndexDocValues} represent a
   * {@link SortedSource}.
   */
  public Source getSource() throws IOException {
    return cache.load(this);
  }

  /**
   * Returns a {@link SortedSource} instance for this {@link IndexDocValues} field
   * instance like {@link #getSource()}.
   * <p>
   * This method will return null iff this {@link IndexDocValues} represent a
   * {@link Source} instead of a {@link SortedSource}.
   */
  public SortedSource getSortedSorted(Comparator<BytesRef> comparator)
      throws IOException {
    return cache.loadSorted(this, comparator);
  }

  /**
   * Loads and returns a {@link SortedSource} instance for this
   * {@link IndexDocValues} field instance like {@link #load()}.
   * <p>
   * This method will return null iff this {@link IndexDocValues} represent a
   * {@link Source} instead of a {@link SortedSource}.
   */
  public SortedSource loadSorted(Comparator<BytesRef> comparator)
      throws IOException {
    throw new UnsupportedOperationException();
  }

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
   * Sets the {@link SourceCache} used by this {@link IndexDocValues} instance. This
   * method should be called before {@link #load()} or
   * {@link #loadSorted(Comparator)} is called. All {@link Source} or
   * {@link SortedSource} instances in the currently used cache will be closed
   * before the new cache is installed.
   * <p>
   * Note: All instances previously obtained from {@link #load()} or
   * {@link #loadSorted(Comparator)} will be closed.
   * 
   * @throws IllegalArgumentException
   *           if the given cache is <code>null</code>
   * 
   */
  public void setCache(SourceCache cache) {
    if (cache == null)
      throw new IllegalArgumentException("cache must not be null");
    synchronized (this.cache) {
      this.cache.close(this);
      this.cache = cache;
    }
  }

  /**
   * Source of per document values like long, double or {@link BytesRef}
   * depending on the {@link IndexDocValues} fields {@link ValueType}. Source
   * implementations provide random access semantics similar to array lookups
   * and typically are entirely memory resident.
   * <p>
   * {@link Source} defines 3 {@link ValueType} //TODO finish this
   */
  public static abstract class Source {

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
     * 
     * @throws UnsupportedOperationException
     *           if this source doesn't support <tt>byte[]</tt> values.
     */
    public BytesRef getBytes(int docID, BytesRef ref) {
      throw new UnsupportedOperationException("bytes are not supported");
    }

    /**
     * Returns number of unique values. Some implementations may throw
     * UnsupportedOperationException.
     */
    public int getValueCount() {
      throw new UnsupportedOperationException();
    }

    /**
     * Returns a {@link DocValuesEnum} for this source.
     */
    public DocValuesEnum getEnum() throws IOException {
      return getEnum(null);
    }

    /**
     * Returns the {@link ValueType} of this source.
     * 
     * @return the {@link ValueType} of this source.
     */
    public abstract ValueType type();

    /**
     * Returns a {@link DocValuesEnum} for this source which uses the given
     * {@link AttributeSource}.
     */
    public abstract DocValuesEnum getEnum(AttributeSource attrSource)
        throws IOException;
  }

  /**
   * {@link DocValuesEnum} utility for {@link Source} implemenations.
   * 
   */
  public abstract static class SourceEnum extends DocValuesEnum {
    protected final Source source;
    protected final int numDocs;
    protected int pos = -1;

    /**
     * Creates a new {@link SourceEnum}
     * 
     * @param attrs
     *          the {@link AttributeSource} for this enum
     * @param type
     *          the enums {@link ValueType}
     * @param source
     *          the source this enum operates on
     * @param numDocs
     *          the number of documents within the source
     */
    protected SourceEnum(AttributeSource attrs, ValueType type, Source source,
        int numDocs) {
      super(attrs, type);
      this.source = source;
      this.numDocs = numDocs;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public int docID() {
      return pos;
    }

    @Override
    public int nextDoc() throws IOException {
      if (pos == NO_MORE_DOCS)
        return NO_MORE_DOCS;
      return advance(pos + 1);
    }
  }

  /**
   * A sorted variant of {@link Source} for <tt>byte[]</tt> values per document.
   * <p>
   * Note: {@link DocValuesEnum} obtained from a {@link SortedSource} will
   * enumerate values in document order and not in sorted order.
   */
  public static abstract class SortedSource extends Source {

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
     * Returns ord for specified docID. If this docID had not been added to the
     * Writer, the ord is 0. Ord is dense, ie, starts at 0, then increments by 1
     * for the next (as defined by {@link Comparator} value.
     */
    public abstract int ord(int docID);

    /** Returns value for specified ord. */
    public abstract BytesRef getByOrd(int ord, BytesRef bytesRef);


    /**
     * Finds the ordinal whose value is greater or equal to the given value.
     * 
     * @return the given values ordinal if found or otherwise
     *         <code>(-(ord)-1)</code>, defined as the ordinal of the first
     *         element that is greater than the given value. This guarantees
     *         that the return value will always be &gt;= 0 if the given value
     *         is found.
     * 
     */
    public final int getByValue(BytesRef value) {
      return getByValue(value, new BytesRef());
    }

    /**
     * Performs a lookup by value.
     * 
     * @param value
     *          the value to look up
     * @param tmpRef
     *          a temporary {@link BytesRef} instance used to compare internal
     *          values to the given value. Must not be <code>null</code>
     * @return the given values ordinal if found or otherwise
     *         <code>(-(ord)-1)</code>, defined as the ordinal of the first
     *         element that is greater than the given value. This guarantees
     *         that the return value will always be &gt;= 0 if the given value
     *         is found.
     */
    public abstract int getByValue(BytesRef value, BytesRef tmpRef);
  }
}
