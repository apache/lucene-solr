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
 * 
 * @see FieldsEnum#docValues()
 * @see Fields#docValues(String)
 * @lucene.experimental
 */
public abstract class DocValues implements Closeable {

  public static final DocValues[] EMPTY_ARRAY = new DocValues[0];

  private SourceCache cache = new SourceCache.DirectSourceCache();

  /**
   * Returns an iterator that steps through all documents values for this
   * {@link DocValues} field instance. {@link DocValuesEnum} will skip document
   * without a value if applicable.
   */
  public DocValuesEnum getEnum() throws IOException {
    return getEnum(null);
  }

  /**
   * Returns an iterator that steps through all documents values for this
   * {@link DocValues} field instance. {@link DocValuesEnum} will skip document
   * without a value if applicable.
   * <p>
   * If an {@link AttributeSource} is supplied to this method the
   * {@link DocValuesEnum} will use the given source to access implementation
   * related attributes.
   */
  public abstract DocValuesEnum getEnum(AttributeSource attrSource)
      throws IOException;

  /**
   * Loads a new {@link Source} instance for this {@link DocValues} field
   * instance. Source instances returned from this method are not cached. It is
   * the callers responsibility to maintain the instance and release its
   * resources once the source is not needed anymore.
   * <p>
   * This method will return null iff this {@link DocValues} represent a
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
   * from the cache once this {@link DocValues} instance is closed by the
   * {@link IndexReader}, {@link Fields} or {@link FieldsEnum} the
   * {@link DocValues} was created from.
   * <p>
   * This method will return null iff this {@link DocValues} represent a
   * {@link SortedSource}.
   */
  public Source getSource() throws IOException {
    return cache.load(this);
  }

  /**
   * Returns a {@link SortedSource} instance for this {@link DocValues} field
   * instance like {@link #getSource()}.
   * <p>
   * This method will return null iff this {@link DocValues} represent a
   * {@link Source} instead of a {@link SortedSource}.
   */
  public SortedSource getSortedSorted(Comparator<BytesRef> comparator)
      throws IOException {
    return cache.loadSorted(this, comparator);
  }

  /**
   * Loads and returns a {@link SortedSource} instance for this
   * {@link DocValues} field instance like {@link #load()}.
   * <p>
   * This method will return null iff this {@link DocValues} represent a
   * {@link Source} instead of a {@link SortedSource}.
   */
  public SortedSource loadSorted(Comparator<BytesRef> comparator)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the {@link Type} of this {@link DocValues} instance
   */
  public abstract Type type();

  /**
   * Closes this {@link DocValues} instance. This method should only be called
   * by the creator of this {@link DocValues} instance. API users should not
   * close {@link DocValues} instances.
   */
  public void close() throws IOException {
    this.cache.close(this);
  }

  /**
   * Sets the {@link SourceCache} used by this {@link DocValues} instance. This
   * method should be called before {@link #load()} or
   * {@link #loadSorted(Comparator)} is called. All {@link Source} or
   * {@link SortedSource} instances in the currently used cache will be closed
   * before the new cache is installed.
   * <p>
   * Note: All instances previously obtained from {@link #load()} or
   * {@link #loadSorted(Comparator)} will be closed.
   */
  public void setCache(SourceCache cache) {
    assert cache != null : "cache must not be null";
    synchronized (this.cache) {
      this.cache.close(this);
      this.cache = cache;
    }
  }

  /**
   * Source of per document values like long, double or {@link BytesRef}
   * depending on the {@link DocValues} fields {@link Type}. Source
   * implementations provide random access semantics similar to array lookups
   * and typically are entirely memory resident.
   * <p>
   * {@link Source} defines 3 {@link Type} //TODO finish this 
   */
  public static abstract class Source {
    protected final MissingValue missingValue = new MissingValue();

    public long getInt(int docID) {
      throw new UnsupportedOperationException("ints are not supported");
    }

    public double getFloat(int docID) {
      throw new UnsupportedOperationException("floats are not supported");
    }

    public BytesRef getBytes(int docID, BytesRef ref) {
      throw new UnsupportedOperationException("bytes are not supported");
    }

    /**
     * Returns number of unique values. Some impls may throw
     * UnsupportedOperationException.
     */
    public int getValueCount() {
      throw new UnsupportedOperationException();
    }

    public DocValuesEnum getEnum() throws IOException {
      return getEnum(null);
    }

    public MissingValue getMissing() {
      return missingValue;
    }

    public abstract Type type();

    public abstract DocValuesEnum getEnum(AttributeSource attrSource)
        throws IOException;

  }

  abstract static class SourceEnum extends DocValuesEnum {
    protected final Source source;
    protected final int numDocs;
    protected int pos = -1;

    SourceEnum(AttributeSource attrs, Type type, Source source, int numDocs) {
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

  public static abstract class SortedSource extends Source {

    @Override
    public BytesRef getBytes(int docID, BytesRef bytesRef) {
      return getByOrd(ord(docID), bytesRef);
    }

    /**
     * Returns ord for specified docID. If this docID had not been added to the
     * Writer, the ord is 0. Ord is dense, ie, starts at 0, then increments by 1
     * for the next (as defined by {@link Comparator} value.
     */
    public abstract int ord(int docID);

    /** Returns value for specified ord. */
    public abstract BytesRef getByOrd(int ord, BytesRef bytesRef);

    public static class LookupResult {
      public boolean found;
      public int ord;
    }

    /**
     * Finds the largest ord whose value is <= the requested value. If
     * {@link LookupResult#found} is true, then ord is an exact match. The
     * returned {@link LookupResult} may be reused across calls.
     */
    public final LookupResult getByValue(BytesRef value) {
      return getByValue(value, new BytesRef());
    }

    public abstract LookupResult getByValue(BytesRef value, BytesRef tmpRef);
  }

  public final static class MissingValue {
    public long longValue;
    public double doubleValue;
    public BytesRef bytesValue;

    public final void copy(MissingValue values) {
      longValue = values.longValue;
      doubleValue = values.doubleValue;
      bytesValue = values.bytesValue;
    }
  }

}
