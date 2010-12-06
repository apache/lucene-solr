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

import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
/**
 * 
 * @lucene.experimental
 */
public abstract class DocValues implements Closeable {

  public static final DocValues[] EMPTY_ARRAY = new DocValues[0];
  private SourceCache cache = new SourceCache.DirectSourceCache();

  public ValuesEnum getEnum() throws IOException {
    return getEnum(null);
  }

  public abstract ValuesEnum getEnum(AttributeSource attrSource)
      throws IOException;

  public abstract Source load() throws IOException;

  public Source getSource() throws IOException {
    return cache.load(this);
  }

  public SortedSource getSortedSorted(Comparator<BytesRef> comparator)
      throws IOException {
    return cache.laodSorted(this, comparator);
  }

  public SortedSource loadSorted(Comparator<BytesRef> comparator)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  public abstract Values type();

  public void close() throws IOException {
    this.cache.close(this);
  }
  
  public void setCache(SourceCache cache) {
    synchronized (this.cache) {
      this.cache.close(this);
      this.cache = cache;
    }
  }

  /**
   * Source of integer (returned as java long), per document. The underlying
   * implementation may use different numbers of bits per value; long is only
   * used since it can handle all precisions.
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

    public ValuesEnum getEnum() throws IOException {
      return getEnum(null);
    }
    
    public MissingValue getMissing() {
      return missingValue;
    }
    
    public abstract Values type();

    public abstract ValuesEnum getEnum(AttributeSource attrSource)
        throws IOException;

  }

  abstract static class SourceEnum extends ValuesEnum {
    protected final Source source;
    protected final int numDocs;
    protected int pos = -1;

    SourceEnum(AttributeSource attrs, Values type, Source source, int numDocs) {
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
      if(pos == NO_MORE_DOCS)
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
