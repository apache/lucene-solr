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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/**
 * A LeafReader implementation that will cache docvalues iterators, so that multiple
 * calls to {@link #getNumericDocValues(String)}, etc, for the same field will only
 * pull a single underlying iterator.
 *
 * The underlying iterators should be advanced by calling {@link #advanceAll(int)}.
 * Iterators returned from the {@code getXXXDocValues} methods will only allow
 * advancing by calling {@code advance(target)} or {@code advanceExact(target)} to
 * the doc passed to {@link #advanceAll(int)}.  Calling {@code nextDoc()}, or
 * advancing to a different document will throw an exception.
 *
 * Useful for e.g. grouping code that maintains multiple
 * {@link org.apache.lucene.search.FieldComparator} instances for a common
 * sort field.
 *
 * Consumers that use a reader context to pass references to this reader should
 * call {@link #getPooledContext()} rather than {@link #getContext()}, to ensure
 * that {@code docBase} and {@code ord} values from the wrapped reader context
 * are preserved.
 *
 * If you only want to pool certain fields (for example, if you are using an
 * expression with several variables, only one of which is referenced multiple
 * times) then you can pass a field filter predicate to the constructor which
 * will select which fields to pool.  Pooling has an overhead, particularly
 * for {@link SortedSetDocValues} and {@link SortedNumericDocValues} which need
 * to read all values for a document up-front.
 */
public class PooledDocValuesReader extends FilterLeafReader {

  private final Map<String, PooledIterator> pooledValues = new HashMap<>();
  private final LeafReaderContext pooledContext;
  private final Predicate<String> fieldFilter;

  private int currentDoc = -1;

  /**
   * Creates a new PooledDocValuesReader, wrapping an existing reader context
   * and pooling all docvalues
   */
  public PooledDocValuesReader(LeafReaderContext in) {
    this(in, field -> true);
  }

  /**
   * Creates a new PooledDocValuesReader, wrapping an existing reader context and
   * pooling docvalues from fields that match the fieldFilter predicate
   */
  public PooledDocValuesReader(LeafReaderContext in, Predicate<String> fieldFilter) {
    super(in.reader());
    this.fieldFilter = fieldFilter;
    this.pooledContext = new LeafReaderContext(in.parent, this, in.ordInParent, in.docBaseInParent, in.ord, in.docBase);
  }

  /**
   * The reader context for this reader.
   *
   * Preserves the docBase and ord from the wrapped reader context - prefer this
   * to calling {@link #getContext()}
   */
  public LeafReaderContext getPooledContext() {
    return pooledContext;
  }

  /**
   * Advances all pooled doc values iterators to the given target doc
   */
  public void advanceAll(int target) throws IOException {
    this.currentDoc = target;
    for (PooledIterator it : pooledValues.values()) {
      it.advanceReal(target);
    }
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    if (fieldFilter.test(field) == false) {
      return super.getNumericDocValues(field);
    }
    if (pooledValues.containsKey(field) == false) {
      NumericDocValues ndv = in.getNumericDocValues(field);
      if (ndv == null) {
        return null;
      }
      pooledValues.put(field, new PooledNumeric(ndv));
    }
    PooledIterator it = pooledValues.get(field);
    if (it instanceof PooledNumeric == false) {
      return null;
    }
    return (NumericDocValues) it;
  }

  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    if (fieldFilter.test(field) == false) {
      return super.getSortedNumericDocValues(field);
    }
    if (pooledValues.containsKey(field) == false) {
      SortedNumericDocValues sndv = in.getSortedNumericDocValues(field);
      if (sndv == null) {
        return null;
      }
      pooledValues.put(field, new PooledSortedNumeric(sndv));
    }
    PooledIterator it = pooledValues.get(field);
    if (it instanceof PooledSortedNumeric == false) {
      return null;
    }
    return ((PooledSortedNumeric)it).getIterator();
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    if (fieldFilter.test(field) == false) {
      return super.getBinaryDocValues(field);
    }
    if (pooledValues.containsKey(field) == false) {
      BinaryDocValues bdv = in.getBinaryDocValues(field);
      if (bdv == null) {
        return null;
      }
      pooledValues.put(field, new PooledBinary(bdv));
    }
    PooledIterator it = pooledValues.get(field);
    if (it instanceof PooledBinary == false) {
      return null;
    }
    return (BinaryDocValues) it;
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    if (fieldFilter.test(field) == false) {
      return super.getSortedDocValues(field);
    }
    if (pooledValues.containsKey(field) == false) {
      SortedDocValues sdv = in.getSortedDocValues(field);
      if (sdv == null) {
        return null;
      }
      pooledValues.put(field, new PooledSorted(sdv));
    }
    PooledIterator it = pooledValues.get(field);
    if (it instanceof PooledSorted == false) {
      return null;
    }
    return (SortedDocValues) it;
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    if (fieldFilter.test(field) == false) {
      return super.getSortedSetDocValues(field);
    }
    if (pooledValues.containsKey(field) == false) {
      SortedSetDocValues ssdv = in.getSortedSetDocValues(field);
      if (ssdv == null) {
        return null;
      }
      pooledValues.put(field, new PooledSortedSet(ssdv));
    }
    PooledIterator it = pooledValues.get(field);
    if (it instanceof PooledSortedSet == false) {
      return null;
    }
    return ((PooledSortedSet)it).getIterator();
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    return in.getCoreCacheHelper();
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return in.getReaderCacheHelper();
  }

  private interface PooledIterator {
    void advanceReal(int target) throws IOException;
  }

  private class PooledNumeric extends NumericDocValues implements PooledIterator {

    final NumericDocValues in;
    boolean positioned = false;
    int realDoc = -1;

    private PooledNumeric(NumericDocValues in) {
      this.in = in;
    }

    @Override
    public void advanceReal(int target) throws IOException {
      if (target > realDoc) {
        realDoc = in.advance(target);
      }
      positioned = realDoc == target;
    }

    @Override
    public long longValue() throws IOException {
      return in.longValue();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      if (target != currentDoc) {
        throw new IllegalStateException("Pooled docvalues can only be advanced to their controlling doc " + target);
      }
      return positioned;
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      throw new IllegalStateException("Cannot call nextDoc() on pooled docvalues");
    }

    @Override
    public int advance(int target) throws IOException {
      if (target != currentDoc) {
        throw new IllegalStateException("Pooled docvalues can only be advanced to their controlling doc " + target);
      }
      return realDoc;
    }

    @Override
    public long cost() {
      return in.cost();
    }
  }

  private class PooledSortedNumeric implements PooledIterator {

    final SortedNumericDocValues in;
    boolean positioned;
    int realDoc = -1;
    long[] values = new long[1];

    private PooledSortedNumeric(SortedNumericDocValues in) {
      this.in = in;
    }

    @Override
    public void advanceReal(int target) throws IOException {
      if (target > realDoc) {
        realDoc = in.advance(target);
      }
      positioned = realDoc == target;
      if (positioned) {
        int c = in.docValueCount();
        values = ArrayUtil.grow(values, c);
        for (int i = 0; i < c; i++) {
          values[i] = in.nextValue();
        }
      }
    }

    SortedNumericDocValues getIterator() {
      return new SortedNumericDocValues() {

        int currentValue;

        @Override
        public long nextValue() {
          currentValue++;
          return values[currentValue - 1];
        }

        @Override
        public int docValueCount() {
          return in.docValueCount();
        }

        @Override
        public boolean advanceExact(int target) {
          if (target != currentDoc) {
            throw new IllegalStateException("Pooled docvalues can only be advanced to their controlling doc " + target);
          }
          currentValue = 0;
          return positioned;
        }

        @Override
        public int docID() {
          return currentDoc;
        }

        @Override
        public int nextDoc() {
          throw new IllegalStateException("Cannot call nextDoc() on pooled docvalues");
        }

        @Override
        public int advance(int target) {
          if (target != currentDoc) {
            throw new IllegalStateException("Pooled docvalues can only be advanced to their controlling doc " + target);
          }
          currentValue = 0;
          return realDoc;
        }

        @Override
        public long cost() {
          return in.cost();
        }
      };
    }
  }

  private class PooledBinary extends BinaryDocValues implements PooledIterator {

    final BinaryDocValues in;
    boolean positioned = false;
    int realDoc = -1;

    private PooledBinary(BinaryDocValues in) {
      this.in = in;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
      return in.binaryValue();
    }

    @Override
    public void advanceReal(int target) throws IOException {
      if (target > realDoc) {
        realDoc = in.advance(target);
      }
      positioned = realDoc == target;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      if (target != currentDoc) {
        throw new IllegalStateException("Pooled docvalues can only be advanced to their controlling doc " + target);
      }
      return positioned;
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      throw new IllegalStateException("Cannot call nextDoc() on pooled docvalues");
    }

    @Override
    public int advance(int target) throws IOException {
      if (target != currentDoc) {
        throw new IllegalStateException("Pooled docvalues can only be advanced to their controlling doc " + target);
      }
      return realDoc;
    }

    @Override
    public long cost() {
      return in.cost();
    }
  }

  private class PooledSorted extends SortedDocValues implements PooledIterator {

    final SortedDocValues in;
    boolean positioned = false;
    int realDoc = -1;

    private PooledSorted(SortedDocValues in) {
      this.in = in;
    }

    @Override
    public int ordValue() throws IOException {
      return in.ordValue();
    }

    @Override
    public BytesRef lookupOrd(int ord) throws IOException {
      return in.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
      return in.getValueCount();
    }

    @Override
    public void advanceReal(int target) throws IOException {
      if (target > realDoc) {
        realDoc = in.advance(target);
      }
      positioned = realDoc == target;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      if (target != currentDoc) {
        throw new IllegalStateException("Pooled docvalues can only be advanced to their controlling doc " + target);
      }
      return positioned;
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      throw new IllegalStateException("Cannot call nextDoc() on pooled docvalues");
    }

    @Override
    public int advance(int target) throws IOException {
      if (target != currentDoc) {
        throw new IllegalStateException("Pooled docvalues can only be advanced to their controlling doc " + target);
      }
      return realDoc;
    }

    @Override
    public long cost() {
      return in.cost();
    }
  }

  private class PooledSortedSet implements PooledIterator {

    final SortedSetDocValues in;
    boolean positioned = false;
    int realDoc = -1;
    long[] ords = new long[1];

    private PooledSortedSet(SortedSetDocValues in) {
      this.in = in;
    }

    @Override
    public void advanceReal(int target) throws IOException {
      if (target > realDoc) {
        realDoc = in.advance(target);
      }
      positioned = realDoc == target;
      if (positioned) {
        int c = 0;
        long ord;
        do {
          ord = in.nextOrd();
          ords = ArrayUtil.grow(ords, c + 1);
          ords[c] = ord;
          c++;
        } while (ord != SortedSetDocValues.NO_MORE_ORDS);
      }
    }

    SortedSetDocValues getIterator() {
      return new SortedSetDocValues() {

        int current;

        @Override
        public long nextOrd() {
          current++;
          return ords[current - 1];
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
          return in.lookupOrd(ord);
        }

        @Override
        public long getValueCount() {
          return in.getValueCount();
        }

        @Override
        public boolean advanceExact(int target) {
          if (target != currentDoc) {
            throw new IllegalStateException("Pooled docvalues can only be advanced to their controlling doc " + target);
          }
          current = 0;
          return positioned;
        }

        @Override
        public int docID() {
          return in.docID();
        }

        @Override
        public int nextDoc() {
          throw new IllegalStateException("Cannot call nextDoc() on pooled docvalues");
        }

        @Override
        public int advance(int target) {
          if (target != currentDoc) {
            throw new IllegalStateException("Pooled docvalues can only be advanced to their controlling doc " + target);
          }
          current = 0;
          return realDoc;
        }

        @Override
        public long cost() {
          return in.cost();
        }
      };
    }
  }
}
