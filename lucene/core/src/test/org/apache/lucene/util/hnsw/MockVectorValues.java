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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

class MockVectorValues extends VectorValues
    implements RandomAccessVectorValues, RandomAccessVectorValuesProducer {
  private final float[] scratch;

  protected final int dimension;
  protected final float[][] denseValues;
  protected final float[][] values;
  protected final SearchStrategy searchStrategy;
  private final int numVectors;

  private int pos = -1;

  MockVectorValues(SearchStrategy searchStrategy, float[][] values) {
    this.searchStrategy = searchStrategy;
    this.dimension = values[0].length;
    this.values = values;
    int maxDoc = values.length;
    denseValues = new float[maxDoc][];
    int count = 0;
    for (int i = 0; i < maxDoc; i++) {
      if (values[i] != null) {
        denseValues[count++] = values[i];
      }
    }
    numVectors = count;
    scratch = new float[dimension];
  }

  public MockVectorValues copy() {
    return new MockVectorValues(searchStrategy, values);
  }

  @Override
  public int size() {
    return numVectors;
  }

  @Override
  public SearchStrategy searchStrategy() {
    return searchStrategy;
  }

  @Override
  public int dimension() {
    return dimension;
  }

  @Override
  public float[] vectorValue() {
    if (LuceneTestCase.random().nextBoolean()) {
      return values[pos];
    } else {
      // Sometimes use the same scratch array repeatedly, mimicing what the codec will do.
      // This should help us catch cases of aliasing where the same VectorValues source is used
      // twice in a
      // single computation.
      System.arraycopy(values[pos], 0, scratch, 0, dimension);
      return scratch;
    }
  }

  @Override
  public RandomAccessVectorValues randomAccess() {
    return copy();
  }

  @Override
  public float[] vectorValue(int targetOrd) {
    return denseValues[targetOrd];
  }

  @Override
  public BytesRef binaryValue(int targetOrd) {
    return null;
  }

  @Override
  public TopDocs search(float[] target, int k, int fanout) {
    return null;
  }

  private boolean seek(int target) {
    if (target >= 0 && target < values.length && values[target] != null) {
      pos = target;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int docID() {
    return pos;
  }

  @Override
  public int nextDoc() {
    return advance(pos + 1);
  }

  public int advance(int target) {
    while (++pos < values.length) {
      if (seek(pos)) {
        return pos;
      }
    }
    return NO_MORE_DOCS;
  }

  @Override
  public long cost() {
    return size();
  }
}
