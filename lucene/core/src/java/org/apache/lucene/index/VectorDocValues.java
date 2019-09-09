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
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;

import org.apache.lucene.store.ByteBufferIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * A per-document fixed-length array of float values.
 * TODO: genericize to support doubles and integers
 * JEP-338 would be a nice help here.
 */
public abstract class VectorDocValues extends DocValuesIterator {

  public static final String DIMENSION_ATTR = "dimension";

  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected VectorDocValues() {}

  /**
   * @return the number of values in each vector, which does not vary by document. This must always
   * be greater than zero unless there are no documents with this value, in which case it is -1.
   */
  public abstract int dimension();

  /**
   * Provides direct access to an array of values.
   * @param vector an array of values of size {@link #dimension()}. The array values are copied from the
   * field values into this array. It is an error to call this method if the iterator is not
   * positioned (ie one of the advance methods returned NO_MORE_DOCS, or false).
   */
  public abstract void vector(float[] vector) throws IOException;

  /**
   * Returns VectorDocValues for the field, or {@link DocValues#emptyBinary} if it has none.
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#BINARY}
   *                               or {@link DocValuesType#SORTED}
   * @throws IOException if an I/O error occurs.
   */
  public static VectorDocValues get(LeafReader reader, String field) throws IOException {
    return new BinaryVectorDV(reader, field);
  }

  public static VectorDocValues get(BinaryDocValues bdv, int dimension) {
    return new BinaryVectorDV(bdv, dimension);
  }

}

class BinaryVectorDV extends VectorDocValues {

  final BinaryDocValues bdv;
  final int dimension;

  BinaryVectorDV(LeafReader reader, String field) throws IOException {
    this(DocValues.getBinary(reader, field), reader.getFieldInfos().fieldInfo(field));
  }

  BinaryVectorDV(BinaryDocValues bdv, FieldInfo fieldInfo) throws IOException {
    if (fieldInfo != null) {
      String dimStr = fieldInfo.getAttribute(DIMENSION_ATTR);
      if (dimStr == null) {
        throw new IllegalStateException("DocValues type for field '" + fieldInfo.name + "' was indexed without a dimension.");
      }
      dimension = Integer.valueOf(dimStr);
      assert dimension > 0;
    } else {
      dimension = -1;
    }
    this.bdv = bdv;
  }

  BinaryVectorDV(BinaryDocValues bdv, int dimension) {
    this.bdv = bdv;
    this.dimension = dimension;
  }

  @Override
  public int dimension() {
    return dimension;
  }

  @Override
  public void vector(float[] vector) throws IOException {
    BytesRef b = bdv.binaryValue();
    ByteBuffer.wrap(b.bytes, b.offset, b.length).asFloatBuffer().get(vector);
  }

  @Override
  public boolean advanceExact(int target) throws IOException {
    return bdv.advanceExact(target);
  }

  @Override
  public int docID() {
    return bdv.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return bdv.nextDoc();
  }

  @Override
  public int advance(int target) throws IOException {
    return bdv.advance(target);
  }

  @Override
  public long cost() {
    return bdv.cost();
  }
}
