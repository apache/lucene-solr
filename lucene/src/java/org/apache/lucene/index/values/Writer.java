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
import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.codecs.DocValuesConsumer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * Abstract API for per-document stored primitive values of type <tt>byte[]</tt>
 * , <tt>long</tt> or <tt>double</tt>. The API accepts a single value for each
 * document. The underlying storage mechanism, file formats, data-structures and
 * representations depend on the actual implementation.
 * <p>
 * Document IDs passed to this API must always be increasing unless stated
 * otherwise.
 * </p>
 * 
 * @lucene.experimental
 */
public abstract class Writer extends DocValuesConsumer {

  /**
   * Creates a new {@link Writer}.
   * 
   * @param bytesUsed
   *          bytes-usage tracking reference used by implementation to track
   *          internally allocated memory. All tracked bytes must be released
   *          once {@link #finish(int)} has been called.
   */
  protected Writer(AtomicLong bytesUsed) {
    super(bytesUsed);
  }

  /**
   * Filename extension for index files
   */
  public static final String INDEX_EXTENSION = "idx";
  
  /**
   * Filename extension for data files.
   */
  public static final String DATA_EXTENSION = "dat";

  /**
   * Records the specified <tt>long</tt> value for the docID or throws an
   * {@link UnsupportedOperationException} if this {@link Writer} doesn't record
   * <tt>long</tt> values.
   * 
   * @throws UnsupportedOperationException
   *           if this writer doesn't record <tt>long</tt> values
   */
  public void add(int docID, long value) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Records the specified <tt>double</tt> value for the docID or throws an
   * {@link UnsupportedOperationException} if this {@link Writer} doesn't record
   * <tt>double</tt> values.
   * 
   * @throws UnsupportedOperationException
   *           if this writer doesn't record <tt>double</tt> values
   */
  public void add(int docID, double value) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Records the specified {@link BytesRef} value for the docID or throws an
   * {@link UnsupportedOperationException} if this {@link Writer} doesn't record
   * {@link BytesRef} values.
   * 
   * @throws UnsupportedOperationException
   *           if this writer doesn't record {@link BytesRef} values
   */
  public void add(int docID, BytesRef value) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Records a value from the given document id. The methods implementation
   * obtains the value for the document id from the last {@link ValuesEnum}
   * set to {@link #setNextEnum(ValuesEnum)}.
   * <p>
   * This method is used during merging to provide implementation agnostic
   * default merge implementation.
   * </p>
   * <p>
   * The given document id must be the same document id returned from
   * {@link ValuesEnum#docID()} when this method is called. All documents IDs
   * between the given ID and the previously given ID or <tt>0</tt> if the
   * method is call the first time are filled with default values depending on
   * the {@link Writer} implementation. The given document ID must always be
   * greater than the previous ID or <tt>0</tt> if called the first time.
   */
  protected abstract void add(int docID) throws IOException;

  /**
   * Sets the next {@link ValuesEnum} to consume values from on calls to
   * {@link #add(int)}
   * 
   * @param valuesEnum
   *          the next {@link ValuesEnum}, this must not be null
   */
  protected abstract void setNextEnum(ValuesEnum valuesEnum);

  /**
   * Finish writing and close any files and resources used by this Writer.
   * 
   * @param docCount
   *          the total number of documents for this writer. This must be
   *          greater that or equal to the largest document id passed to one of
   *          the add methods after the {@link Writer} was created.
   */
  public abstract void finish(int docCount) throws IOException;

  @Override
  protected void merge(MergeState state) throws IOException {
    // This enables bulk copies in subclasses per MergeState, subclasses can
    // simply override this and decide if they want to merge
    // segments using this generic implementation or if a bulk merge is possible
    // / feasible.
    final ValuesEnum valEnum = state.reader.getEnum();
    assert valEnum != null;
    try {
      setNextEnum(valEnum); // set the current enum we are working on - the
      // impl. will get the correct reference for the type
      // it supports
      int docID = state.docBase;
      final Bits bits = state.bits;
      final int docCount = state.docCount;
      int currentDocId;
      if ((currentDocId = valEnum.advance(0)) != ValuesEnum.NO_MORE_DOCS) {
        for (int i = 0; i < docCount; i++) {
          if (bits == null || !bits.get(i)) {
            if (currentDocId < i) {
              if ((currentDocId = valEnum.advance(i)) == ValuesEnum.NO_MORE_DOCS) {
                break; // advance can jump over default values
              }
            }
            if (currentDocId == i) { // we are on the doc to merge
              add(docID);
            }
            ++docID;
          }
        }
      }
    } finally {
      valEnum.close();
    }
  }

  /**
   * Factory method to create a {@link Writer} instance for a given type. This
   * method returns default implementations for each of the different types
   * defined in the {@link ValueType} enumeration.
   * 
   * @param type
   *          the {@link ValueType} to create the {@link Writer} for
   * @param id
   *          the file name id used to create files within the writer.
   * @param directory
   *          the {@link Directory} to create the files from.
   * @param comp
   *          a {@link BytesRef} comparator used for {@link Bytes} variants. If
   *          <code>null</code>
   *          {@link BytesRef#getUTF8SortedAsUnicodeComparator()} is used as the
   *          default.
   * @param bytesUsed
   *          a byte-usage tracking reference
   * @return a new {@link Writer} instance for the given {@link ValueType}
   * @throws IOException
   */
  public static Writer create(ValueType type, String id, Directory directory,
      Comparator<BytesRef> comp, AtomicLong bytesUsed, IOContext context) throws IOException {
    if (comp == null) {
      comp = BytesRef.getUTF8SortedAsUnicodeComparator();
    }
    switch (type) {
    case INTS:
      return Ints.getWriter(directory, id, true, bytesUsed, context);
    case FLOAT_32:
      return Floats.getWriter(directory, id, 4, bytesUsed, context);
    case FLOAT_64:
      return Floats.getWriter(directory, id, 8, bytesUsed, context);
    case BYTES_FIXED_STRAIGHT:
      return Bytes.getWriter(directory, id, Bytes.Mode.STRAIGHT, comp, true,
          bytesUsed, context);
    case BYTES_FIXED_DEREF:
      return Bytes.getWriter(directory, id, Bytes.Mode.DEREF, comp, true,
          bytesUsed, context);
    case BYTES_FIXED_SORTED:
      return Bytes.getWriter(directory, id, Bytes.Mode.SORTED, comp, true,
          bytesUsed, context);
    case BYTES_VAR_STRAIGHT:
      return Bytes.getWriter(directory, id, Bytes.Mode.STRAIGHT, comp, false,
          bytesUsed, context);
    case BYTES_VAR_DEREF:
      return Bytes.getWriter(directory, id, Bytes.Mode.DEREF, comp, false,
          bytesUsed, context);
    case BYTES_VAR_SORTED:
      return Bytes.getWriter(directory, id, Bytes.Mode.SORTED, comp, false,
          bytesUsed, context);
    default:
      throw new IllegalArgumentException("Unknown Values: " + type);
    }
  }
}
