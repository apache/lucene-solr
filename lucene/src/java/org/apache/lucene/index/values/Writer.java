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

import org.apache.lucene.index.codecs.DocValuesConsumer;
import org.apache.lucene.index.values.IndexDocValues.Source;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;

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
  protected Source currentMergeSource;
  /**
   * Creates a new {@link Writer}.
   * 
   * @param bytesUsed
   *          bytes-usage tracking reference used by implementation to track
   *          internally allocated memory. All tracked bytes must be released
   *          once {@link #finish(int)} has been called.
   */
  protected Writer(Counter bytesUsed) {
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
   * Merges a document with the given <code>docID</code>. The methods
   * implementation obtains the value for the <i>sourceDoc</i> id from the
   * current {@link Source} set to <i>setNextMergeSource(Source)</i>.
   * <p>
   * This method is used during merging to provide implementation agnostic
   * default merge implementation.
   * </p>
   * <p>
   * All documents IDs between the given ID and the previously given ID or
   * <tt>0</tt> if the method is call the first time are filled with default
   * values depending on the {@link Writer} implementation. The given document
   * ID must always be greater than the previous ID or <tt>0</tt> if called the
   * first time.
   */
  protected abstract void mergeDoc(int docID, int sourceDoc) throws IOException;

  /**
   * Sets the next {@link Source} to consume values from on calls to
   * {@link #mergeDoc(int, int)}
   * 
   * @param mergeSource
   *          the next {@link Source}, this must not be null
   */
  protected void setNextMergeSource(Source mergeSource) {
    currentMergeSource = mergeSource;
  }

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
    final Source source = state.reader.getDirectSource();
    assert source != null;
    setNextMergeSource(source); // set the current enum we are working on - the
    // impl. will get the correct reference for the type
    // it supports
    int docID = state.docBase;
    final Bits liveDocs = state.liveDocs;
    final int docCount = state.docCount;
    for (int i = 0; i < docCount; i++) {
      if (liveDocs == null || liveDocs.get(i)) {
        mergeDoc(docID++, i);
      }
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
   * @param bytesUsed
   *          a byte-usage tracking reference
   * @return a new {@link Writer} instance for the given {@link ValueType}
   * @throws IOException
   */
  public static Writer create(ValueType type, String id, Directory directory,
      Comparator<BytesRef> comp, Counter bytesUsed, IOContext context) throws IOException {
    if (comp == null) {
      comp = BytesRef.getUTF8SortedAsUnicodeComparator();
    }
    switch (type) {
    case FIXED_INTS_16:
    case FIXED_INTS_32:
    case FIXED_INTS_64:
    case FIXED_INTS_8:
    case VAR_INTS:
      return Ints.getWriter(directory, id, bytesUsed, type, context);
    case FLOAT_32:
      return Floats.getWriter(directory, id, bytesUsed, context, type);
    case FLOAT_64:
      return Floats.getWriter(directory, id, bytesUsed, context, type);
    case BYTES_FIXED_STRAIGHT:
      return Bytes.getWriter(directory, id, Bytes.Mode.STRAIGHT, true, comp,
          bytesUsed, context);
    case BYTES_FIXED_DEREF:
      return Bytes.getWriter(directory, id, Bytes.Mode.DEREF, true, comp,
          bytesUsed, context);
    case BYTES_FIXED_SORTED:
      return Bytes.getWriter(directory, id, Bytes.Mode.SORTED, true, comp,
          bytesUsed, context);
    case BYTES_VAR_STRAIGHT:
      return Bytes.getWriter(directory, id, Bytes.Mode.STRAIGHT, false, comp,
          bytesUsed, context);
    case BYTES_VAR_DEREF:
      return Bytes.getWriter(directory, id, Bytes.Mode.DEREF, false, comp,
          bytesUsed, context);
    case BYTES_VAR_SORTED:
      return Bytes.getWriter(directory, id, Bytes.Mode.SORTED, false, comp,
          bytesUsed, context);
    default:
      throw new IllegalArgumentException("Unknown Values: " + type);
    }
  }
}
