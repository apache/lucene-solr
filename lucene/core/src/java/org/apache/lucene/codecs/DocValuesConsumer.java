package org.apache.lucene.codecs;

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

import org.apache.lucene.document.DocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * Abstract API that consumes {@link IndexableField}s.
 * {@link DocValuesConsumer} are always associated with a specific field and
 * segments. Concrete implementations of this API write the given
 * {@link IndexableField} into a implementation specific format depending on
 * the fields meta-data.
 * 
 * @lucene.experimental
 */
public abstract class DocValuesConsumer {

  protected final BytesRef spare = new BytesRef();

  protected abstract Type getType();
  /**
   * Adds the given {@link IndexableField} instance to this
   * {@link DocValuesConsumer}
   * 
   * @param docID
   *          the document ID to add the value for. The docID must always
   *          increase or be <tt>0</tt> if it is the first call to this method.
   * @param value
   *          the value to add
   * @throws IOException
   *           if an {@link IOException} occurs
   */
  public abstract void add(int docID, IndexableField value)
      throws IOException;

  /**
   * Called when the consumer of this API is done adding values.
   * 
   * @param docCount
   *          the total number of documents in this {@link DocValuesConsumer}.
   *          Must be greater than or equal the last given docID to
   *          {@link #add(int, IndexableField)}.
   * @throws IOException
   */
  public abstract void finish(int docCount) throws IOException;

  /**
   * Merges the given {@link org.apache.lucene.index.MergeState} into
   * this {@link DocValuesConsumer}.
   * 
   * @param mergeState
   *          the state to merge
   * @param docValues docValues array containing one instance per reader (
   *          {@link org.apache.lucene.index.MergeState#readers}) or <code>null</code> if the reader has
   *          no {@link DocValues} instance.
   * @throws IOException
   *           if an {@link IOException} occurs
   */
  public void merge(MergeState mergeState, DocValues[] docValues) throws IOException {
    assert mergeState != null;
    boolean hasMerged = false;
    for(int readerIDX=0;readerIDX<mergeState.readers.size();readerIDX++) {
      final org.apache.lucene.index.MergeState.IndexReaderAndLiveDocs reader = mergeState.readers.get(readerIDX);
      if (docValues[readerIDX] != null) {
        hasMerged = true;
        merge(docValues[readerIDX], mergeState.docBase[readerIDX],
              reader.reader.maxDoc(), reader.liveDocs);
        mergeState.checkAbort.work(reader.reader.maxDoc());
      }
    }
    // only finish if no exception is thrown!
    if (hasMerged) {
      finish(mergeState.mergedDocCount);
    }
  }

  /**
   * Merges the given {@link DocValues} into this {@link DocValuesConsumer}.
   * 
   * @throws IOException
   *           if an {@link IOException} occurs
   */
  protected void merge(DocValues reader, int docBase, int docCount, Bits liveDocs) throws IOException {
    // This enables bulk copies in subclasses per MergeState, subclasses can
    // simply override this and decide if they want to merge
    // segments using this generic implementation or if a bulk merge is possible
    // / feasible.
    final Source source = reader.getDirectSource();
    assert source != null;
    int docID = docBase;
    final Type type = getType();
    final Field scratchField;
    switch(type) {
    case VAR_INTS:
    case FIXED_INTS_16:
    case FIXED_INTS_32:
    case FIXED_INTS_64:
    case FIXED_INTS_8:
      scratchField = new DocValuesField("", (long) 0, type);
      break;
    case FLOAT_32:
    case FLOAT_64:
      scratchField = new DocValuesField("", (double) 0, type);
      break;
    case BYTES_FIXED_STRAIGHT:
    case BYTES_FIXED_DEREF:
    case BYTES_FIXED_SORTED:
    case BYTES_VAR_STRAIGHT:
    case BYTES_VAR_DEREF:
    case BYTES_VAR_SORTED:
      scratchField = new DocValuesField("", new BytesRef(), type);
      break;
    default:
      assert false;
      scratchField = null;
    }
    for (int i = 0; i < docCount; i++) {
      if (liveDocs == null || liveDocs.get(i)) {
        mergeDoc(scratchField, source, docID++, i);
      }
    }
  }

  /**
   * Merges a document with the given <code>docID</code>. The methods
   * implementation obtains the value for the <i>sourceDoc</i> id from the
   * current {@link Source}.
   * <p>
   * This method is used during merging to provide implementation agnostic
   * default merge implementation.
   * </p>
   * <p>
   * All documents IDs between the given ID and the previously given ID or
   * <tt>0</tt> if the method is call the first time are filled with default
   * values depending on the implementation. The given document
   * ID must always be greater than the previous ID or <tt>0</tt> if called the
   * first time.
   */
  protected void mergeDoc(Field scratchField, Source source, int docID, int sourceDoc)
      throws IOException {
    switch(getType()) {
    case BYTES_FIXED_DEREF:
    case BYTES_FIXED_SORTED:
    case BYTES_FIXED_STRAIGHT:
    case BYTES_VAR_DEREF:
    case BYTES_VAR_SORTED:
    case BYTES_VAR_STRAIGHT:
      scratchField.setBytesValue(source.getBytes(sourceDoc, spare));
      break;
    case FIXED_INTS_16:
    case FIXED_INTS_32:
    case FIXED_INTS_64:
    case FIXED_INTS_8:
    case VAR_INTS:
      scratchField.setLongValue(source.getInt(sourceDoc));
      break;
    case FLOAT_32:
    case FLOAT_64:
      scratchField.setDoubleValue(source.getFloat(sourceDoc));
      break;
    }
    add(docID, scratchField);
  }
}
