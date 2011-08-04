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

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.store.IndexInput;

/**
 * Expert: provides a low-level means of accessing the stored field
 * values in an index.  See {@link IndexReader#document(int,
 * StoredFieldVisitor)}.
 *
 * See {@link DocumentStoredFieldVisitor}, which is a
 * <code>StoredFieldVisitor</code> that builds the
 * {@link Document} containing all stored fields.  This is
 * used by {@link IndexReader#document(int)}.
 *
 * @lucene.experimental */

public class StoredFieldVisitor {
  /** Process a binary field.  Note that if you want to
   *  skip the field you must seek the IndexInput
   *  (e.g., call <code>in.seek(numUTF8Bytes + in.getFilePointer()</code>)
   *
   *  <p>Return true to stop loading fields. */
  public boolean binaryField(FieldInfo fieldInfo, IndexInput in, int numBytes) throws IOException {
    in.seek(in.getFilePointer() + numBytes);
    return false;
  }

  /** Process a string field by reading numUTF8Bytes.
   *  Note that if you want to skip the field you must
   *  seek the IndexInput as if you had read numBytes by
   *  (e.g., call <code>in.seek(numUTF8Bytes + in.getFilePointer()</code>)
   *
   *  <p>Return true to stop loading fields. */
  public boolean stringField(FieldInfo fieldInfo, IndexInput in, int numUTF8Bytes) throws IOException {
    in.seek(in.getFilePointer() + numUTF8Bytes);
    return false;
  }

  /** Process a int numeric field.
   *
   *  <p>Return true to stop loading fields. */
  public boolean intField(FieldInfo fieldInfo, int value) throws IOException {
    return false;
  }

  /** Process a long numeric field.
   *
   *  <p>Return true to stop loading fields. */
  public boolean longField(FieldInfo fieldInfo, long value) throws IOException {
    return false;
  }

  /** Process a float numeric field.
   *
   *  <p>Return true to stop loading fields. */
  public boolean floatField(FieldInfo fieldInfo, float value) throws IOException {
    return false;
  }

  /** Process a double numeric field.
   *
   *  <p>Return true to stop loading fields. */
  public boolean doubleField(FieldInfo fieldInfo, double value) throws IOException {
    return false;
  }
}

