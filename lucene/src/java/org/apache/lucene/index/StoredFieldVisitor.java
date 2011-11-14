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
import org.apache.lucene.document.DocumentStoredFieldVisitor;

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

public abstract class StoredFieldVisitor {
  /** Process a binary field. */
  public void binaryField(FieldInfo fieldInfo, byte[] value, int offset, int length) throws IOException {
  }

  /** Process a string field */
  public void stringField(FieldInfo fieldInfo, String value) throws IOException {
  }

  /** Process a int numeric field. */
  public void intField(FieldInfo fieldInfo, int value) throws IOException {
  }

  /** Process a long numeric field. */
  public void longField(FieldInfo fieldInfo, long value) throws IOException {
  }

  /** Process a float numeric field. */
  public void floatField(FieldInfo fieldInfo, float value) throws IOException {
  }

  /** Process a double numeric field. */
  public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
  }
  
  public abstract Status needsField(FieldInfo fieldInfo) throws IOException;
  
  public static enum Status {
    /** yes, i want the field */
    YES,
    /** no, i do not */
    NO,
    /** stop loading fields for this document entirely */
    STOP
  }
}