package org.apache.lucene.codecs;

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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;

/** Abstract API that produces numeric, binary and
 * sorted docvalues.
 *
 * @lucene.experimental
 */
public abstract class DocValuesProducer implements Closeable {
  
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected DocValuesProducer() {}

  /** Returns {@link NumericDocValues} for this field.
   *  The returned instance need not be thread-safe: it will only be
   *  used by a single thread. */
  public abstract NumericDocValues getNumeric(FieldInfo field) throws IOException;

  /** Returns {@link BinaryDocValues} for this field.
   *  The returned instance need not be thread-safe: it will only be
   *  used by a single thread. */
  public abstract BinaryDocValues getBinary(FieldInfo field) throws IOException;

  /** Returns {@link SortedDocValues} for this field.
   *  The returned instance need not be thread-safe: it will only be
   *  used by a single thread. */
  public abstract SortedDocValues getSorted(FieldInfo field) throws IOException;
  
  /** Returns {@link SortedSetDocValues} for this field.
   *  The returned instance need not be thread-safe: it will only be
   *  used by a single thread. */
  public abstract SortedSetDocValues getSortedSet(FieldInfo field) throws IOException;
}
