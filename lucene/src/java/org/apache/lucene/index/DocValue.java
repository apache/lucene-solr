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
import java.util.Comparator;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.document.DocValuesField;
import org.apache.lucene.util.BytesRef;

/**
 * Per document and field values consumed by {@link DocValuesConsumer}. 
 * @see DocValuesField
 * 
 * @lucene.experimental
 */
public interface DocValue {

  /**
   * Returns the set {@link BytesRef} or <code>null</code> if not set.
   */
  public BytesRef getBytes();

  /**
   * Returns the set {@link BytesRef} comparator or <code>null</code> if not set
   */
  public Comparator<BytesRef> bytesComparator();

  /**
   * Returns the set floating point value or <code>0.0d</code> if not set.
   */
  public double getFloat();

  /**
   * Returns the set <code>long</code> value of <code>0</code> if not set.
   */
  public long getInt();

}
