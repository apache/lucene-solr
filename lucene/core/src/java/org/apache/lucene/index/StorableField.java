package org.apache.lucene.index;

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

import java.io.Reader;

import org.apache.lucene.util.BytesRef;

/** Represents a single stored field in lucene document. These fields
 * are contained in document retrieved from IndexReader.
 *
 *  @lucene.experimental */

public interface StorableField extends GeneralField {

  /** Non-null if this field has a binary value */
  public BytesRef binaryValue();

  /** Non-null if this field has a string value */
  public String stringValue();

  /** Non-null if this field has a Reader value */
  public Reader readerValue();

  /** Non-null if this field has a numeric value */
  public Number numericValue(); 
}
