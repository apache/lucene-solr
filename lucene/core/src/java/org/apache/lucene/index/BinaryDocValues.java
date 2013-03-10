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

import org.apache.lucene.util.BytesRef;

/**
 * A per-document byte[]
 */
public abstract class BinaryDocValues {
  
  /** Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.) */
  protected BinaryDocValues() {}

  /** Lookup the value for document. */
  public abstract void get(int docID, BytesRef result);

  /**
   * Indicates the value was missing for the document.
   */
  public static final byte[] MISSING = new byte[0];
  
  /** An empty BinaryDocValues which returns {@link #MISSING} for every document */
  public static final BinaryDocValues EMPTY = new BinaryDocValues() {
    @Override
    public void get(int docID, BytesRef result) {
      result.bytes = MISSING;
      result.offset = 0;
      result.length = 0;
    }
  };
}
