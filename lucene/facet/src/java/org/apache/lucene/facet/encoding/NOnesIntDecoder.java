package org.apache.lucene.facet.encoding;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

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

/**
 * Decodes values encoded encoded with {@link NOnesIntEncoder}.
 * 
 * @lucene.experimental
 */
public class NOnesIntDecoder extends FourFlagsIntDecoder {

  // Number of consecutive '1's to generate upon decoding a '2'
  private final int n;
  private final IntsRef internalBuffer;
  
  /**
   * Constructs a decoder with a given N (Number of consecutive '1's which are
   * translated into a single target value '2'.
   */
  public NOnesIntDecoder(int n) {
    this.n = n;
    // initial size (room for 100 integers)
    internalBuffer = new IntsRef(100);
  }

  @Override
  public void decode(BytesRef buf, IntsRef values) {
    values.offset = values.length = 0;
    internalBuffer.length = 0;
    super.decode(buf, internalBuffer);
    if (values.ints.length < internalBuffer.length) {
      // need space for internalBuffer.length to internalBuffer.length*N,
      // grow mildly at first
      values.grow(internalBuffer.length * n/2);
    }
    
    for (int i = 0; i < internalBuffer.length; i++) {
      int decode = internalBuffer.ints[i];
      if (decode == 1) {
        if (values.length == values.ints.length) {
          values.grow(values.length + 10); // grow by few items, however not too many
        }
        // 1 is 1
        values.ints[values.length++] = 1;
      } else if (decode == 2) {
        if (values.length + n >= values.ints.length) {
          values.grow(values.length + n); // grow by few items, however not too many
        }
        // '2' means N 1's
        for (int j = 0; j < n; j++) {
          values.ints[values.length++] = 1;
        }
      } else {
        if (values.length == values.ints.length) {
          values.grow(values.length + 10); // grow by few items, however not too many
        }
        // any other value is val-1
        values.ints[values.length++] = decode - 1;
      }
    }
  }

  @Override
  public String toString() {
    return "NOnes(" + n + ") (" + super.toString() + ")";
  }

}
