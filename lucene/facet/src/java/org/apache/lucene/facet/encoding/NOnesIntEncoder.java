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
 * A variation of {@link FourFlagsIntEncoder} which translates the data as
 * follows:
 * <ul>
 * <li>Values &ge; 2 are trnalsated to <code>value+1</code> (2 &rArr; 3, 3
 * &rArr; 4 and so forth).
 * <li>Any <code>N</code> occurrences of 1 are encoded as a single 2.
 * <li>Otherwise, each 1 is encoded as 1.
 * </ul>
 * <p>
 * Encoding examples:
 * <ul>
 * <li>N = 4: the data 1,1,1,1,1 is translated to: 2, 1
 * <li>N = 3: the data 1,2,3,4,1,1,1,1,5 is translated to 1,3,4,5,2,1,6
 * </ul>
 * <b>NOTE:</b> this encoder does not support values &le; 0 and
 * {@link Integer#MAX_VALUE}. 0 is not supported because it's not supported by
 * {@link FourFlagsIntEncoder} and {@link Integer#MAX_VALUE} because this
 * encoder translates N to N+1, which will cause an overflow and
 * {@link Integer#MAX_VALUE} will become a negative number, which is not
 * supported as well.<br>
 * This does not mean you cannot encode {@link Integer#MAX_VALUE}. If it is not
 * the first value to encode, and you wrap this encoder with
 * {@link DGapIntEncoder}, then the value that will be sent to this encoder will
 * be <code>MAX_VAL - prev</code>.
 * 
 * @lucene.experimental
 */
public class NOnesIntEncoder extends FourFlagsIntEncoder {

  private final IntsRef internalBuffer;
  
  /** Number of consecutive '1's to be translated into single target value '2'. */
  private final int n;

  /**
   * Constructs an encoder with a given value of N (N: Number of consecutive
   * '1's to be translated into single target value '2').
   */
  public NOnesIntEncoder(int n) {
    this.n = n;
    internalBuffer = new IntsRef(n);
  }

  @Override
  public void encode(IntsRef values, BytesRef buf) {
    internalBuffer.length = 0;
    // make sure the internal buffer is large enough
    if (values.length > internalBuffer.ints.length) {
      internalBuffer.grow(values.length);
    }
    
    int onesCounter = 0;
    int upto = values.offset + values.length;
    for (int i = values.offset; i < upto; i++) {
      int value = values.ints[i];
      if (value == 1) {
        // every N 1's should be encoded as '2'
        if (++onesCounter == n) {
          internalBuffer.ints[internalBuffer.length++] = 2;
          onesCounter = 0;
        }
      } else {
        // there might have been 1's that we need to encode
        while (onesCounter > 0) {
          --onesCounter;
          internalBuffer.ints[internalBuffer.length++] = 1;
        }
        
        // encode value as value+1
        internalBuffer.ints[internalBuffer.length++] = value + 1;
      }
    }
    // there might have been 1's that we need to encode
    while (onesCounter > 0) {
      --onesCounter;
      internalBuffer.ints[internalBuffer.length++] = 1;
    }
    super.encode(internalBuffer, buf);
  }

  @Override
  public IntDecoder createMatchingDecoder() {
    return new NOnesIntDecoder(n);
  }

  @Override
  public String toString() {
    return "NOnes(" + n + ") (" + super.toString() + ")";
  }

}
