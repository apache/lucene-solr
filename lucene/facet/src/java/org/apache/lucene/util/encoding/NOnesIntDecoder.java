package org.apache.lucene.util.encoding;

import java.io.IOException;
import java.io.InputStream;

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
 * Decodes data which was encoded by {@link NOnesIntEncoder}. Uses a
 * {@link FourFlagsIntDecoder} to perform the actual encoding and translates the
 * values back as described in {@link NOnesIntEncoder}.
 * 
 * @see NOnesIntEncoder
 * @lucene.experimental
 */
public class NOnesIntDecoder extends FourFlagsIntDecoder {

  /** Number of consecutive '1's to generate upon decoding a '2'. */
  private int n;

  private int onesCounter;

  /**
   * Constructs a decoder with a given N (Number of consecutive '1's which are
   * translated into a single target value '2'.
   */
  public NOnesIntDecoder(int n) {
    this.n = n;
  }

  @Override
  public long decode() throws IOException {
    // If we read '2', we should return n '1's.
    if (onesCounter > 0) {
      --onesCounter;
      return 1;
    }

    long decode = super.decode();
    if (decode == 1) {
      return 1;
    }
    if (decode == 2) {
      onesCounter = n - 1;
      return 1;
    }
    if (decode == 3) {
      return 2;
    }
    return decode == EOS ? EOS : decode - 1;
  }

  @Override
  public void reInit(InputStream in) {
    super.reInit(in);
    onesCounter = 0;
  }

  @Override
  public String toString() {
    return "NOnes (" + n + ") (" + super.toString() + ")";
  }

}
