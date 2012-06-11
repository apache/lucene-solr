package org.apache.lucene.util.encoding;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

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
 * An {@link IntEncoderFilter} which sorts the values to encode in ascending
 * order before encoding them. Encoding therefore happens upon calling
 * {@link #close()}. Since this encoder is usually chained with another encoder
 * that relies on sorted values, it does not offer a default constructor.
 * 
 * @lucene.experimental
 */
public class SortingIntEncoder extends IntEncoderFilter {

  private float grow = 2.0f;
  private int index = 0;
  private int[] set = new int[1024];

  /** Initializes with the given encoder. */
  public SortingIntEncoder(IntEncoder encoder) {
    super(encoder);
  }

  @Override
  public void close() throws IOException {
    if (index == 0) {
      return;
    }

    Arrays.sort(set, 0, index);
    for (int i = 0; i < index; i++) {
      encoder.encode(set[i]);
    }
    encoder.close();
    index = 0;

    super.close();
  }

  @Override
  public void encode(int value) throws IOException {
    if (index == set.length) {
      int[] newSet = new int[(int) (set.length * grow)];
      System.arraycopy(set, 0, newSet, 0, set.length);
      set = newSet;
    }
    set[index++] = value;
  }

  @Override
  public IntDecoder createMatchingDecoder() {
    return encoder.createMatchingDecoder();
  }
  
  @Override
  public void reInit(OutputStream out) {
    super.reInit(out);
    index = 0;
  }

  @Override
  public String toString() {
    return "Sorting (" + encoder.toString() + ")";
  }
  
}
