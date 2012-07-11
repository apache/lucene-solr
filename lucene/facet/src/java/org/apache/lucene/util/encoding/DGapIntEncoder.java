package org.apache.lucene.util.encoding;

import java.io.IOException;
import java.io.OutputStream;

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
 * An {@link IntEncoderFilter} which encodes the gap between the given values,
 * rather than the values themselves. This encoder usually yields better
 * encoding performance space-wise (i.e., the final encoded values consume less
 * space) if the values are 'close' to each other.
 * <p>
 * <b>NOTE:</b> this encoder assumes the values are given to
 * {@link #encode(int)} in an ascending sorted manner, which ensures only
 * positive values are encoded and thus yields better performance. If you are
 * not sure whether the values are sorted or not, it is possible to chain this
 * encoder with {@link SortingIntEncoder} to ensure the values will be
 * sorted before encoding.
 * 
 * @lucene.experimental
 */
public class DGapIntEncoder extends IntEncoderFilter {

  private int prev = 0;

  /** Initializes with the given encoder. */
  public DGapIntEncoder(IntEncoder encoder) {
    super(encoder);
  }

  @Override
  public void encode(int value) throws IOException {
    encoder.encode(value - prev);
    prev = value;
  }

  @Override
  public IntDecoder createMatchingDecoder() {
    return new DGapIntDecoder(encoder.createMatchingDecoder());
  }
  
  @Override
  public void reInit(OutputStream out) {
    super.reInit(out);
    prev = 0;
  }

  @Override
  public String toString() {
    return "DGap (" + encoder.toString() + ")";
  }
  
}
