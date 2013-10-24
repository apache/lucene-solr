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
 * An {@link IntDecoder} which wraps another decoder and reverts the d-gap that
 * was encoded by {@link DGapIntEncoder}.
 * 
 * @lucene.experimental
 */
public final class DGapIntDecoder extends IntDecoder {

  private final IntDecoder decoder;

  public DGapIntDecoder(IntDecoder decoder) {
    this.decoder = decoder;
  }

  @Override
  public void decode(BytesRef buf, IntsRef values) {
    decoder.decode(buf, values);
    int prev = 0;
    for (int i = 0; i < values.length; i++) {
      values.ints[i] += prev;
      prev = values.ints[i];
    }
  }

  @Override
  public String toString() {
    return "DGap(" + decoder.toString() + ")";
  }

}
