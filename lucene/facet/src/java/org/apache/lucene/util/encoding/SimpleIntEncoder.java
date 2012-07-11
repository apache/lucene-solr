package org.apache.lucene.util.encoding;

import java.io.IOException;

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
 * A simple {@link IntEncoder}, writing an integer as 4 raw bytes. *
 * 
 * @lucene.experimental
 */
public class SimpleIntEncoder extends IntEncoder {

  /**
   * This method makes sure the value wasn't previously encoded by checking
   * against the Set. If the value wasn't encoded, it's added to the Set, and
   * encoded with {#link Vint8#encode}
   * 
   * @param value
   *            an integer to be encoded
   * @throws IOException
   *             possibly thrown by the OutputStream
   */
  @Override
  public void encode(int value) throws IOException {
    out.write(value >>> 24);
    out.write((value >> 16) & 0xFF);
    out.write((value >> 8) & 0xFF);
    out.write(value & 0xFF);
  }

  @Override
  public IntDecoder createMatchingDecoder() {
    return new SimpleIntDecoder();
  }

  @Override
  public String toString() {
    return "Simple";
  }

}
