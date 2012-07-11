package org.apache.lucene.util.encoding;

import java.io.IOException;
import java.io.StreamCorruptedException;

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
 * A simple stream decoder which can decode values encoded with
 * {@link SimpleIntEncoder}.
 * 
 * @lucene.experimental
 */
public class SimpleIntDecoder extends IntDecoder {

  /**
   * reusable buffer - allocated only once as this is not a thread-safe object
   */
  private byte[] buffer = new byte[4];

  @Override
  public long decode() throws IOException {

    // we need exactly 4 bytes to decode an int in this decoder impl, otherwise, throw an exception
    int offset = 0;
    while (offset < 4) {
      int nRead = in.read(buffer, offset, 4 - offset);
      if (nRead == -1) {
        if (offset > 0) {
          throw new StreamCorruptedException(
              "Need 4 bytes for decoding an int, got only " + offset);
        }
        return EOS;
      }
      offset += nRead;
    }

    int v = buffer[3] & 0xff;
    v |= (buffer[2] << 8) & 0xff00;
    v |= (buffer[1] << 16) & 0xff0000;
    v |= (buffer[0] << 24) & 0xff000000;

    return v;
  }

  @Override
  public String toString() {
    return "Simple";
  }

}
