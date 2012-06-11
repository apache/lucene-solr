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
 * An {@link IntDecoder} which can decode values encoded by
 * {@link VInt8IntEncoder}.
 * 
 * @lucene.experimental
 */
public class VInt8IntDecoder extends IntDecoder {

  private boolean legalEOS = true;

  @Override
  public long decode() throws IOException {
    int value = 0;
    while (true) {
      int first = in.read();
      if (first < 0) {
        if (!legalEOS) {
          throw new IOException("Unexpected End-Of-Stream");
        }
        return EOS;
      }
      value |= first & 0x7F;
      if ((first & 0x80) == 0) {
        legalEOS = true;
        return value;
      }
      legalEOS = false;
      value <<= 7;
    }
  }

  @Override
  public String toString() {
    return "VInt8";
  }

} 
