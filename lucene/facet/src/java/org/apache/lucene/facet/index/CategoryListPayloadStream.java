package org.apache.lucene.facet.index;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.lucene.util.encoding.IntEncoder;

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
 * Accumulates category IDs for a single document, for writing in byte array
 * form, for example, to a Lucene payload.
 * 
 * @lucene.experimental
 */
public class CategoryListPayloadStream {

  private ByteArrayOutputStream baos = new ByteArrayOutputStream(50);
  private IntEncoder encoder;

  /** Creates a payload stream using the specified encoder. */
  public CategoryListPayloadStream(IntEncoder encoder) {
    this.encoder = encoder;
    this.encoder.reInit(baos);
  }

  /** Appends an integer to the stream. */
  public void appendIntToStream(int intValue) throws IOException {
    encoder.encode(intValue);
  }

  /** Returns the streamed bytes so far accumulated, as an array of bytes. */
  public byte[] convertStreamToByteArray() {
    try {
      encoder.close();
      return baos.toByteArray();
    } catch (IOException e) {
      // This cannot happen, because of BAOS (no I/O).
      return new byte[0];
    }
  }

  /** Resets this stream to begin building a new payload. */
  public void reset() throws IOException {
    encoder.close();
    baos.reset();
    encoder.reInit(baos);
  }

}
