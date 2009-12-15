package org.apache.lucene.analysis.payloads;
/**
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

import org.apache.lucene.index.Payload;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;


/**
 *  Does nothing other than convert the char array to a byte array using the specified encoding.
 *
 **/
public class IdentityEncoder extends AbstractEncoder implements PayloadEncoder{

  protected Charset charset = Charset.forName("UTF-8");
  
  /** @deprecated This field is no longer used. Use {@link #charset} instead. */
  @Deprecated
  protected String charsetName = charset.name();

  public IdentityEncoder() {
  }

  public IdentityEncoder(Charset charset) {
    this.charset = charset;
    // @deprecated, remove this in 4.0:
    charsetName = charset.name();
  }


  public Payload encode(char[] buffer, int offset, int length) {
    final ByteBuffer bb = charset.encode(CharBuffer.wrap(buffer, offset, length));
    if (bb.hasArray()) {
      return new Payload(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
    } else {
      // normally it should always have an array, but who knows?
      final byte[] b = new byte[bb.remaining()];
      bb.get(b);
      return new Payload(b);
    }
  }
}
