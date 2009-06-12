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

import java.nio.charset.Charset;
import java.util.Arrays;
import java.io.UnsupportedEncodingException;


/**
 *  Does nothing other than convert the char array to a byte array using the specified encoding.
 *
 **/
public class IdentityEncoder extends AbstractEncoder implements PayloadEncoder{

  protected Charset charset = Charset.forName("UTF-8");
  protected String charsetName = "UTF-8";  //argh, stupid 1.4

  public IdentityEncoder() {
  }

  public IdentityEncoder(Charset charset) {
    this.charset = charset;
    charsetName = charset.name();
  }


  public Payload encode(char[] buffer, int offset, int length) {
    //what's the most efficient way to get a byte [] from a char[] array
    //Do we have to go through String?
    String tmp = new String(buffer, offset, length);
    Payload result = null;//Can we avoid allocating by knowing where using the new API?
    try {
      result = new Payload(tmp.getBytes(charsetName));
    } catch (UnsupportedEncodingException e) {
      //should never hit this, since we get the name from the Charset
    }

    return result;
  }
}
