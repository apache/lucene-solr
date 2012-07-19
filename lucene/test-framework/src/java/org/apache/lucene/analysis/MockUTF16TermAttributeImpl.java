package org.apache.lucene.analysis;

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

import java.nio.charset.Charset;

import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;
import org.apache.lucene.util.BytesRef;

/**
 * Extension of {@link CharTermAttributeImpl} that encodes the term
 * text as UTF-16 bytes instead of as UTF-8 bytes.
 */
public class MockUTF16TermAttributeImpl extends CharTermAttributeImpl {
  static final Charset charset = Charset.forName("UTF-16LE");
  
  @Override
  public int fillBytesRef() {
    BytesRef bytes = getBytesRef();
    byte[] utf16 = toString().getBytes(charset);
    bytes.bytes = utf16;
    bytes.offset = 0;
    bytes.length = utf16.length;
    return bytes.hashCode();
  }
}
