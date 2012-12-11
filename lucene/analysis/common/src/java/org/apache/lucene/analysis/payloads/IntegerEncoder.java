package org.apache.lucene.analysis.payloads;
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

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;


/**
 *  Encode a character array Integer as a {@link BytesRef}.
 * <p/>
 * See {@link org.apache.lucene.analysis.payloads.PayloadHelper#encodeInt(int, byte[], int)}.
 *
 **/
public class IntegerEncoder extends AbstractEncoder implements PayloadEncoder {

  @Override
  public BytesRef encode(char[] buffer, int offset, int length) {
    int payload = ArrayUtil.parseInt(buffer, offset, length);//TODO: improve this so that we don't have to new Strings
    byte[] bytes = PayloadHelper.encodeInt(payload);
    BytesRef result = new BytesRef(bytes);
    return result;
  }
}