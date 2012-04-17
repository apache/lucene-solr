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


/**
 * Mainly for use with the DelimitedPayloadTokenFilter, converts char buffers to Payload.
 * <p/>
 * NOTE: This interface is subject to change 
 *
 **/
public interface PayloadEncoder {

  Payload encode(char[] buffer);

  /**
   * Convert a char array to a {@link org.apache.lucene.index.Payload}
   * @param buffer
   * @param offset
   * @param length
   * @return encoded {@link Payload}
   */
  Payload encode(char [] buffer, int offset, int length);
}
