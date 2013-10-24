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
 * Encodes integers to a set {@link BytesRef}. For convenience, each encoder
 * implements {@link #createMatchingDecoder()} for easy access to the matching
 * decoder.
 * 
 * @lucene.experimental
 */
public abstract class IntEncoder {

  public IntEncoder() {}

  /**
   * Encodes the values to the given buffer. Note that the buffer's offset and
   * length are set to 0.
   */
  public abstract void encode(IntsRef values, BytesRef buf);

  /**
   * Returns an {@link IntDecoder} which can decode the values that were encoded
   * with this encoder.
   */
  public abstract IntDecoder createMatchingDecoder();
  
}
