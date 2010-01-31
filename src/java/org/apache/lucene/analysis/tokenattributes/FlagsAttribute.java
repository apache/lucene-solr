package org.apache.lucene.analysis.tokenattributes;

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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.Attribute;

/**
 * This attribute can be used to pass different flags down the {@link Tokenizer} chain,
 * eg from one TokenFilter to another one. 
 * @lucene.experimental While we think this is here to stay, we may want to change it to be a long.
 */
public interface FlagsAttribute extends Attribute {
  /**
   * <p/>
   *
   * Get the bitset for any bits that have been set.  This is completely distinct from {@link TypeAttribute#type()}, although they do share similar purposes.
   * The flags can be used to encode information about the token for use by other {@link org.apache.lucene.analysis.TokenFilter}s.
   *
   *
   * @return The bits
   */
  public int getFlags();

  /**
   * @see #getFlags()
   */
  public void setFlags(int flags);  
}
