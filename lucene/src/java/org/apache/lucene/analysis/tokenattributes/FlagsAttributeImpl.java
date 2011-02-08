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

import org.apache.lucene.util.AttributeImpl;

/**
 * This attribute can be used to pass different flags down the tokenizer chain,
 * eg from one TokenFilter to another one. 
 * @lucene.experimental While we think this is here to stay, we may want to change it to be a long.
 */
public class FlagsAttributeImpl extends AttributeImpl implements FlagsAttribute, Cloneable {
  private int flags = 0;
  
  /**
   * <p/>
   *
   * Get the bitset for any bits that have been set.  This is completely distinct from {@link TypeAttribute#type()}, although they do share similar purposes.
   * The flags can be used to encode information about the token for use by other {@link org.apache.lucene.analysis.TokenFilter}s.
   *
   *
   * @return The bits
   */
  public int getFlags() {
    return flags;
  }

  /**
   * @see #getFlags()
   */
  public void setFlags(int flags) {
    this.flags = flags;
  }
  
  @Override
  public void clear() {
    flags = 0;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    
    if (other instanceof FlagsAttributeImpl) {
      return ((FlagsAttributeImpl) other).flags == flags;
    }
    
    return false;
  }

  @Override
  public int hashCode() {
    return flags;
  }
  
  @Override
  public void copyTo(AttributeImpl target) {
    FlagsAttribute t = (FlagsAttribute) target;
    t.setFlags(flags);
  }
}
