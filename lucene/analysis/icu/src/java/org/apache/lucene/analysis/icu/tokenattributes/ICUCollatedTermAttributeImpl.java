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
package org.apache.lucene.analysis.icu.tokenattributes;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RawCollationKey;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;
import org.apache.lucene.util.BytesRef;

/**
 * Extension of {@link CharTermAttributeImpl} that encodes the term text as a binary Unicode
 * collation key instead of as UTF-8 bytes.
 */
public class ICUCollatedTermAttributeImpl extends CharTermAttributeImpl {
  private final Collator collator;
  private final RawCollationKey key = new RawCollationKey();

  /**
   * Create a new ICUCollatedTermAttributeImpl
   *
   * @param collator Collation key generator
   */
  public ICUCollatedTermAttributeImpl(Collator collator) {
    // clone the collator: see http://userguide.icu-project.org/collation/architecture
    try {
      this.collator = (Collator) collator.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public BytesRef getBytesRef() {
    collator.getRawCollationKey(toString(), key);
    final BytesRef ref = this.builder.get();
    ref.bytes = key.bytes;
    ref.offset = 0;
    ref.length = key.size;
    return ref;
  }
}
