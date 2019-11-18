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

package org.apache.lucene.codecs.uniformsplit;

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;

/**
 * Supplier for a new stateful {@link IndexDictionary.Browser} created on
 * the immutable {@link IndexDictionary}.
 * <p>
 * The immutable {@link IndexDictionary} is lazy loaded thread safely. This
 * lazy loading allows us to load it only when {@link TermsEnum#seekCeil}
 * or {@link TermsEnum#seekExact} are called (it is not loaded for a direct
 * all-terms enumeration).
 *
 * @lucene.experimental
 */
public class DictionaryBrowserSupplier implements Supplier<IndexDictionary.Browser>, Accountable {

  protected final IndexInput dictionaryInput;
  protected final BlockDecoder blockDecoder;

  /**
   * Lazy loaded immutable index dictionary (trie hold in RAM).
   */
  protected IndexDictionary dictionary;

  public DictionaryBrowserSupplier(IndexInput dictionaryInput, long startFilePointer, BlockDecoder blockDecoder) throws IOException {
    this.dictionaryInput = dictionaryInput.clone();
    this.dictionaryInput.seek(startFilePointer);
    this.blockDecoder = blockDecoder;
  }

  /**
   * Gets or lazy loads the immutable {@link IndexDictionary} thread safely
   * and creates a new {@link IndexDictionary.Browser}.
   */
  @Override
  public IndexDictionary.Browser get() {
    // This double-check idiom does not require the dictionary to be volatile
    // because it is immutable. See section "Double-Checked Locking Immutable Objects"
    // of https://www.cs.umd.edu/~pugh/java/memoryModel/DoubleCheckedLocking.html.
    if (dictionary == null) {
      synchronized (this) {
        try {
          if (dictionary == null) {
            dictionary = FSTDictionary.read(dictionaryInput, blockDecoder);
          }
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    return dictionary.browser();
  }

  @Override
  public long ramBytesUsed() {
    return dictionary == null ? 0 : dictionary.ramBytesUsed();
  }
}
