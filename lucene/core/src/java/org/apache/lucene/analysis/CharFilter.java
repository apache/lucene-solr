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

import java.io.FilterReader;
import java.io.Reader;

/**
 * Subclasses of CharFilter can be chained to filter a Reader
 * They can be used as {@link java.io.Reader} with additional offset
 * correction. {@link Tokenizer}s will automatically use {@link #correctOffset}
 * if a CharFilter subclass is used.
 */
public abstract class CharFilter extends FilterReader {

  /**
   * Create a new CharFilter wrapping the provided reader.
   * @param in a Reader, can also be a CharFilter for chaining.
   */
  public CharFilter(Reader in) {
    super(in);
  }
  
  /**
   * Subclasses override to correct the current offset.
   *
   * @param currentOff current offset
   * @return corrected offset
   */
  protected abstract int correct(int currentOff);
  
  /**
   * Chains the corrected offset through the input
   * CharFilter(s).
   */
  public final int correctOffset(int currentOff) {
    final int corrected = correct(currentOff);
    return (in instanceof CharFilter) ? ((CharFilter) in).correctOffset(corrected) : corrected;
  }
}
