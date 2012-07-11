package org.apache.lucene.analysis.hunspell;

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

import java.util.Arrays;

/**
 * A dictionary (.dic) entry with its associated flags.
 */
public class HunspellWord {
  
  private final char flags[]; // sorted, can we represent more concisely?

  /**
   * Creates a new HunspellWord with no associated flags
   */
  public HunspellWord() {
    flags = null;
  }

  /**
   * Constructs a new HunspellWord with the given flags
   *
   * @param flags Flags to associate with the word
   */
  public HunspellWord(char[] flags) {
    this.flags = flags;
  }

  /**
   * Checks whether the word has the given flag associated with it
   *
   * @param flag Flag to check whether it is associated with the word
   * @return {@code true} if the flag is associated, {@code false} otherwise
   */
  public boolean hasFlag(char flag) {
    return flags != null && Arrays.binarySearch(flags, flag) >= 0;
  }

  /**
   * Returns the flags associated with the word
   *
   * @return Flags associated with the word
   */
  public char[] getFlags() {
    return flags;
  }
}
