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
package org.apache.lucene.analysis.icu.segmentation;


import com.ibm.icu.text.BreakIterator;

/**
 * Class that allows for tailored Unicode Text Segmentation on
 * a per-writing system basis.
 * @lucene.experimental
 */
public abstract class ICUTokenizerConfig {
  
  /**
   * Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.)
   */
  public ICUTokenizerConfig() {}
  /** Return a breakiterator capable of processing a given script. */
  public abstract BreakIterator getBreakIterator(int script);
  /** Return a token type value for a given script and BreakIterator
   *  rule status. */
  public abstract String getType(int script, int ruleStatus);
  /** true if Han, Hiragana, and Katakana scripts should all be returned as Japanese */
  public abstract boolean combineCJ();
}
