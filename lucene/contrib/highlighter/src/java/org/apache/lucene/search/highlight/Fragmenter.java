package org.apache.lucene.search.highlight;
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

import org.apache.lucene.analysis.TokenStream;

/**
 * Implements the policy for breaking text into multiple fragments for
 * consideration by the {@link Highlighter} class. A sophisticated
 * implementation may do this on the basis of detecting end of sentences in the
 * text.
 */
public interface Fragmenter {

  /**
   * Initializes the Fragmenter. You can grab references to the Attributes you are
   * interested in from tokenStream and then access the values in {@link #isNewFragment()}.
   * 
   * @param originalText the original source text
   * @param tokenStream the {@link TokenStream} to be fragmented
   */
  public void start(String originalText, TokenStream tokenStream);


  /**
   * Test to see if this token from the stream should be held in a new
   * TextFragment. Every time this is called, the TokenStream
   * passed to start(String, TokenStream) will have been incremented.
   * 
   */
  public boolean isNewFragment();
}
