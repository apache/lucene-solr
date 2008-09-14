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

package org.apache.lucene.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * A SinkTokenizer can be used to cache Tokens for use in an Analyzer
 *
 * @see TeeTokenFilter
 *
 **/
public class SinkTokenizer extends Tokenizer {
  protected List/*<Token>*/ lst = new ArrayList/*<Token>*/();
  protected Iterator/*<Token>*/ iter;

  public SinkTokenizer(List/*<Token>*/ input) {
    this.lst = input;
    if (this.lst == null) this.lst = new ArrayList/*<Token>*/();
  }

  public SinkTokenizer() {
    this.lst = new ArrayList/*<Token>*/();
  }

  public SinkTokenizer(int initCap){
    this.lst = new ArrayList/*<Token>*/(initCap);
  }

  /**
   * Get the tokens in the internal List.
   * <p/>
   * WARNING: Adding tokens to this list requires the {@link #reset()} method to be called in order for them
   * to be made available.  Also, this Tokenizer does nothing to protect against {@link java.util.ConcurrentModificationException}s
   * in the case of adds happening while {@link #next(org.apache.lucene.analysis.Token)} is being called.
   * <p/>
   * WARNING: Since this SinkTokenizer can be reset and the cached tokens made available again, do not modify them. Modify clones instead.
   *
   * @return A List of {@link org.apache.lucene.analysis.Token}s
   */
  public List/*<Token>*/ getTokens() {
    return lst;
  }

  /**
   * Returns the next token out of the list of cached tokens
   * @return The next {@link org.apache.lucene.analysis.Token} in the Sink.
   * @throws IOException
   */
  public Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    if (iter == null) iter = lst.iterator();
    // Since this TokenStream can be reset we have to maintain the tokens as immutable
    if (iter.hasNext()) {
      Token nextToken = (Token) iter.next();
      return (Token) nextToken.clone();
    }
    return null;
  }



  /**
   * Override this method to cache only certain tokens, or new tokens based
   * on the old tokens.
   *
   * @param t The {@link org.apache.lucene.analysis.Token} to add to the sink
   */
  public void add(Token t) {
    if (t == null) return;
    lst.add((Token) t.clone());
  }

  public void close() throws IOException {
    //nothing to close
    input = null;
    lst = null;
  }

  /**
   * Reset the internal data structures to the start at the front of the list of tokens.  Should be called
   * if tokens were added to the list after an invocation of {@link #next(Token)}
   * @throws IOException
   */
  public void reset() throws IOException {
    iter = lst.iterator();
  }
}

