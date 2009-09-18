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

package org.apache.solr.analysis;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenFilter;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Handles input and output buffering of TokenStream
 *
 * <pre>
 * // Example of a class implementing the rule "A" "B" => "Q" "B"
 * class MyTokenStream extends BufferedTokenStream {
 *   public MyTokenStream(TokenStream input) {super(input);}
 *   protected Token process(Token t) throws IOException {
 *     if ("A".equals(t.termText())) {
 *       Token t2 = read();
 *       if (t2!=null && "B".equals(t2.termText())) t.setTermText("Q");
 *       if (t2!=null) pushBack(t2);
 *     }
 *     return t;
 *   }
 * }
 *
 * // Example of a class implementing "A" "B" => "A" "A" "B"
 * class MyTokenStream extends BufferedTokenStream {
 *   public MyTokenStream(TokenStream input) {super(input);}
 *   protected Token process(Token t) throws IOException {
 *     if ("A".equals(t.termText()) && "B".equals(peek(1).termText()))
 *       write(t);
 *     return t;
 *   }
 * }
 * </pre>
 *
 *
 * @version $Id$
 */
public abstract class BufferedTokenStream extends TokenFilter {
  // in the future, might be faster if we implemented as an array based CircularQueue
  private final LinkedList<Token> inQueue = new LinkedList<Token>();
  private final LinkedList<Token> outQueue = new LinkedList<Token>();

  public BufferedTokenStream(TokenStream input) {
    super(input);
  }

  /**
   * Process a token.  Subclasses may read more tokens from the input stream,
   * write more tokens to the output stream, or simply return the next token
   * to be output.  Subclasses may return null if the token is to be dropped.
   * If a subclass writes tokens to the output stream and returns a
   * non-null Token, the returned Token is considered to be at the head of
   * the token output stream.
   */
  protected abstract Token process(Token t) throws IOException;

  public final Token next() throws IOException {
    while (true) {
      if (!outQueue.isEmpty()) return outQueue.removeFirst();
      Token t = read();
      if (null == t) return null;
      Token out = process(t);
      if (null != out) return out;
      // loop back to top in case process() put something on the output queue
    }
  }

  /**
   * Read a token from the buffered input stream.  
   * @return null at EOS
   */
  protected Token read() throws IOException {
    if (inQueue.isEmpty()) {
      Token t = input.next();
      return t;
    }
    return inQueue.removeFirst();
  }

  /**
   * Push a token back into the buffered input stream, such that it will
   * be returned by a future call to <code>read()</code>
   */
  protected void pushBack(Token t) {
    inQueue.addFirst(t);
  }

  /**
   * Peek n tokens ahead in the buffered input stream, without modifying
   * the stream. 
   * @param n Number of tokens into the input stream to peek, 1 based ...
   *          0 is invalid
   * @return a Token which exists in the input stream, any modifications
   *         made to this Token will be "real" if/when the Token is
   *         <code>read()</code> from the stream.
   */
  protected Token peek(int n) throws IOException {
    int fillCount = n-inQueue.size();
    for (int i=0; i < fillCount; i++) {
      Token t = input.next();
      if (null==t) return null;
      inQueue.addLast(t);
    }
    return inQueue.get(n-1);
  }

  /**
   * Write a token to the buffered output stream
   */
  protected void write(Token t) {
    outQueue.addLast(t);
  }

  /**
   * Provides direct Iterator access to the buffered output stream.
   * Modifying any token in this Iterator will affect the resulting stream.
   */
  protected Iterable<Token> output() {
    return outQueue;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    inQueue.clear();
    outQueue.clear();
  }

} 
