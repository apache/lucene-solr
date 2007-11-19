package org.apache.lucene.analysis;

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

import org.apache.lucene.index.Payload;

import java.io.IOException;

/** A TokenStream enumerates the sequence of tokens, either from
  fields of a document or from query text.
  <p>
  This is an abstract class.  Concrete subclasses are:
  <ul>
  <li>{@link Tokenizer}, a TokenStream
  whose input is a Reader; and
  <li>{@link TokenFilter}, a TokenStream
  whose input is another TokenStream.
  </ul>
  NOTE: subclasses must override at least one of {@link
  #next()} or {@link #next(Token)}.
  */

public abstract class TokenStream {

  /** Returns the next token in the stream, or null at EOS.
   *  The returned Token is a "full private copy" (not
   *  re-used across calls to next()) but will be slower
   *  than calling {@link #next(Token)} instead.. */
  public Token next() throws IOException {
    Token result = next(new Token());

    if (result != null) {
      Payload p = result.getPayload();
      if (p != null)
        result.setPayload(new Payload(p.toByteArray(), 0, p.length()));
    }

    return result;
  }

  /** Returns the next token in the stream, or null at EOS.
   *  When possible, the input Token should be used as the
   *  returned Token (this gives fastest tokenization
   *  performance), but this is not required and a new Token
   *  may be returned.  Callers may re-use a single Token
   *  instance for successive calls to this method and must
   *  therefore fully consume the previously returned Token
   *  before calling this method again.
   *  @param result a Token that may or may not be used to
   *   return
   *  @return next token in the stream or null if
   *   end-of-stream was hit*/
  public Token next(Token result) throws IOException {
    return next();
  }

  /** Resets this stream to the beginning. This is an
   *  optional operation, so subclasses may or may not
   *  implement this method. Reset() is not needed for
   *  the standard indexing process. However, if the Tokens 
   *  of a TokenStream are intended to be consumed more than 
   *  once, it is neccessary to implement reset(). 
   */
  public void reset() throws IOException {}
  
  /** Releases resources associated with this stream. */
  public void close() throws IOException {}
}
