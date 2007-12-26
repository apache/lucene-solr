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

import java.io.Reader;
import java.io.IOException;

/** A Tokenizer is a TokenStream whose input is a Reader.
  <p>
  This is an abstract class.
  NOTE: subclasses must override at least one of {@link
  #next()} or {@link #next(Token)}.
 */

public abstract class Tokenizer extends TokenStream {
  /** The text source for this Tokenizer. */
  protected Reader input;

  /** Construct a tokenizer with null input. */
  protected Tokenizer() {}

  /** Construct a token stream processing the given input. */
  protected Tokenizer(Reader input) {
    this.input = input;
  }

  /** By default, closes the input Reader. */
  public void close() throws IOException {
    input.close();
  }

  /** Expert: Reset the tokenizer to a new reader.  Typically, an
   *  analyzer (in its reusableTokenStream method) will use
   *  this to re-use a previously created tokenizer. */
  public void reset(Reader input) throws IOException {
    this.input = input;
  }
}

