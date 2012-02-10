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

import org.apache.lucene.util.AttributeSource;

import java.io.Reader;
import java.io.IOException;

/** A Tokenizer is a TokenStream whose input is a Reader.
  <p>
  This is an abstract class; subclasses must override {@link #incrementToken()}
  <p>
  NOTE: Subclasses overriding {@link #incrementToken()} must
  call {@link AttributeSource#clearAttributes()} before
  setting attributes.
 */
public abstract class Tokenizer extends TokenStream {
  /** The text source for this Tokenizer. */
  protected Reader input;

  /** Construct a token stream processing the given input. */
  protected Tokenizer(Reader input) {
    assert input != null: "input must not be null";
    this.input = input;
  }
  
  /** Construct a token stream processing the given input using the given AttributeFactory. */
  protected Tokenizer(AttributeFactory factory, Reader input) {
    super(factory);
    assert input != null: "input must not be null";
    this.input = input;
  }

  /** Construct a token stream processing the given input using the given AttributeSource. */
  protected Tokenizer(AttributeSource source, Reader input) {
    super(source);
    assert input != null: "input must not be null";
    this.input = input;
  }
  
  /** By default, closes the input Reader. */
  @Override
  public void close() throws IOException {
    if (input != null) {
      input.close();
      // LUCENE-2387: don't hold onto Reader after close, so
      // GC can reclaim
      input = null;
    }
  }
  
  /** Return the corrected offset. If {@link #input} is a {@link CharStream} subclass
   * this method calls {@link CharStream#correctOffset}, else returns <code>currentOff</code>.
   * @param currentOff offset as seen in the output
   * @return corrected offset based on the input
   * @see CharStream#correctOffset
   */
  protected final int correctOffset(int currentOff) {
    assert input != null: "this tokenizer is closed";
    return (input instanceof CharStream) ? ((CharStream) input).correctOffset(currentOff) : currentOff;
  }

  /** Expert: Reset the tokenizer to a new reader.  Typically, an
   *  analyzer (in its tokenStream method) will use
   *  this to re-use a previously created tokenizer. */
  public void reset(Reader input) throws IOException {
    assert input != null: "input must not be null";
    this.input = input;
  }
}

