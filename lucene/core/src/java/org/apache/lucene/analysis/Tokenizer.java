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

import org.apache.lucene.util.AttributeFactory;
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
  protected Reader input = ILLEGAL_STATE_READER;
  
  /** Pending reader: not actually assigned to input until reset() */
  private Reader inputPending = ILLEGAL_STATE_READER;

  /** Construct a token stream processing the given input. */
  protected Tokenizer(Reader input) {
    if (input == null) {
      throw new NullPointerException("input must not be null");
    }
    this.inputPending = input;
  }
  
  /** Construct a token stream processing the given input using the given AttributeFactory. */
  protected Tokenizer(AttributeFactory factory, Reader input) {
    super(factory);
    if (input == null) {
      throw new NullPointerException("input must not be null");
    }
    this.inputPending = input;
  }

  /**
   * {@inheritDoc}
   * <p>
   * <b>NOTE:</b> 
   * The default implementation closes the input Reader, so
   * be sure to call <code>super.close()</code> when overriding this method.
   */
  @Override
  public void close() throws IOException {
    input.close();
    // LUCENE-2387: don't hold onto Reader after close, so
    // GC can reclaim
    inputPending = input = ILLEGAL_STATE_READER;
  }
  
  /** Return the corrected offset. If {@link #input} is a {@link CharFilter} subclass
   * this method calls {@link CharFilter#correctOffset}, else returns <code>currentOff</code>.
   * @param currentOff offset as seen in the output
   * @return corrected offset based on the input
   * @see CharFilter#correctOffset
   */
  protected final int correctOffset(int currentOff) {
    return (input instanceof CharFilter) ? ((CharFilter) input).correctOffset(currentOff) : currentOff;
  }

  /** Expert: Set a new reader on the Tokenizer.  Typically, an
   *  analyzer (in its tokenStream method) will use
   *  this to re-use a previously created tokenizer. */
  public final void setReader(Reader input) throws IOException {
    if (input == null) {
      throw new NullPointerException("input must not be null");
    } else if (this.input != ILLEGAL_STATE_READER) {
      throw new IllegalStateException("TokenStream contract violation: close() call missing");
    }
    this.inputPending = input;
    assert setReaderTestPoint();
  }
  
  @Override
  public void reset() throws IOException {
    super.reset();
    input = inputPending;
    inputPending = ILLEGAL_STATE_READER;
  }

  // only used by assert, for testing
  boolean setReaderTestPoint() {
    return true;
  }
  
  private static final Reader ILLEGAL_STATE_READER = new Reader() {
    @Override
    public int read(char[] cbuf, int off, int len) {
      throw new IllegalStateException("TokenStream contract violation: reset()/close() call missing, " +
          "reset() called multiple times, or subclass does not call super.reset(). " +
          "Please see Javadocs of TokenStream class for more information about the correct consuming workflow.");
    }

    @Override
    public void close() {} 
  };
}

