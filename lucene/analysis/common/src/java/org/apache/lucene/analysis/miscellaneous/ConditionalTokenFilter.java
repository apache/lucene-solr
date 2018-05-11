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

package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;
import java.util.function.Function;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.AttributeSource;

/**
 * Allows skipping TokenFilters based on the current set of attributes.
 *
 * To use, implement the {@link #shouldFilter()} method.  If it returns {@code false},
 * then calling {@link #incrementToken()} will use the wrapped TokenFilter to
 * make changes to the tokenstream.  If it returns {@code true}, then the wrapped
 * filter will be skipped
 */
public abstract class ConditionalTokenFilter extends TokenFilter {

  private enum TokenState {
    READING, PREBUFFERING, BUFFERING, DELEGATING
  }

  private final class OneTimeWrapper extends TokenStream {

    public OneTimeWrapper(AttributeSource attributeSource) {
      super(attributeSource);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (state == TokenState.PREBUFFERING) {
        state = TokenState.BUFFERING;
        return true;
      }
      if (state == TokenState.DELEGATING) {
        return false;
      }
      return ConditionalTokenFilter.this.incrementToken();
    }

    @Override
    public void reset() throws IOException {
      // clearing attributes etc is done by the parent stream,
      // so must be avoided here
    }

    @Override
    public void end() throws IOException {
      endCalled = true;
      ConditionalTokenFilter.this.end();
    }
  }

  private final TokenStream delegate;
  private TokenState state = TokenState.READING;
  private boolean lastTokenFiltered;
  private boolean endCalled;

  /**
   * Create a new BypassingTokenFilter
   * @param input         the input TokenStream
   * @param inputFactory  a factory function to create a new instance of the TokenFilter to wrap
   */
  protected ConditionalTokenFilter(TokenStream input, Function<TokenStream, TokenStream> inputFactory) {
    super(input);
    this.delegate = inputFactory.apply(new OneTimeWrapper(this));
  }

  /**
   * Whether or not to execute the wrapped TokenFilter for the current token
   */
  protected abstract boolean shouldFilter() throws IOException;

  @Override
  public void reset() throws IOException {
    super.reset();
    this.delegate.reset();
    this.state = TokenState.READING;
    this.lastTokenFiltered = false;
    this.endCalled = false;
  }

  @Override
  public void end() throws IOException {
    super.end();
    if (endCalled == false && lastTokenFiltered) {
      this.delegate.end();
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.delegate.close();
  }

  @Override
  public final boolean incrementToken() throws IOException {
    while (true) {
      if (state == TokenState.READING) {
        if (input.incrementToken() == false) {
          return false;
        }
        if (shouldFilter()) {
          lastTokenFiltered = true;
          state = TokenState.PREBUFFERING;
          // we determine that the delegate has emitted all the tokens it can at the current
          // position when OneTimeWrapper.incrementToken() is called in DELEGATING state.  To
          // signal this back to the delegate, we return false, so we now need to reset it
          // to ensure that it can continue to emit more tokens
          delegate.reset();
          boolean more = delegate.incrementToken();
          state = TokenState.DELEGATING;
          return more;
        }
        lastTokenFiltered = false;
        return true;
      }
      if (state == TokenState.BUFFERING) {
        return input.incrementToken();
      }
      if (state == TokenState.DELEGATING) {
        clearAttributes();
        if (delegate.incrementToken()) {
          return true;
        }
        // no more cached tokens
        state = TokenState.READING;
      }
    }
  }

}
