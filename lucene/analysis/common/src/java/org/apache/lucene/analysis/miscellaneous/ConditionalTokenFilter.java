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
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.AttributeSource;

/**
 * Allows skipping TokenFilters based on the current set of attributes.
 *
 * <p>To use, implement the {@link #shouldFilter()} method. If it returns {@code true}, then calling
 * {@link #incrementToken()} will use the wrapped TokenFilter(s) to make changes to the tokenstream.
 * If it returns {@code false}, then the wrapped filter(s) will be skipped.
 */
public abstract class ConditionalTokenFilter extends TokenFilter {

  private enum TokenState {
    READING,
    PREBUFFERING,
    DELEGATING
  }

  private final class OneTimeWrapper extends TokenStream {

    private final OffsetAttribute offsetAtt;
    private final PositionIncrementAttribute posIncAtt;

    public OneTimeWrapper(AttributeSource attributeSource) {
      super(attributeSource);
      this.offsetAtt = attributeSource.addAttribute(OffsetAttribute.class);
      this.posIncAtt = attributeSource.addAttribute(PositionIncrementAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (state == TokenState.PREBUFFERING) {
        if (posIncAtt.getPositionIncrement() == 0) {
          adjustPosition = true;
          posIncAtt.setPositionIncrement(1);
        }
        state = TokenState.DELEGATING;
        return true;
      }
      assert state == TokenState.DELEGATING;
      if (input.incrementToken()) {
        if (shouldFilter()) {
          return true;
        }
        endOffset = offsetAtt.endOffset();
        bufferedState = captureState();
      } else {
        exhausted = true;
      }
      return false;
    }

    @Override
    public void reset() throws IOException {
      // clearing attributes etc is done by the parent stream,
      // so must be avoided here
    }

    @Override
    public void end() throws IOException {
      // imitate Tokenizer.end() call - endAttributes, set final offset
      if (exhausted) {
        if (endState == null) {
          input.end();
          endState = captureState();
        }
        endOffset = offsetAtt.endOffset();
      }
      endAttributes();
      offsetAtt.setOffset(endOffset, endOffset);
    }
  }

  private final TokenStream delegate;
  private TokenState state = TokenState.READING;
  private boolean lastTokenFiltered;
  private State bufferedState = null;
  private boolean exhausted;
  private boolean adjustPosition;
  private State endState = null;
  private int endOffset;

  private final PositionIncrementAttribute posIncAtt =
      addAttribute(PositionIncrementAttribute.class);

  /**
   * Create a new ConditionalTokenFilter
   *
   * @param input the input TokenStream
   * @param inputFactory a factory function to create the wrapped filter(s)
   */
  protected ConditionalTokenFilter(
      TokenStream input, Function<TokenStream, TokenStream> inputFactory) {
    super(input);
    this.delegate = inputFactory.apply(new OneTimeWrapper(this.input));
  }

  /** Whether or not to execute the wrapped TokenFilter(s) for the current token */
  protected abstract boolean shouldFilter() throws IOException;

  @Override
  public void reset() throws IOException {
    super.reset();
    this.delegate.reset();
    this.state = TokenState.READING;
    this.lastTokenFiltered = false;
    this.bufferedState = null;
    this.exhausted = false;
    this.adjustPosition = false;
    this.endOffset = -1;
    this.endState = null;
  }

  @Override
  public void end() throws IOException {
    if (endState == null) {
      super.end();
      endState = captureState();
    } else {
      restoreState(endState);
    }
    endOffset = getAttribute(OffsetAttribute.class).endOffset();
    if (lastTokenFiltered) {
      this.delegate.end();
      endState = captureState();
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.delegate.close();
  }

  @Override
  public final boolean incrementToken() throws IOException {
    lastTokenFiltered = false;
    while (true) {
      if (state == TokenState.READING) {
        if (bufferedState != null) {
          restoreState(bufferedState);
          bufferedState = null;
          lastTokenFiltered = false;
          return true;
        }
        if (exhausted == true) {
          return false;
        }
        if (input.incrementToken() == false) {
          exhausted = true;
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
          if (more) {
            state = TokenState.DELEGATING;
            if (adjustPosition) {
              int posInc = posIncAtt.getPositionIncrement();
              posIncAtt.setPositionIncrement(posInc - 1);
            }
            adjustPosition = false;
          } else {
            state = TokenState.READING;
            return endDelegating();
          }
          return true;
        }
        return true;
      }
      if (state == TokenState.DELEGATING) {
        lastTokenFiltered = true;
        if (delegate.incrementToken()) {
          return true;
        }
        // no more cached tokens
        state = TokenState.READING;
        return endDelegating();
      }
    }
  }

  private boolean endDelegating() throws IOException {
    if (bufferedState == null) {
      assert exhausted == true;
      return false;
    }
    delegate.end();
    int posInc = posIncAtt.getPositionIncrement();
    restoreState(bufferedState);
    // System.out.println("Buffered posInc: " + posIncAtt.getPositionIncrement() + "   Delegated
    // posInc: " + posInc);
    posIncAtt.setPositionIncrement(posIncAtt.getPositionIncrement() + posInc);
    if (adjustPosition) {
      posIncAtt.setPositionIncrement(posIncAtt.getPositionIncrement() - 1);
      adjustPosition = false;
    }
    bufferedState = null;
    return true;
  }
}
