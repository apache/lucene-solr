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
package org.apache.lucene.analysis.sinks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.AttributeSource;

/**
 * This TokenFilter provides the ability to set aside attribute states that have already been
 * analyzed. This is useful in situations where multiple fields share many common analysis steps and
 * then go their separate ways.
 *
 * <p>It is also useful for doing things like entity extraction or proper noun analysis as part of
 * the analysis workflow and saving off those tokens for use in another field.
 *
 * <pre class="prettyprint">
 * TeeSinkTokenFilter source1 = new TeeSinkTokenFilter(new WhitespaceTokenizer());
 * TeeSinkTokenFilter.SinkTokenStream sink1 = source1.newSinkTokenStream();
 * TeeSinkTokenFilter.SinkTokenStream sink2 = source1.newSinkTokenStream();
 *
 * TokenStream final1 = new LowerCaseFilter(source1);
 * TokenStream final2 = new EntityDetect(sink1);
 * TokenStream final3 = new URLDetect(sink2);
 *
 * d.add(new TextField("f1", final1));
 * d.add(new TextField("f2", final2));
 * d.add(new TextField("f3", final3));
 * </pre>
 *
 * <p>In this example, {@code sink1} and {@code sink2} will both get tokens from {@code source1}
 * after whitespace tokenization, and will further do additional token filtering, e.g. detect
 * entities and URLs.
 *
 * <p><b>NOTE</b>: it is important, that tees are consumed before sinks, therefore you should add
 * them to the document before the sinks. In the above example, <i>f1</i> is added before the other
 * fields, and so by the time they are processed, it has already been consumed, which is the correct
 * way to index the three streams. If for some reason you cannot ensure that, you should call {@link
 * #consumeAllTokens()} before adding the sinks to document fields.
 */
public final class TeeSinkTokenFilter extends TokenFilter {

  private final States cachedStates = new States();

  public TeeSinkTokenFilter(TokenStream input) {
    super(input);
  }

  /** Returns a new {@link SinkTokenStream} that receives all tokens consumed by this stream. */
  public TokenStream newSinkTokenStream() {
    return new SinkTokenStream(this.cloneAttributes(), cachedStates);
  }

  /**
   * <code>TeeSinkTokenFilter</code> passes all tokens to the added sinks when itself is consumed.
   * To be sure that all tokens from the input stream are passed to the sinks, you can call this
   * methods. This instance is exhausted after this method returns, but all sinks are instant
   * available.
   */
  public void consumeAllTokens() throws IOException {
    while (incrementToken()) {}
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      cachedStates.add(captureState());
      return true;
    }

    return false;
  }

  @Override
  public final void end() throws IOException {
    super.end();
    cachedStates.setFinalState(captureState());
  }

  @Override
  public void reset() throws IOException {
    cachedStates.reset();
    super.reset();
  }

  /** TokenStream output from a tee. */
  public static final class SinkTokenStream extends TokenStream {
    private final States cachedStates;
    private Iterator<AttributeSource.State> it = null;

    private SinkTokenStream(AttributeSource source, States cachedStates) {
      super(source);
      this.cachedStates = cachedStates;
    }

    @Override
    public final boolean incrementToken() {
      if (!it.hasNext()) {
        return false;
      }

      AttributeSource.State state = it.next();
      restoreState(state);
      return true;
    }

    @Override
    public void end() throws IOException {
      State finalState = cachedStates.getFinalState();
      if (finalState != null) {
        restoreState(finalState);
      }
    }

    @Override
    public final void reset() {
      it = cachedStates.getStates();
    }
  }

  /** A convenience wrapper for storing the cached states as well the final state of the stream. */
  private static final class States {

    private final List<State> states = new ArrayList<>();
    private State finalState;

    public States() {}

    void setFinalState(State finalState) {
      this.finalState = finalState;
    }

    State getFinalState() {
      return finalState;
    }

    void add(State state) {
      states.add(state);
    }

    Iterator<State> getStates() {
      return states.iterator();
    }

    void reset() {
      finalState = null;
      states.clear();
    }
  }
}
