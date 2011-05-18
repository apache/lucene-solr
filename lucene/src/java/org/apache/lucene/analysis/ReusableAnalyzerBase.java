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
import java.io.Reader;

/**
 * An convenience subclass of Analyzer that makes it easy to implement
 * {@link TokenStream} reuse.
 * <p>
 * ReusableAnalyzerBase is a simplification of Analyzer that supports easy reuse
 * for the most common use-cases. Analyzers such as
 * {@link PerFieldAnalyzerWrapper} that behave differently depending upon the
 * field name need to subclass Analyzer directly instead.
 * </p>
 * <p>
 * To prevent consistency problems, this class does not allow subclasses to
 * extend {@link #reusableTokenStream(String, Reader)} or
 * {@link #tokenStream(String, Reader)} directly. Instead, subclasses must
 * implement {@link #createComponents(String, Reader)}.
 * </p>
 */
public abstract class ReusableAnalyzerBase extends Analyzer {

  /**
   * Creates a new {@link TokenStreamComponents} instance for this analyzer.
   * 
   * @param fieldName
   *          the name of the fields content passed to the
   *          {@link TokenStreamComponents} sink as a reader
   * @param aReader
   *          the reader passed to the {@link Tokenizer} constructor
   * @return the {@link TokenStreamComponents} for this analyzer.
   */
  protected abstract TokenStreamComponents createComponents(String fieldName,
      Reader aReader);

  /**
   * This method uses {@link #createComponents(String, Reader)} to obtain an
   * instance of {@link TokenStreamComponents}. It returns the sink of the
   * components and stores the components internally. Subsequent calls to this
   * method will reuse the previously stored components if and only if the
   * {@link TokenStreamComponents#reset(Reader)} method returned
   * <code>true</code>. Otherwise a new instance of
   * {@link TokenStreamComponents} is created.
   * 
   * @param fieldName the name of the field the created TokenStream is used for
   * @param reader the reader the streams source reads from
   */
  @Override
  public final TokenStream reusableTokenStream(final String fieldName,
      final Reader reader) throws IOException {
    TokenStreamComponents streamChain = (TokenStreamComponents)
    getPreviousTokenStream();
    final Reader r = initReader(reader);
    if (streamChain == null || !streamChain.reset(r)) {
      streamChain = createComponents(fieldName, r);
      setPreviousTokenStream(streamChain);
    }
    return streamChain.getTokenStream();
  }

  /**
   * This method uses {@link #createComponents(String, Reader)} to obtain an
   * instance of {@link TokenStreamComponents} and returns the sink of the
   * components. Each calls to this method will create a new instance of
   * {@link TokenStreamComponents}. Created {@link TokenStream} instances are 
   * never reused.
   * 
   * @param fieldName the name of the field the created TokenStream is used for
   * @param reader the reader the streams source reads from
   */
  @Override
  public final TokenStream tokenStream(final String fieldName,
      final Reader reader) {
    return createComponents(fieldName, initReader(reader)).getTokenStream();
  }
  
  /**
   * Override this if you want to add a CharFilter chain.
   */
  protected Reader initReader(Reader reader) {
    return reader;
  }
  
  /**
   * This class encapsulates the outer components of a token stream. It provides
   * access to the source ({@link Tokenizer}) and the outer end (sink), an
   * instance of {@link TokenFilter} which also serves as the
   * {@link TokenStream} returned by
   * {@link Analyzer#tokenStream(String, Reader)} and
   * {@link Analyzer#reusableTokenStream(String, Reader)}.
   */
  public static class TokenStreamComponents {
    protected final Tokenizer source;
    protected final TokenStream sink;

    /**
     * Creates a new {@link TokenStreamComponents} instance.
     * 
     * @param source
     *          the analyzer's tokenizer
     * @param result
     *          the analyzer's resulting token stream
     */
    public TokenStreamComponents(final Tokenizer source,
        final TokenStream result) {
      this.source = source;
      this.sink = result;
    }
    
    /**
     * Creates a new {@link TokenStreamComponents} instance.
     * 
     * @param source
     *          the analyzer's tokenizer
     */
    public TokenStreamComponents(final Tokenizer source) {
      this.source = source;
      this.sink = source;
    }

    /**
     * Resets the encapsulated components with the given reader. This method by
     * default returns <code>true</code> indicating that the components have
     * been reset successfully. Subclasses of {@link ReusableAnalyzerBase} might use
     * their own {@link TokenStreamComponents} returning <code>false</code> if
     * the components cannot be reset.
     * 
     * @param reader
     *          a reader to reset the source component
     * @return <code>true</code> if the components were reset, otherwise
     *         <code>false</code>
     * @throws IOException
     *           if the component's reset method throws an {@link IOException}
     */
    protected boolean reset(final Reader reader) throws IOException {
      source.reset(reader);
      return true;
    }

    /**
     * Returns the sink {@link TokenStream}
     * 
     * @return the sink {@link TokenStream}
     */
    protected TokenStream getTokenStream() {
      return sink;
    }

  }

}
