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

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.CloseableThreadLocal;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * An convenience subclass of Analyzer that makes it easy to implement
 * {@link TokenStream} reuse.
 * <p>
 * ReusableAnalyzerBase is a simplification of Analyzer that supports easy reuse
 * for the most common use-cases. Analyzers such as
 * PerFieldAnalyzerWrapper that behave differently depending upon the
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

  private final ReuseStrategy reuseStrategy;

  public ReusableAnalyzerBase() {
    this(new GlobalReuseStrategy());
  }

  public ReusableAnalyzerBase(ReuseStrategy reuseStrategy) {
    this.reuseStrategy = reuseStrategy;
  }

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
    TokenStreamComponents components = reuseStrategy.getReusableComponents(fieldName);
    final Reader r = initReader(reader);
    if (components == null) {
      components = createComponents(fieldName, r);
      reuseStrategy.setReusableComponents(fieldName, components);
    } else {
      components.reset(r);
    }
    return components.getTokenStream();
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
   * {@inheritDoc}
   */
  @Override
  public void close() {
    super.close();
    reuseStrategy.close();
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
     * Resets the encapsulated components with the given reader. If the components
     * cannot be reset, an Exception should be thrown.
     * 
     * @param reader
     *          a reader to reset the source component
     * @throws IOException
     *           if the component's reset method throws an {@link IOException}
     */
    protected void reset(final Reader reader) throws IOException {
      source.reset(reader);
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

  /**
   * Strategy defining how TokenStreamComponents are reused per call to
   * {@link ReusableAnalyzerBase#tokenStream(String, java.io.Reader)}.
   */
  public static abstract class ReuseStrategy {

    private CloseableThreadLocal<Object> storedValue = new CloseableThreadLocal<Object>();

    /**
     * Gets the reusable TokenStreamComponents for the field with the given name
     *
     * @param fieldName Name of the field whose reusable TokenStreamComponents
     *        are to be retrieved
     * @return Reusable TokenStreamComponents for the field, or {@code null}
     *         if there was no previous components for the field
     */
    public abstract TokenStreamComponents getReusableComponents(String fieldName);

    /**
     * Stores the given TokenStreamComponents as the reusable components for the
     * field with the give name
     *
     * @param fieldName Name of the field whose TokenStreamComponents are being set
     * @param components TokenStreamComponents which are to be reused for the field
     */
    public abstract void setReusableComponents(String fieldName, TokenStreamComponents components);

    /**
     * Returns the currently stored value
     *
     * @return Currently stored value or {@code null} if no value is stored
     */
    protected final Object getStoredValue() {
      try {
        return storedValue.get();
      } catch (NullPointerException npe) {
        if (storedValue == null) {
          throw new AlreadyClosedException("this Analyzer is closed");
        } else {
          throw npe;
        }
      }
    }

    /**
     * Sets the stored value
     *
     * @param storedValue Value to store
     */
    protected final void setStoredValue(Object storedValue) {
      try {
        this.storedValue.set(storedValue);
      } catch (NullPointerException npe) {
        if (storedValue == null) {
          throw new AlreadyClosedException("this Analyzer is closed");
        } else {
          throw npe;
        }
      }
    }

    /**
     * Closes the ReuseStrategy, freeing any resources
     */
    public void close() {
      storedValue.close();
      storedValue = null;
    }
  }

  /**
   * Implementation of {@link ReuseStrategy} that reuses the same components for
   * every field.
   */
  public final static class GlobalReuseStrategy extends ReuseStrategy {

    /**
     * {@inheritDoc}
     */
    public TokenStreamComponents getReusableComponents(String fieldName) {
      return (TokenStreamComponents) getStoredValue();
    }

    /**
     * {@inheritDoc}
     */
    public void setReusableComponents(String fieldName, TokenStreamComponents components) {
      setStoredValue(components);
    }
  }

  /**
   * Implementation of {@link ReuseStrategy} that reuses components per-field by
   * maintaining a Map of TokenStreamComponent per field name.
   */
  public static class PerFieldReuseStrategy extends ReuseStrategy {

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public TokenStreamComponents getReusableComponents(String fieldName) {
      Map<String, TokenStreamComponents> componentsPerField = (Map<String, TokenStreamComponents>) getStoredValue();
      return componentsPerField != null ? componentsPerField.get(fieldName) : null;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public void setReusableComponents(String fieldName, TokenStreamComponents components) {
      Map<String, TokenStreamComponents> componentsPerField = (Map<String, TokenStreamComponents>) getStoredValue();
      if (componentsPerField == null) {
        componentsPerField = new HashMap<String, TokenStreamComponents>();
        setStoredValue(componentsPerField);
      }
      componentsPerField.put(fieldName, components);
    }
  }

}
