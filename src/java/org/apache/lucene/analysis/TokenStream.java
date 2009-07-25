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

import java.io.IOException;

import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeSource;

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
  A new TokenStream API is introduced with Lucene 2.9. Since
  2.9 Token is deprecated and the preferred way to store
  the information of a token is to use {@link AttributeImpl}s.
  <p>
  For that reason TokenStream extends {@link AttributeSource}
  now. Note that only one instance per {@link AttributeImpl} is
  created and reused for every token. This approach reduces
  object creations and allows local caching of references to
  the {@link AttributeImpl}s. See {@link #incrementToken()} for further details.
  <p>
  <b>The workflow of the new TokenStream API is as follows:</b>
  <ol>
    <li>Instantiation of TokenStream/TokenFilters which add/get attributes
        to/from the {@link AttributeSource}. 
    <li>The consumer calls {@link TokenStream#reset()}.
    <li>the consumer retrieves attributes from the
        stream and stores local references to all attributes it wants to access
    <li>The consumer calls {@link #incrementToken()} until it returns false and
        consumes the attributes after each call.    
  </ol>
  To make sure that filters and consumers know which attributes are available
  the attributes must be added during instantiation. Filters and 
  consumers are not required to check for availability of attributes in {@link #incrementToken()}.
  <p>
  Sometimes it is desirable to capture a current state of a
  TokenStream, e. g. for buffering purposes (see {@link CachingTokenFilter},
  {@link TeeSinkTokenFilter}). For this usecase
  {@link AttributeSource#captureState} and {@link AttributeSource#restoreState} can be used.  
 * <p><font color="#FF0000">
 */
public abstract class TokenStream extends AttributeSource {

  /** @deprecated Remove this when old API is removed! */
  private static final AttributeFactory DEFAULT_TOKEN_WRAPPER_ATTRIBUTE_FACTORY
    = new TokenWrapperAttributeFactory(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY);
  
  /** @deprecated Remove this when old API is removed! */
  private static final Class[] METHOD_NO_PARAMS = new Class[0];

  /** @deprecated Remove this when old API is removed! */
  private static final Class[] METHOD_TOKEN_PARAM = new Class[]{Token.class};
  
  /** @deprecated Remove this when old API is removed! */
  private final TokenWrapper tokenWrapper;
  
  /** @deprecated Remove this when old API is removed! */
  private static boolean onlyUseNewAPI = false;

  /** @deprecated Remove this when old API is removed! */
  private final boolean
    hasIncrementToken = isMethodOverridden("incrementToken", METHOD_NO_PARAMS),
    hasReusableNext = onlyUseNewAPI ? false : isMethodOverridden("next", METHOD_TOKEN_PARAM),
    hasNext = onlyUseNewAPI ? false : isMethodOverridden("next", METHOD_NO_PARAMS);
  
  /** @deprecated Remove this when old API is removed! */
  private boolean isMethodOverridden(String name, Class[] params) {
    try {
      return this.getClass().getMethod(name, params).getDeclaringClass() != TokenStream.class;
    } catch (NoSuchMethodException e) {
      // should not happen
      throw new RuntimeException(e);
    }
  }
  
  /** @deprecated Remove this when old API is removed! */
  private static final class TokenWrapperAttributeFactory extends AttributeFactory {
    private final AttributeFactory delegate;
  
    private TokenWrapperAttributeFactory(AttributeFactory delegate) {
      this.delegate = delegate;
    }
  
    public AttributeImpl createAttributeInstance(Class attClass) {
      return attClass.isAssignableFrom(TokenWrapper.class)
        ? new TokenWrapper()
        : delegate.createAttributeInstance(attClass);
    }
    
    // this is needed for TeeSinkTokenStream's check for compatibility of AttributeSource,
    // so two TokenStreams using old API have the same AttributeFactory wrapped by this one.
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other instanceof TokenWrapperAttributeFactory) {
        final TokenWrapperAttributeFactory af = (TokenWrapperAttributeFactory) other;
        return this.delegate.equals(af.delegate);
      }
      return false;
    }
  }

  /**
   * A TokenStream using the default attribute factory.
   */
  protected TokenStream() {
    super(onlyUseNewAPI
      ? AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY
      : TokenStream.DEFAULT_TOKEN_WRAPPER_ATTRIBUTE_FACTORY
    );
    tokenWrapper = initTokenWrapper(null);
    check();
  }
  
  /**
   * A TokenStream that uses the same attributes as the supplied one.
   */
  protected TokenStream(AttributeSource input) {
    super(input);
    tokenWrapper = initTokenWrapper(input);
    check();
  }
  
  /**
   * A TokenStream using the supplied AttributeFactory for creating new {@link Attribute} instances.
   */
  protected TokenStream(AttributeFactory factory) {
    super(onlyUseNewAPI
      ? factory
      : new TokenWrapperAttributeFactory(factory)
    );
    tokenWrapper = initTokenWrapper(null);
    check();
  }

  /** @deprecated Remove this when old API is removed! */
  private TokenWrapper initTokenWrapper(AttributeSource input) {
    if (onlyUseNewAPI) {
      // no wrapper needed
      return null;
    } else {
      // if possible get the wrapper from the filter's input stream
      if (input instanceof TokenStream && ((TokenStream) input).tokenWrapper != null) {
        return ((TokenStream) input).tokenWrapper;
      }
      // check that all attributes are implemented by the same TokenWrapper instance
      final AttributeImpl att = addAttribute(TermAttribute.class);
      if (att instanceof TokenWrapper &&
        addAttribute(TypeAttribute.class) == att &&
        addAttribute(PositionIncrementAttribute.class) == att &&
        addAttribute(FlagsAttribute.class) == att &&
        addAttribute(OffsetAttribute.class) == att &&
        addAttribute(PayloadAttribute.class) == att
      ) {
        return (TokenWrapper) att;
      } else {
        throw new UnsupportedOperationException(
          "If onlyUseNewAPI is disabled, all basic Attributes must be implemented by the internal class "+
          "TokenWrapper. Please make sure, that all TokenStreams/TokenFilters in this chain have been "+
          "instantiated with this flag disabled and do not add any custom instances for the basic Attributes!"
        );
      }
    }
  }

  /** @deprecated Remove this when old API is removed! */
  private void check() {
    if (onlyUseNewAPI && !hasIncrementToken) {
      throw new UnsupportedOperationException(getClass().getName()+" does not implement incrementToken() which is needed for onlyUseNewAPI.");
    }

    // a TokenStream subclass must at least implement one of the methods!
    if (!(hasIncrementToken || hasNext || hasReusableNext)) {
      throw new UnsupportedOperationException(getClass().getName()+" does not implement any of incrementToken(), next(Token), next().");
    }
  }
  
  /**
   * For extra performance you can globally enable the new {@link #incrementToken}
   * API using {@link Attribute}s. There will be a small, but in most cases neglectible performance 
   * increase by enabling this, but it only works if <b>all</b> TokenStreams and -Filters
   * use the new API and implement {@link #incrementToken}. This setting can only be enabled
   * globally.
   * <P>This setting only affects TokenStreams instantiated after this call. All TokenStreams
   * already created use the other setting.
   * <P>All core analyzers are compatible with this setting, if you have own
   * TokenStreams/-Filters, that are also compatible, enable this.
   * <P>When enabled, tokenization may throw {@link UnsupportedOperationException}s,
   * if the whole tokenizer chain is not compatible.
   * <P>The default is <code>false</code>, so there is the fallback to the old API available.
   * @deprecated This setting will be <code>true</code> per default in Lucene 3.0,
   * when {@link #incrementToken} is abstract and must be always implemented.
   */
  public static void setOnlyUseNewAPI(boolean onlyUseNewAPI) {
    TokenStream.onlyUseNewAPI = onlyUseNewAPI;
  }
  
  /** Returns if only the new API is used.
   * @see #setOnlyUseNewAPI
   * @deprecated This setting will be <code>true</code> per default in Lucene 3.0,
   * when {@link #incrementToken} is abstract and must be always implemented.
   */
  public static boolean getOnlyUseNewAPI() {
    return onlyUseNewAPI;
  }
  
  /**
   * Consumers (e. g. the indexer) use this method to advance the stream 
   * to the next token. Implementing classes must implement this method 
   * and update the appropriate {@link AttributeImpl}s with content of the 
   * next token.
   * <p>
   * This method is called for every token of a document, so an efficient
   * implementation is crucial for good performance. To avoid calls to 
   * {@link #addAttribute(Class)} and {@link #getAttribute(Class)} and
   * downcasts, references to all {@link AttributeImpl}s that this stream uses 
   * should be retrieved during instantiation.   
   * <p>
   * To make sure that filters and consumers know which attributes are available
   * the attributes must be added during instantiation. Filters and 
   * consumers are not required to check for availability of attributes in {@link #incrementToken()}.
   * 
   * @return false for end of stream; true otherwise
   *
   * <p>
   * <b>Note that this method will be defined abstract in Lucene 3.0.</b>
   */
  public boolean incrementToken() throws IOException {
    assert !onlyUseNewAPI && tokenWrapper != null;
    
    final Token token;
    if (hasReusableNext) {
      token = next(tokenWrapper.delegate);
    } else {
      assert hasNext;
      token = next();
    }
    if (token == null) return false;
    tokenWrapper.delegate = token;
    return true;
  }

  /** Returns the next token in the stream, or null at EOS.
   *  When possible, the input Token should be used as the
   *  returned Token (this gives fastest tokenization
   *  performance), but this is not required and a new Token
   *  may be returned. Callers may re-use a single Token
   *  instance for successive calls to this method.
   *  <p>
   *  This implicitly defines a "contract" between 
   *  consumers (callers of this method) and 
   *  producers (implementations of this method 
   *  that are the source for tokens):
   *  <ul>
   *   <li>A consumer must fully consume the previously 
   *       returned Token before calling this method again.</li>
   *   <li>A producer must call {@link Token#clear()}
   *       before setting the fields in it & returning it</li>
   *  </ul>
   *  Also, the producer must make no assumptions about a
   *  Token after it has been returned: the caller may
   *  arbitrarily change it.  If the producer needs to hold
   *  onto the token for subsequent calls, it must clone()
   *  it before storing it.
   *  Note that a {@link TokenFilter} is considered a consumer.
   *  @param reusableToken a Token that may or may not be used to
   *  return; this parameter should never be null (the callee
   *  is not required to check for null before using it, but it is a
   *  good idea to assert that it is not null.)
   *  @return next token in the stream or null if end-of-stream was hit
   *  @deprecated The new {@link #incrementToken()} and {@link AttributeSource}
   *  APIs should be used instead.
   */
  public Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    
    if (onlyUseNewAPI)
      throw new UnsupportedOperationException("This TokenStream only supports the new Attributes API.");
    
    if (hasIncrementToken) {
      tokenWrapper.delegate = reusableToken;
      return incrementToken() ? tokenWrapper.delegate : null;
    } else {
      assert hasNext;
      final Token token = next();
      if (token == null) return null;
      tokenWrapper.delegate = token;
      return token;
    }
  }

  /** Returns the next token in the stream, or null at EOS.
   * @deprecated The returned Token is a "full private copy" (not
   * re-used across calls to next()) but will be slower
   * than calling {@link #next(Token)} or using the new
   * {@link #incrementToken()} method with the new
   * {@link AttributeSource} API.
   */
  public Token next() throws IOException {
    if (onlyUseNewAPI)
      throw new UnsupportedOperationException("This TokenStream only supports the new Attributes API.");
    
    if (hasIncrementToken) {
      return incrementToken() ? ((Token) tokenWrapper.delegate.clone()) : null;
    } else {
      assert hasReusableNext;
      final Token token = next(tokenWrapper.delegate);
      if (token == null) return null;
      tokenWrapper.delegate = token;
      return (Token) token.clone();
    }
  }

  /** Resets this stream to the beginning. This is an
   *  optional operation, so subclasses may or may not
   *  implement this method. Reset() is not needed for
   *  the standard indexing process. However, if the Tokens 
   *  of a TokenStream are intended to be consumed more than 
   *  once, it is necessary to implement reset().  Note that
   *  if your TokenStream caches tokens and feeds them back
   *  again after a reset, it is imperative that you
   *  clone the tokens when you store them away (on the
   *  first pass) as well as when you return them (on future
   *  passes after reset()).
   */
  public void reset() throws IOException {}
  
  /** Releases resources associated with this stream. */
  public void close() throws IOException {}
  
}
