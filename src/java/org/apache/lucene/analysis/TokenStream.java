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
import java.util.Iterator;

import org.apache.lucene.index.Payload;
import org.apache.lucene.util.Attribute;
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
  the information of a token is to use {@link Attribute}s.
  <p>
  For that reason TokenStream extends {@link AttributeSource}
  now. Note that only one instance per {@link Attribute} is
  created and reused for every token. This approach reduces
  object creations and allows local caching of references to
  the {@link Attribute}s. See {@link #incrementToken()} for further details.
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
  the attributes must be added in the during instantiation. Filters and 
  consumers are not required to check for availability of attributes in {@link #incrementToken()}.
  <p>
  Sometimes it is desirable to capture a current state of a
  TokenStream, e. g. for buffering purposes (see {@link CachingTokenFilter},
  {@link TeeTokenFilter}/{@link SinkTokenizer}). For this usecase
  {@link AttributeSource#captureState()} and {@link AttributeSource#restoreState(AttributeSource)} can be used.  
  <p>
  <b>NOTE:</b> In order to enable the new API the method
  {@link #useNewAPI()} has to be called with useNewAPI=true.
  Otherwise the deprecated method {@link #next(Token)} will 
  be used by Lucene consumers (indexer and queryparser) to
  consume the tokens. {@link #next(Token)} will be removed
  in Lucene 3.0.
  <p>
  NOTE: To use the old API subclasses must override {@link #next(Token)}.
  It's also OK to instead override {@link #next()} but that
  method is slower compared to {@link #next(Token)}.
 * <p><font color="#FF0000">
 * WARNING: The status of the new TokenStream, AttributeSource and Attributes is experimental. 
 * The APIs introduced in these classes with Lucene 2.9 might change in the future. 
 * We will make our best efforts to keep the APIs backwards-compatible.</font>
  */

public abstract class TokenStream extends AttributeSource {
  private static boolean useNewAPIDefault = false;
  private boolean useNewAPI = useNewAPIDefault;
  
  protected TokenStream() {
    super();
  }
  
  protected TokenStream(AttributeSource input) {
    super(input);
  }

  /**
   * Returns whether or not the new TokenStream APIs are used
   * by default. 
   * (see {@link #incrementToken()}, {@link AttributeSource}).
   */
  public static boolean useNewAPIDefault() {
    return useNewAPIDefault;
  }

  /**
   * Use this API to enable or disable the new TokenStream API.
   * by default. Can be overridden by calling {@link #setUseNewAPI(boolean)}. 
   * (see {@link #incrementToken()}, {@link AttributeSource}).
   * <p>
   * If set to true, the indexer will call {@link #incrementToken()} 
   * to consume Tokens from this stream.
   * <p>
   * If set to false, the indexer will call {@link #next(Token)}
   * instead. 
   */
  public static void setUseNewAPIDefault(boolean use) {
    useNewAPIDefault = use;
  }
  
  /**
   * Returns whether or not the new TokenStream APIs are used 
   * for this stream.
   * (see {@link #incrementToken()}, {@link AttributeSource}).
   */
  public boolean useNewAPI() {
    return useNewAPI;
  }

  /**
   * Use this API to enable or disable the new TokenStream API
   * for this stream. Overrides {@link #setUseNewAPIDefault(boolean)}.
   * (see {@link #incrementToken()}, {@link AttributeSource}).
   * <p>
   * If set to true, the indexer will call {@link #incrementToken()} 
   * to consume Tokens from this stream.
   * <p>
   * If set to false, the indexer will call {@link #next(Token)}
   * instead. 
   * <p>
   * <b>NOTE: All streams and filters in one chain must use the
   * same API. </b>
   */
  public void setUseNewAPI(boolean use) {
    useNewAPI = use;
  }
    	
	/**
	 * Consumers (e. g. the indexer) use this method to advance the stream 
	 * to the next token. Implementing classes must implement this method 
	 * and update the appropriate {@link Attribute}s with content of the 
	 * next token.
	 * <p>
	 * This method is called for every token of a document, so an efficient
	 * implementation is crucial for good performance. To avoid calls to 
	 * {@link #addAttribute(Class)} and {@link #getAttribute(Class)} and
	 * downcasts, references to all {@link Attribute}s that this stream uses 
	 * should be retrieved during instantiation.   
	 * <p>
	 * To make sure that filters and consumers know which attributes are available
   * the attributes must be added during instantiation. Filters and 
   * consumers are not required to check for availability of attributes in {@link #incrementToken()}.
	 * 
	 * @return false for end of stream; true otherwise
	 *
	 * <p>
	 * <b>Note that this method will be defined abstract in Lucene 3.0.<b>
	 */
	public boolean incrementToken() throws IOException {
	  // subclasses must implement this method; will be made abstract in Lucene 3.0
	  return false;
	}
	
  /** Returns the next token in the stream, or null at EOS.
   *  @deprecated The returned Token is a "full private copy" (not
   *  re-used across calls to next()) but will be slower
   *  than calling {@link #next(Token)} instead.. */
  public Token next() throws IOException {
    final Token reusableToken = new Token();
    Token nextToken = next(reusableToken);

    if (nextToken != null) {
      Payload p = nextToken.getPayload();
      if (p != null) {
        nextToken.setPayload((Payload) p.clone());
      }
    }

    return nextToken;
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
   *  APIs should be used instead. See also {@link #useNewAPI()}.
   */
  public Token next(final Token reusableToken) throws IOException {
    // We don't actually use inputToken, but still add this assert
    assert reusableToken != null;
    return next();
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
  
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append('(');
    
    if (hasAttributes()) {
      // TODO Java 1.5
      //Iterator<Attribute> it = attributes.values().iterator();
      Iterator it = getAttributesIterator();
      if (it.hasNext()) {
        sb.append(it.next().toString());
      }
      while (it.hasNext()) {
        sb.append(',');
        sb.append(it.next().toString());
      }
    }
    sb.append(')');
    return sb.toString();
  }

}
