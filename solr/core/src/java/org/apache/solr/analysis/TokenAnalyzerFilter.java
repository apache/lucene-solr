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

package org.apache.solr.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.util.Iterator;

import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenAnalyzerFilter extends TokenFilter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final TokenAnalyzerFilterFactory.AnalyzerProvider analyzer;
  private final boolean preserveType;

  // things that need reset()
  private TokenStream currentStream;
  private int flags;


  /**
   * A token filter that applies the Analyzer from a specified field type to the individual
   * tokens of the current stream. This is typically only useful when the original tokenizer for the stream
   * is extremely coarse such as WhitespaceTokenizer. The goal is usually to interact with punctuation normally
   * removed by the specified field's tokenizer before it is removed. Note that careful consideration of the
   * effects of query parsers on punctuation must also be given.<br>
   * <br>
   * <p>The tokenizer used within this filter produces tokens from the contents of the {@link CharTermAttribute}
   * produced by the input token stream. Several attributes are copied onto the new tokens from the parent token
   * as follows:
   * <ul>
   *   <li>{@link KeywordAttribute} - if true on the parent token is also set on the child token</li>
   *   <li>{@link PayloadAttribute} - always transferred to the child token</li>
   *   <li>{@link TypeAttribute} - transferred if configured with {@code preserveType="true"}</li>
   *   <li>{@link FlagsAttribute} - contents are combined with the sub token using a bitwise OR operation</li>
   * </ul>
   * <p>
   * Future patches for fine grained control of transfer behavior based on needs encountered in actual usage
   * and additional attributes that should be transferred are (of course) welcome.
   * @since 8.4
   */
  @SuppressWarnings("WeakerAccess")
  protected TokenAnalyzerFilter(TokenStream input, TokenAnalyzerFilterFactory.AnalyzerProvider provider, boolean preserveType) {
    super(input);
    analyzer = provider;
    this.preserveType = preserveType;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (currentStream == null) {
      newSubStream();
      transferState();
      return currentStream != null;
    }
    if (currentStream.incrementToken()) {
      return transferState();
    } else {
      finishCurrent();
      newSubStream();
      return transferState();
    }
  }

  private void newSubStream() throws IOException {
    // create a stream from the current char term attribute
    while (input.incrementToken()) {
      String text = input.getAttribute(CharTermAttribute.class).toString();
      final int offset = getOffset();
      currentStream = this.analyzer.get()
          .tokenStream("TokenAnalyzerFilter_" + this.analyzer.get().getClass().getSimpleName(),
              new CharFilter(new StringReader(text)) {
                @Override
                protected int correct(int currentOff) {
                  return currentOff + offset;
                }

                @Override
                public int read(char[] cbuf, int off, int len) throws IOException {
                  return input.read(cbuf, off, len);
                }
              });
      currentStream.reset();
      if (currentStream.incrementToken()) {
        // success now transfer attributes from parent token.
        if (input.hasAttribute(KeywordAttribute.class)) {
          // only propogate if true
          // todo, maybe config to control this behavior?
          boolean iskw = input.getAttribute(KeywordAttribute.class).isKeyword();
          if (iskw) {
            currentStream.addAttribute(KeywordAttribute.class).setKeyword(iskw);
          }
        }
        if (input.hasAttribute(PayloadAttribute.class)) {
          // todo: config to control this behavior?
          if (!currentStream.hasAttribute(PayloadAttribute.class)) {
            BytesRef payload = input.getAttribute(PayloadAttribute.class).getPayload();
            currentStream.addAttribute(PayloadAttribute.class).setPayload(payload);
          }
        }
        if (input.hasAttribute(TypeAttribute.class)) {
          if (preserveType) {
            String type = input.getAttribute(TypeAttribute.class).type();
            currentStream.addAttribute(TypeAttribute.class).setType(type);
          }
        }
        if (input.hasAttribute(FlagsAttribute.class)) {
          this.flags = input.getAttribute(FlagsAttribute.class).getFlags();
        }
        // todo: other attributes?
        break;
      } else {
        finishCurrent();
      }
    }
  }

  private int getOffset() {
    int offset = 0;
    if (input.hasAttribute(OffsetAttribute.class)) {
      OffsetAttribute parent = input.getAttribute(OffsetAttribute.class);
      offset = parent.startOffset();
    }
    return offset;
  }

  private void finishCurrent() throws IOException {
    currentStream.end();
    currentStream.close();
    currentStream = null;
    flags = 0;
  }

  private boolean transferState() {
    if (currentStream != null) {
      State state = currentStream.captureState();
      Iterator<Class<? extends Attribute>> attributeClassesIterator = currentStream.getAttributeClassesIterator();
      while(attributeClassesIterator.hasNext()) {
        addAttribute(attributeClassesIterator.next());
      }
      this.restoreState(state);
      if (hasAttribute(FlagsAttribute.class)) {
        addAttribute(FlagsAttribute.class).setFlags(flags | getAttribute(FlagsAttribute.class).getFlags());
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void reset() throws IOException {
    flags = 0;
    currentStream = null;
    super.reset();
  }

  @Override
  public void end() throws IOException {
    if (currentStream != null) {
      currentStream.end();
      currentStream.close();
      currentStream = null;
    }
    super.end();
  }

  @Override
  public void close() throws IOException {
    if (currentStream != null) {
      currentStream.end();
      currentStream.close();
      currentStream = null;
    }
    super.close();
  }
}
