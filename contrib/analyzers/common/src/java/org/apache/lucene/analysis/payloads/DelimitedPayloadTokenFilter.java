package org.apache.lucene.analysis.payloads;
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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;


/**
 * Characters before the delimiter are the "token", those after are the payload.
 * <p/>
 * For example, if the delimiter is '|', then for the string "foo|bar", foo is the token
 * and "bar" is a payload.
 * <p/>
 * Note, you can also include a {@link org.apache.lucene.analysis.payloads.PayloadEncoder} to convert the payload in an appropriate way (from characters to bytes).
 * <p/>
 * Note make sure your Tokenizer doesn't split on the delimiter, or this won't work
 *
 * @see PayloadEncoder
 */
public final class DelimitedPayloadTokenFilter extends TokenFilter {
  public static final char DEFAULT_DELIMITER = '|';
  protected char delimiter = DEFAULT_DELIMITER;
  protected TermAttribute termAtt;
  protected PayloadAttribute payAtt;
  protected PayloadEncoder encoder;

  /**
   * Construct a token stream filtering the given input.
   */
  protected DelimitedPayloadTokenFilter(TokenStream input) {
    this(input, DEFAULT_DELIMITER, new IdentityEncoder());
  }


  public DelimitedPayloadTokenFilter(TokenStream input, char delimiter, PayloadEncoder encoder) {
    super(input);
    termAtt = addAttribute(TermAttribute.class);
    payAtt = addAttribute(PayloadAttribute.class);
    this.delimiter = delimiter;
    this.encoder = encoder;
  }

  @Override
  public boolean incrementToken() throws IOException {
    boolean result = false;
    if (input.incrementToken()) {
      final char[] buffer = termAtt.termBuffer();
      final int length = termAtt.termLength();
      //look for the delimiter
      boolean seen = false;
      for (int i = 0; i < length; i++) {
        if (buffer[i] == delimiter) {
          termAtt.setTermBuffer(buffer, 0, i);
          payAtt.setPayload(encoder.encode(buffer, i + 1, (length - (i + 1))));
          seen = true;
          break;//at this point, we know the whole piece, so we can exit.  If we don't see the delimiter, then the termAtt is the same
        }
      }
      if (seen == false) {
        //no delimiter
        payAtt.setPayload(null);
      }
      result = true;
    }
    return result;
  }
}
