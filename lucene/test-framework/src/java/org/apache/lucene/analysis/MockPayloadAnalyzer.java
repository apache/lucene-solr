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
package org.apache.lucene.analysis;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;


/**
 * Wraps a whitespace tokenizer with a filter that sets
 * the first token, and odd tokens to posinc=1, and all others
 * to 0, encoding the position as pos: XXX in the payload.
 **/
public final class MockPayloadAnalyzer extends Analyzer {

  @Override
  public TokenStreamComponents createComponents(String fieldName) {
    Tokenizer result = new MockTokenizer( MockTokenizer.WHITESPACE, true);
    return new TokenStreamComponents(result, new MockPayloadFilter(result, fieldName));
  }
}

/**
 *
 *
 **/
final class MockPayloadFilter extends TokenFilter {
  String fieldName;

  int pos;

  int i;

  final PositionIncrementAttribute posIncrAttr;
  final PayloadAttribute payloadAttr;
  final CharTermAttribute termAttr;

  public MockPayloadFilter(TokenStream input, String fieldName) {
    super(input);
    this.fieldName = fieldName;
    pos = 0;
    i = 0;
    posIncrAttr = input.addAttribute(PositionIncrementAttribute.class);
    payloadAttr = input.addAttribute(PayloadAttribute.class);
    termAttr = input.addAttribute(CharTermAttribute.class);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      payloadAttr.setPayload(new BytesRef(("pos: " + pos).getBytes(StandardCharsets.UTF_8)));
      int posIncr;
      if (pos == 0 || i % 2 == 1) {
        posIncr = 1;
      } else {
        posIncr = 0;
      }
      posIncrAttr.setPositionIncrement(posIncr);
      pos += posIncr;
      i++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    i = 0;
    pos = 0;
  }
}

