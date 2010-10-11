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

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.index.Payload;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.io.Reader;

/**
 * Analyzer for testing
 */
public final class MockAnalyzer extends Analyzer {

  public MockAnalyzer() {
  }

  public TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream result = new LowerCaseTokenizer(LuceneTestCase.TEST_VERSION_CURRENT, reader);
    result = new SimplePayloadFilter(result, fieldName);
    return result;
  }

  private class SavedStreams {
    Tokenizer upstream;
    TokenFilter filter;
  }

  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    SavedStreams saved = (SavedStreams) getPreviousTokenStream();
    if (saved == null){
      saved = new SavedStreams();
      saved.upstream = new LowerCaseTokenizer(LuceneTestCase.TEST_VERSION_CURRENT, reader);
      saved.filter = new SimplePayloadFilter(saved.upstream, fieldName);
      setPreviousTokenStream(saved);
      return saved.filter;
    } else {
      saved.upstream.reset(reader);
      saved.filter.reset();
      return saved.filter;
    }                         
  }
}

final class SimplePayloadFilter extends TokenFilter {
  String fieldName;
  int pos;
  final PayloadAttribute payloadAttr;
  final CharTermAttribute termAttr;

  public SimplePayloadFilter(TokenStream input, String fieldName) {
    super(input);
    this.fieldName = fieldName;
    pos = 0;
    payloadAttr = input.addAttribute(PayloadAttribute.class);
    termAttr = input.addAttribute(CharTermAttribute.class);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      payloadAttr.setPayload(new Payload(("pos: " + pos).getBytes()));
      pos++;
      return true;
    } else {
      return false;
    }
  }
}