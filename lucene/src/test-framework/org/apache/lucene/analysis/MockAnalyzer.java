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

import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Analyzer for testing
 */
public final class MockAnalyzer extends Analyzer {
  private boolean lowerCase;
  public static final int KEYWORD = 0;
  public static final int WHITESPACE = 1;
  public static final int SIMPLE = 2;
  private int tokenizer;
  private final Random random;
  private Map<String,Integer> previousMappings = new HashMap<String,Integer>();
  
  public MockAnalyzer(Random random) {
    this(random, WHITESPACE, true);
  }
  
  public MockAnalyzer(Random random, int tokenizer, boolean lowercase) {
    this.tokenizer = tokenizer;
    this.lowerCase = lowercase;
    this.random = random;
  }

  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream result;
    if (tokenizer == KEYWORD)
      result = new KeywordTokenizer(reader);
    else if (tokenizer == SIMPLE)
      result = new LetterTokenizer(LuceneTestCase.TEST_VERSION_CURRENT, reader);
    else
      result = new WhitespaceTokenizer(LuceneTestCase.TEST_VERSION_CURRENT, reader);
    if (lowerCase)
      result = new LowerCaseFilter(LuceneTestCase.TEST_VERSION_CURRENT, result);
    result = maybePayload(result, fieldName);
    return result;
  }

  private class SavedStreams {
    Tokenizer upstream;
    TokenStream filter;
  }

  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    @SuppressWarnings("unchecked") Map<String,SavedStreams> map = (Map) getPreviousTokenStream();
    if (map == null) {
      map = new HashMap<String,SavedStreams>();
      setPreviousTokenStream(map);
    }
    
    SavedStreams saved = map.get(fieldName);
    if (saved == null){
      saved = new SavedStreams();
      if (tokenizer == KEYWORD)
        saved.upstream = new KeywordTokenizer(reader);
      else if (tokenizer == SIMPLE)
        saved.upstream = new LetterTokenizer(LuceneTestCase.TEST_VERSION_CURRENT, reader);
      else
        saved.upstream = new WhitespaceTokenizer(LuceneTestCase.TEST_VERSION_CURRENT, reader);
      saved.filter = saved.upstream;
      if (lowerCase)
        saved.filter = new LowerCaseFilter(LuceneTestCase.TEST_VERSION_CURRENT, saved.filter);

      saved.filter = maybePayload(saved.filter, fieldName);
      map.put(fieldName, saved);
      return saved.filter;
    } else {
      saved.upstream.reset(reader);
      saved.filter.reset();
      return saved.filter;
    }                         
  }
  
  private synchronized TokenStream maybePayload(TokenStream stream, String fieldName) {
    Integer val = previousMappings.get(fieldName);
    if (val == null) {
      switch(random.nextInt(3)) {
        case 0: val = -1; // no payloads
                break;
        case 1: val = Integer.MAX_VALUE; // variable length payload
                break;
        case 2: val = random.nextInt(12); // fixed length payload
                break;
      }
      previousMappings.put(fieldName, val); // save it so we are consistent for this field
    }
    
    if (val == -1)
      return stream;
    else if (val == Integer.MAX_VALUE)
      return new MockVariableLengthPayloadFilter(random, stream);
    else
      return new MockFixedLengthPayloadFilter(random, stream, val);
  }
}
