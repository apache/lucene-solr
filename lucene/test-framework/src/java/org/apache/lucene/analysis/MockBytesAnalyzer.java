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

import java.io.Reader;

/**
 * Analyzer for testing that encodes terms as UTF-16 bytes.
 */
public class MockBytesAnalyzer extends Analyzer {
  private final MockBytesAttributeFactory factory = new MockBytesAttributeFactory();
  
  @Override
  protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
    Tokenizer t = new MockTokenizer(factory, reader, MockTokenizer.KEYWORD, false, MockTokenizer.DEFAULT_MAX_TOKEN_LENGTH);
    return new TokenStreamComponents(t);
  }
}
