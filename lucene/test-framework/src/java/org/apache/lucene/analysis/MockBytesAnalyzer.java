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

/**
 * Analyzer for testing that encodes terms as UTF-16 bytes.
 */
public final class MockBytesAnalyzer extends Analyzer {  
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    Tokenizer t = new MockTokenizer(MockUTF16TermAttributeImpl.UTF16_TERM_ATTRIBUTE_FACTORY,
        MockTokenizer.KEYWORD, false, MockTokenizer.DEFAULT_MAX_TOKEN_LENGTH);
    return new TokenStreamComponents(t);
  }
}
