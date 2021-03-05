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

package org.apache.lucene.analysis.opennlp.tools;

import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

/**
 * Supply OpenNLP Sentence Tokenizer tool Requires binary models from OpenNLP project on
 * SourceForge.
 */
public class NLPTokenizerOp {
  private final Tokenizer tokenizer;

  public NLPTokenizerOp(TokenizerModel model) {
    tokenizer = new TokenizerME(model);
  }

  public NLPTokenizerOp() {
    tokenizer = null;
  }

  public synchronized Span[] getTerms(String sentence) {
    if (tokenizer == null) {
      Span[] span1 = new Span[1];
      span1[0] = new Span(0, sentence.length());
      return span1;
    }
    return tokenizer.tokenizePos(sentence);
  }
}
