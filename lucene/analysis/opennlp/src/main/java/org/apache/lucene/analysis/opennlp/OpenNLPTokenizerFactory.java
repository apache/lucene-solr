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

package org.apache.lucene.analysis.opennlp;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.opennlp.tools.NLPSentenceDetectorOp;
import org.apache.lucene.analysis.opennlp.tools.NLPTokenizerOp;
import org.apache.lucene.analysis.opennlp.tools.OpenNLPOpsFactory;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;

/**
 * Factory for {@link OpenNLPTokenizer}.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_opennlp" class="solr.TextField" positionIncrementGap="100"
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.OpenNLPTokenizerFactory" sentenceModel="filename" tokenizerModel="filename"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * @since 7.3.0
 */
public class OpenNLPTokenizerFactory extends TokenizerFactory implements ResourceLoaderAware {
  public static final String SENTENCE_MODEL = "sentenceModel";
  public static final String TOKENIZER_MODEL = "tokenizerModel";

  private final String sentenceModelFile;
  private final String tokenizerModelFile;

  public OpenNLPTokenizerFactory(Map<String,String> args) {
    super(args);
    sentenceModelFile = require(args, SENTENCE_MODEL);
    tokenizerModelFile = require(args, TOKENIZER_MODEL);
    if ( ! args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public OpenNLPTokenizer create(AttributeFactory factory) {
    try {
      NLPSentenceDetectorOp sentenceOp = OpenNLPOpsFactory.getSentenceDetector(sentenceModelFile);
      NLPTokenizerOp tokenizerOp = OpenNLPOpsFactory.getTokenizer(tokenizerModelFile);
      return new OpenNLPTokenizer(factory, sentenceOp, tokenizerOp);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    // register models in cache with file/resource names
    if (sentenceModelFile != null) {
      OpenNLPOpsFactory.getSentenceModel(sentenceModelFile, loader);
    }
    if (tokenizerModelFile != null) {
      OpenNLPOpsFactory.getTokenizerModel(tokenizerModelFile, loader);
    }
  }
}
