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
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.opennlp.tools.OpenNLPOpsFactory;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;

/**
 * Factory for {@link OpenNLPPOSFilter}.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_opennlp_pos" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.OpenNLPTokenizerFactory" sentenceModel="filename" tokenizerModel="filename"/&gt;
 *     &lt;filter class="solr.OpenNLPPOSFilterFactory" posTaggerModel="filename"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @since 7.3.0
 * @lucene.spi {@value #NAME}
 */
public class OpenNLPPOSFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {

  /** SPI name */
  public static final String NAME = "openNlppos";

  public static final String POS_TAGGER_MODEL = "posTaggerModel";

  private final String posTaggerModelFile;

  public OpenNLPPOSFilterFactory(Map<String, String> args) {
    super(args);
    posTaggerModelFile = require(args, POS_TAGGER_MODEL);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public OpenNLPPOSFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public OpenNLPPOSFilter create(TokenStream in) {
    try {
      return new OpenNLPPOSFilter(in, OpenNLPOpsFactory.getPOSTagger(posTaggerModelFile));
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void inform(ResourceLoader loader) {
    try { // load and register the read-only model in cache with file/resource name
      OpenNLPOpsFactory.getPOSTaggerModel(posTaggerModelFile, loader);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
