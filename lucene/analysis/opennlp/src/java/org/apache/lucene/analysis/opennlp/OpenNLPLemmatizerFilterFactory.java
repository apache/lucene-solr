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
import org.apache.lucene.analysis.opennlp.tools.NLPLemmatizerOp;
import org.apache.lucene.analysis.opennlp.tools.OpenNLPOpsFactory;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;

/**
 * Factory for {@link OpenNLPLemmatizerFilter}.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_opennlp_lemma" class="solr.TextField" positionIncrementGap="100"
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.OpenNLPTokenizerFactory"
 *                sentenceModel="filename"
 *                tokenizerModel="filename"/&gt;
 *     /&gt;
 *     &lt;filter class="solr.OpenNLPLemmatizerFilterFactory"
 *             dictionary="filename"
 *             lemmatizerModel="filename"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @since 7.3.0
 * @lucene.spi {@value #NAME}
 */
public class OpenNLPLemmatizerFilterFactory extends TokenFilterFactory
    implements ResourceLoaderAware {

  /** SPI name */
  public static final String NAME = "openNlpLemmatizer";

  public static final String DICTIONARY = "dictionary";
  public static final String LEMMATIZER_MODEL = "lemmatizerModel";

  private final String dictionaryFile;
  private final String lemmatizerModelFile;

  public OpenNLPLemmatizerFilterFactory(Map<String, String> args) {
    super(args);
    dictionaryFile = get(args, DICTIONARY);
    lemmatizerModelFile = get(args, LEMMATIZER_MODEL);

    if (dictionaryFile == null && lemmatizerModelFile == null) {
      throw new IllegalArgumentException(
          "Configuration Error: missing parameter: at least one of '"
              + DICTIONARY
              + "' and '"
              + LEMMATIZER_MODEL
              + "' must be provided.");
    }

    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public OpenNLPLemmatizerFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public OpenNLPLemmatizerFilter create(TokenStream in) {
    try {
      NLPLemmatizerOp lemmatizerOp =
          OpenNLPOpsFactory.getLemmatizer(dictionaryFile, lemmatizerModelFile);
      return new OpenNLPLemmatizerFilter(in, lemmatizerOp);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    // register models in cache with file/resource names
    if (dictionaryFile != null) {
      OpenNLPOpsFactory.getLemmatizerDictionary(dictionaryFile, loader);
    }
    if (lemmatizerModelFile != null) {
      OpenNLPOpsFactory.getLemmatizerModel(lemmatizerModelFile, loader);
    }
  }
}
