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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.opennlp.tools.NLPChunkerOp;
import org.apache.lucene.analysis.opennlp.tools.OpenNLPOpsFactory;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link OpenNLPChunkerFilter}.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_opennlp_chunked" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.OpenNLPTokenizerFactory" sentenceModel="filename" tokenizerModel="filename"/&gt;
 *     &lt;filter class="solr.OpenNLPPOSFilterFactory" posTaggerModel="filename"/&gt;
 *     &lt;filter class="solr.OpenNLPChunkerFilterFactory" chunkerModel="filename"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * @since 7.3.0
 */
public class OpenNLPChunkerFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  public static final String CHUNKER_MODEL = "chunkerModel";

  private final String chunkerModelFile;

  public OpenNLPChunkerFilterFactory(Map<String,String> args) {
    super(args);
    chunkerModelFile = get(args, CHUNKER_MODEL);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public OpenNLPChunkerFilter create(TokenStream in) {
    try {
      NLPChunkerOp chunkerOp = null;

      if (chunkerModelFile != null) {
        chunkerOp = OpenNLPOpsFactory.getChunker(chunkerModelFile);
      }
      return new OpenNLPChunkerFilter(in, chunkerOp);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void inform(ResourceLoader loader) {
    try {
      // load and register read-only models in cache with file/resource names
      if (chunkerModelFile != null) {
        OpenNLPOpsFactory.getChunkerModel(chunkerModelFile, loader);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
