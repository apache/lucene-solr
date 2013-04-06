package org.apache.lucene.analysis.standard;

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

import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeSource.AttributeFactory;

import java.io.Reader;
import java.util.Map;

/**
 * Factory for {@link ClassicTokenizer}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_clssc" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.ClassicTokenizerFactory" maxTokenLength="120"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class ClassicTokenizerFactory extends TokenizerFactory {
  private final int maxTokenLength;

  /** Creates a new ClassicTokenizerFactory */
  public ClassicTokenizerFactory(Map<String,String> args) {
    super(args);
    assureMatchVersion();
    maxTokenLength = getInt(args, "maxTokenLength", StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public ClassicTokenizer create(AttributeFactory factory, Reader input) {
    ClassicTokenizer tokenizer = new ClassicTokenizer(luceneMatchVersion, factory, input); 
    tokenizer.setMaxTokenLength(maxTokenLength);
    return tokenizer;
  }
}
