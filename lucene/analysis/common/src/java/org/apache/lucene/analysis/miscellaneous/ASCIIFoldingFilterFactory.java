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
package org.apache.lucene.analysis.miscellaneous;


import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.util.AbstractAnalysisFactory;
import org.apache.lucene.analysis.util.MultiTermAwareComponent;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.TokenStream;

/** 
 * Factory for {@link ASCIIFoldingFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_ascii" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.ASCIIFoldingFilterFactory" preserveOriginal="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class ASCIIFoldingFilterFactory extends TokenFilterFactory implements MultiTermAwareComponent {
  private static final String PRESERVE_ORIGINAL = "preserveOriginal";

  private final boolean preserveOriginal;
  
  /** Creates a new ASCIIFoldingFilterFactory */
  public ASCIIFoldingFilterFactory(Map<String,String> args) {
    super(args);
    preserveOriginal = getBoolean(args, PRESERVE_ORIGINAL, false);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }
  
  @Override
  public ASCIIFoldingFilter create(TokenStream input) {
    return new ASCIIFoldingFilter(input, preserveOriginal);
  }

  @Override
  public AbstractAnalysisFactory getMultiTermComponent() {
    if (preserveOriginal) {
      // The main use-case for using preserveOriginal is to match regardless of
      // case but to give better scores to exact matches. Since most multi-term
      // queries return constant scores anyway, the multi-term component only
      // emits the folded token
      Map<String, String> args = new HashMap<>(getOriginalArgs());
      args.remove(PRESERVE_ORIGINAL);
      return new ASCIIFoldingFilterFactory(args);
    } else {
      return this;
    }
  }
}

