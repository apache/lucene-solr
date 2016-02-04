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


import java.util.Map;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.util.Version;

/**
 * Factory for {@link TrimFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_trm" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.NGramTokenizerFactory"/&gt;
 *     &lt;filter class="solr.TrimFilterFactory" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @see TrimFilter
 */
public class TrimFilterFactory extends TokenFilterFactory {
  
  private boolean updateOffsets;
  
  /** Creates a new TrimFilterFactory */
  public TrimFilterFactory(Map<String,String> args) {
    super(args);
    
    if (luceneMatchVersion.onOrAfter(Version.LUCENE_5_0_0) == false) {
      updateOffsets = getBoolean(args, "updateOffsets", false);
      if (updateOffsets && luceneMatchVersion.onOrAfter(Version.LUCENE_4_4_0)) {
        throw new IllegalArgumentException("updateOffsets=true is not supported anymore as of Lucene 4.4");
      }
    } else if (args.containsKey("updateOffsets")) {
      throw new IllegalArgumentException("updateOffsets is not a valid option as of Lucene 5.0");
    }
    
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }
  
  @Override
  public TokenFilter create(TokenStream input) {
    if (luceneMatchVersion.onOrAfter(Version.LUCENE_4_4_0)) {
      return new TrimFilter(input);
    } else {
      @SuppressWarnings("deprecation")
      final Lucene43TrimFilter filter = new Lucene43TrimFilter(input, updateOffsets);
      return filter;
    }
  }
}
