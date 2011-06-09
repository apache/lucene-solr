/**
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
package org.apache.solr.analysis;

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.phonetic.DoubleMetaphoneFilter;

/**
 * Factory for {@link DoubleMetaphoneFilter}.
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_dblmtphn" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.DoubleMetaphoneFilterFactory" inject="true" maxCodeLength="4"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 */
public class DoubleMetaphoneFilterFactory extends BaseTokenFilterFactory 
{
  public static final String INJECT = "inject"; 
  public static final String MAX_CODE_LENGTH = "maxCodeLength"; 

  public static final int DEFAULT_MAX_CODE_LENGTH = 4;

  private boolean inject = true;
  private int maxCodeLength = DEFAULT_MAX_CODE_LENGTH;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);

    inject = getBoolean(INJECT, true);

    if (args.get(MAX_CODE_LENGTH) != null) {
      maxCodeLength = Integer.parseInt(args.get(MAX_CODE_LENGTH));
    }
  }

  public DoubleMetaphoneFilter create(TokenStream input) {
    return new DoubleMetaphoneFilter(input, maxCodeLength, inject);
  }
}
