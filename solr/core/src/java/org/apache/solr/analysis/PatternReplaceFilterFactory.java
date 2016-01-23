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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.pattern.PatternReplaceFilter;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Factory for {@link PatternReplaceFilter}. 
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_ptnreplace" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.KeywordTokenizerFactory"/&gt;
 *     &lt;filter class="solr.PatternReplaceFilterFactory" pattern="([^a-z])" replacement=""
 *             replace="all"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @see PatternReplaceFilter
 */
public class PatternReplaceFilterFactory extends BaseTokenFilterFactory {
  Pattern p;
  String replacement;
  boolean all = true;
  
  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    try {
      p = Pattern.compile(args.get("pattern"));
    } catch (PatternSyntaxException e) {
      throw new RuntimeException
        ("Configuration Error: 'pattern' can not be parsed in " +
         this.getClass().getName(), e);
    }
    
    replacement = args.get("replacement");
    
    String r = args.get("replace");
    if (null != r) {
      if (r.equals("all")) {
        all = true;
      } else {
        if (r.equals("first")) {
          all = false;
        } else {
          throw new RuntimeException
            ("Configuration Error: 'replace' must be 'first' or 'all' in "
             + this.getClass().getName());
        }
      }
    }

  }
  public PatternReplaceFilter create(TokenStream input) {
    return new PatternReplaceFilter(input, p, replacement, all);
  }
}
