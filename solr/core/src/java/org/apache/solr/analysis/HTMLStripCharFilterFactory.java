package org.apache.solr.analysis;


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

import org.apache.lucene.analysis.CharStream;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
* Factory for {@link HTMLStripCharFilter}. 
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_html" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;charFilter class="solr.HTMLStripCharFilterFactory" escapedTags="a, title" /&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre
 *
 */
 public class HTMLStripCharFilterFactory extends BaseCharFilterFactory {
  
  Set<String> escapedTags = null;
  Pattern TAG_NAME_PATTERN = Pattern.compile("[^\\s,]+");

  public HTMLStripCharFilter create(CharStream input) {
    HTMLStripCharFilter charFilter;
    if (null == escapedTags) {
      charFilter = new HTMLStripCharFilter(input);
    } else {
      charFilter = new HTMLStripCharFilter(input, escapedTags);
    }
    return charFilter;
  }
  
  @Override
  public void init(Map<String,String> args) {
    super.init(args);
    String escapedTagsArg = args.get("escapedTags");
    if (null != escapedTagsArg) {
      Matcher matcher = TAG_NAME_PATTERN.matcher(escapedTagsArg);
      while (matcher.find()) {
        if (null == escapedTags) {
          escapedTags = new HashSet<String>();
        }
        escapedTags.add(matcher.group(0));
      }
    }
  }
}
