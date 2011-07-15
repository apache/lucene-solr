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
import org.apache.lucene.analysis.miscellaneous.CapitalizationFilter;
import org.apache.lucene.analysis.util.CharArraySet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Factory for {@link CapitalizationFilter}.
 * <p/>
 * The factory takes parameters:<br/>
 * "onlyFirstWord" - should each word be capitalized or all of the words?<br/>
 * "keep" - a keep word list.  Each word that should be kept separated by whitespace.<br/>
 * "keepIgnoreCase - true or false.  If true, the keep list will be considered case-insensitive.<br/>
 * "forceFirstLetter" - Force the first letter to be capitalized even if it is in the keep list<br/>
 * "okPrefix" - do not change word capitalization if a word begins with something in this list.
 * for example if "McK" is on the okPrefix list, the word "McKinley" should not be changed to
 * "Mckinley"<br/>
 * "minWordLength" - how long the word needs to be to get capitalization applied.  If the
 * minWordLength is 3, "and" > "And" but "or" stays "or"<br/>
 * "maxWordCount" - if the token contains more then maxWordCount words, the capitalization is
 * assumed to be correct.<br/>
 *
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_cptlztn" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.CapitalizationFilterFactory" onlyFirstWord="true"
 *     	     keep="java solr lucene" keepIgnoreCase="false"
 *     	     okPrefix="McK McD McA"/&gt;   
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 *
 * @since solr 1.3
 */
public class CapitalizationFilterFactory extends BaseTokenFilterFactory {
  public static final String KEEP = "keep";
  public static final String KEEP_IGNORE_CASE = "keepIgnoreCase";
  public static final String OK_PREFIX = "okPrefix";
  public static final String MIN_WORD_LENGTH = "minWordLength";
  public static final String MAX_WORD_COUNT = "maxWordCount";
  public static final String MAX_TOKEN_LENGTH = "maxTokenLength";
  public static final String ONLY_FIRST_WORD = "onlyFirstWord";
  public static final String FORCE_FIRST_LETTER = "forceFirstLetter";

  //Map<String,String> keep = new HashMap<String, String>(); // not synchronized because it is only initialized once
  CharArraySet keep;

  Collection<char[]> okPrefix = Collections.emptyList(); // for Example: McK

  int minWordLength = 0;  // don't modify capitalization for words shorter then this
  int maxWordCount = CapitalizationFilter.DEFAULT_MAX_WORD_COUNT;
  int maxTokenLength = CapitalizationFilter.DEFAULT_MAX_TOKEN_LENGTH;
  boolean onlyFirstWord = true;
  boolean forceFirstLetter = true; // make sure the first letter is capitol even if it is in the keep list

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    assureMatchVersion();

    String k = args.get(KEEP);
    if (k != null) {
      StringTokenizer st = new StringTokenizer(k);
      boolean ignoreCase = false;
      String ignoreStr = args.get(KEEP_IGNORE_CASE);
      if ("true".equalsIgnoreCase(ignoreStr)) {
        ignoreCase = true;
      }
      keep = new CharArraySet(luceneMatchVersion, 10, ignoreCase);
      while (st.hasMoreTokens()) {
        k = st.nextToken().trim();
        keep.add(k.toCharArray());
      }
    }

    k = args.get(OK_PREFIX);
    if (k != null) {
      okPrefix = new ArrayList<char[]>();
      StringTokenizer st = new StringTokenizer(k);
      while (st.hasMoreTokens()) {
        okPrefix.add(st.nextToken().trim().toCharArray());
      }
    }

    k = args.get(MIN_WORD_LENGTH);
    if (k != null) {
      minWordLength = Integer.valueOf(k);
    }

    k = args.get(MAX_WORD_COUNT);
    if (k != null) {
      maxWordCount = Integer.valueOf(k);
    }

    k = args.get(MAX_TOKEN_LENGTH);
    if (k != null) {
      maxTokenLength = Integer.valueOf(k);
    }

    k = args.get(ONLY_FIRST_WORD);
    if (k != null) {
      onlyFirstWord = Boolean.valueOf(k);
    }

    k = args.get(FORCE_FIRST_LETTER);
    if (k != null) {
      forceFirstLetter = Boolean.valueOf(k);
    }
  }

  public CapitalizationFilter create(TokenStream input) {
    return new CapitalizationFilter(input, onlyFirstWord, keep, 
      forceFirstLetter, okPrefix, minWordLength, maxWordCount, maxTokenLength);
  }
}
