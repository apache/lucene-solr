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

package org.apache.lucene.analysis.pattern;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.pattern.PatternTypingFilter.PatternTypingRule;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * Provides a filter that will analyze tokens with the analyzer from an arbitrary field type. By itself this
 * filter is not very useful. Normally it is combined with a filter that reacts to types or flags.
 *
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_taf" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="com.example.PatternTypingFilter" patternFile="patterns.txt"/&gt;
 *     &lt;filter class="solr.TokenAnalyzerFilter" asType="text_en" preserveType="true"/&gt;
 *     &lt;filter class="solr.TypeAsSynonymFilterFactory" prefix="__TAS__"
 *               ignore="word,&amp;lt;ALPHANUM&amp;gt;,&amp;lt;NUM&amp;gt;,&amp;lt;SOUTHEAST_ASIAN&amp;gt;,&amp;lt;IDEOGRAPHIC&amp;gt;,&amp;lt;HIRAGANA&amp;gt;,&amp;lt;KATAKANA&amp;gt;,&amp;lt;HANGUL&amp;gt;,&amp;lt;EMOJI&amp;gt;"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * <p>
 * Note that a configuration such as above may interfere with multi-word synonyms. The patterns file has the format:
 * <pre>
 * (flags) (pattern) ::: (replacement)
 * </pre>
 * Therefore to set the first 2 flag bits on the original token matching 401k or 401(k) and adding a type of
 * 'legal2_401_k' whenever either one is encountered one would use:
 * <pre>
 * 3 (\d+)\(?([a-z])\)? ::: legal2_$1_$2
 * </pre>
 * Note that the number indicating the flag bits to set must not have leading spaces and be followed by a single
 * space, and must be 0 if no flags should be set. The flags number should not contain commas or a decimal point.
 * Lines for which the first character is <code>#</code> will be ignored as comments.  Does not support producing
 * a synonym textually identical to the original term.
 *
 * @lucene.spi {@value #NAME}
 * @since 8.8
 */
public class PatternTypingFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {

  /**
   * SPI name
   */
  public static final String NAME = "patternTyping";

  private final String patternFile;
  private PatternTypingRule[] rules;

  /**
   * Creates a new PatternTypingFilterFactory
   */
  public PatternTypingFilterFactory(Map<String, String> args) {
    super(args);
    patternFile = require(args, "patternFile");
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    List<PatternTypingRule> ruleList = new ArrayList<>();
    List<String> lines = getLines(loader, patternFile);
    // format: # regex ::: typename[_$1[_$2 ...]]    (technically _$1 does not need the '_' but it usually makes sense)
    // eg: 2 (\d+\(?([a-z])\)?\(?(\d+)\)? ::: legal3_$1_$2_3
    // which yields legal3_501_c_3 for 501(c)(3) or 501c3 and sets the second lowest bit in flags
    for (String line : lines) {
      int firstSpace = line.indexOf(" "); // no leading spaces allowed
      int flagsVal = Integer.parseInt(line.substring(0, firstSpace));
      line = line.substring(firstSpace + 1);
      String[] split = line.split(" ::: "); // arbitrary, unlikely to occur in a useful regex easy to read
      if (split.length != 2) {
        throw new RuntimeException("The PatternTypingFilter: Always two there are, no more, no less, a pattern and a replacement (separated by ' ::: ' )");
      }
      Pattern compiled = Pattern.compile(split[0]);
      ruleList.add(new PatternTypingRule(compiled, flagsVal, split[1]));
    }
    this.rules = ruleList.toArray(new PatternTypingRule[0]);
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new PatternTypingFilter(input, rules);
  }
}
