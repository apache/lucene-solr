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

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;


/**
 * Provides a filter that will analyze tokens with the analyzer from an arbitrary field type. By itself this
 * filter is not very useful. Normally it is combined with a filter that reacts to types or flags. Only one
 * flag (or set flags) can be applied or not applied.
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
 * Note that the number indicating the flag bits to set must be followed by a single space, and must be 0
 * if no flags should be set. The flags number should not contain commas or a decimal point.
 *
 * @lucene.spi {@value #NAME}
 * @since 8.4
 */
public class PatternTypingFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {

  /**
   * SPI name
   */
  public static final String NAME = "patternTyping";

  private final String patternFile;
  // todo, perhaps this could be turned into an FST (wanted: regex to fsm with capturing groups in java)
  private LinkedHashMap<Pattern, String> patterns = new LinkedHashMap<>();
  private Map<Pattern, Integer> flags = new HashMap<>();

  /**
   * Creates a new KeepWordFilterFactory
   */
  public PatternTypingFilterFactory(Map<String, String> args) {
    super(args);
    patternFile = require(args, "patternFile");
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public PatternTypingFilterFactory() {
    throw defaultCtorException();
  }


  @Override
  public void inform(ResourceLoader loader) throws IOException {
    List<String> lines = getLines(loader, patternFile);
    // format:   regex ::: typename[_$1[_$2 ...]]
    // eg: (\d+\(?([a-z])\)?\(?(\d+)\)? ::: legal3_$1_$2_3   yeilds legal3_501_c_3 for 501(c)(3) or 501c3
    for (String line : lines) {
      if (line.trim().startsWith("#")) {
        continue;
      }
      int firstSpace = line.indexOf(" ");
      int flagsVal = Integer.parseInt(line.substring(0,firstSpace));
      line = line.substring(firstSpace + 1);
      String[] split = line.split(" ::: ");
      if (split.length != 2) {
        throw new RuntimeException("The PatternRecognizerFilter: Always two there are, no more, no less, a pattern and a replacement (separated by ' ::: ' )");
      }
      Pattern compiled = Pattern.compile(split[0]);
      patterns.put(compiled, split[1]);
      flags.put(compiled, flagsVal);
    }
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new PatternTypingFilter(input, patterns, flags);
  }
}
