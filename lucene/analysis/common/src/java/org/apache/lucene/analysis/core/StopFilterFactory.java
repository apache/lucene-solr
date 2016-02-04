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
package org.apache.lucene.analysis.core;


import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.WordlistLoader; // jdocs
import org.apache.lucene.util.Version;

import java.util.Map;
import java.io.IOException;

/**
 * Factory for {@link StopFilter}.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_stop" class="solr.TextField" positionIncrementGap="100" autoGeneratePhraseQueries="true"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.StopFilterFactory" ignoreCase="true"
 *             words="stopwords.txt" format="wordset"
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * <p>
 * All attributes are optional:
 * </p>
 * <ul>
 *  <li><code>ignoreCase</code> defaults to <code>false</code></li>
 *  <li><code>words</code> should be the name of a stopwords file to parse, if not 
 *      specified the factory will use {@link StopAnalyzer#ENGLISH_STOP_WORDS_SET}
 *  </li>
 *  <li><code>format</code> defines how the <code>words</code> file will be parsed, 
 *      and defaults to <code>wordset</code>.  If <code>words</code> is not specified, 
 *      then <code>format</code> must not be specified.
 *  </li>
 * </ul>
 * <p>
 * The valid values for the <code>format</code> option are:
 * </p>
 * <ul>
 *  <li><code>wordset</code> - This is the default format, which supports one word per 
 *      line (including any intra-word whitespace) and allows whole line comments 
 *      begining with the "#" character.  Blank lines are ignored.  See 
 *      {@link WordlistLoader#getLines WordlistLoader.getLines} for details.
 *  </li>
 *  <li><code>snowball</code> - This format allows for multiple words specified on each 
 *      line, and trailing comments may be specified using the vertical line ("&#124;"). 
 *      Blank lines are ignored.  See 
 *      {@link WordlistLoader#getSnowballWordSet WordlistLoader.getSnowballWordSet} 
 *      for details.
 *  </li>
 * </ul>
 */
public class StopFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  public static final String FORMAT_WORDSET = "wordset";
  public static final String FORMAT_SNOWBALL = "snowball";
  
  private CharArraySet stopWords;
  private final String stopWordFiles;
  private final String format;
  private final boolean ignoreCase;
  private boolean enablePositionIncrements;
  
  /** Creates a new StopFilterFactory */
  public StopFilterFactory(Map<String,String> args) {
    super(args);
    stopWordFiles = get(args, "words");
    format = get(args, "format", (null == stopWordFiles ? null : FORMAT_WORDSET));
    ignoreCase = getBoolean(args, "ignoreCase", false);

    if (luceneMatchVersion.onOrAfter(Version.LUCENE_5_0_0) == false) {
      boolean defaultValue = luceneMatchVersion.onOrAfter(Version.LUCENE_4_4_0);
      enablePositionIncrements = getBoolean(args, "enablePositionIncrements", defaultValue);
      if (enablePositionIncrements == false && luceneMatchVersion.onOrAfter(Version.LUCENE_4_4_0)) {
        throw new IllegalArgumentException("enablePositionIncrements=false is not supported anymore as of Lucene 4.4");
      }
    } else if (args.containsKey("enablePositionIncrements")) {
      throw new IllegalArgumentException("enablePositionIncrements is not a valid option as of Lucene 5.0");
    }
    
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (stopWordFiles != null) {
      if (FORMAT_WORDSET.equalsIgnoreCase(format)) {
        stopWords = getWordSet(loader, stopWordFiles, ignoreCase);
      } else if (FORMAT_SNOWBALL.equalsIgnoreCase(format)) {
        stopWords = getSnowballWordSet(loader, stopWordFiles, ignoreCase);
      } else {
        throw new IllegalArgumentException("Unknown 'format' specified for 'words' file: " + format);
      }
    } else {
      if (null != format) {
        throw new IllegalArgumentException("'format' can not be specified w/o an explicit 'words' file: " + format);
      }
      stopWords = new CharArraySet(StopAnalyzer.ENGLISH_STOP_WORDS_SET, ignoreCase);
    }
  }

  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  public CharArraySet getStopWords() {
    return stopWords;
  }

  @Override
  public TokenStream create(TokenStream input) {
    if (luceneMatchVersion.onOrAfter(Version.LUCENE_4_4_0)) {
      return new StopFilter(input, stopWords);
    } else {
      @SuppressWarnings("deprecation")
      final TokenStream filter = new Lucene43StopFilter(enablePositionIncrements, input, stopWords);
      return filter;
    }
  }
}
