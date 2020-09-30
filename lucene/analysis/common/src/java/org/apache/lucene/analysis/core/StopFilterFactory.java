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


import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;
import org.apache.lucene.analysis.TokenFilterFactory;

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
 *      specified the factory will use {@link EnglishAnalyzer#ENGLISH_STOP_WORDS_SET}
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
 *      beginning with the "#" character.  Blank lines are ignored.  See 
 *      {@link WordlistLoader#getLines WordlistLoader.getLines} for details.
 *  </li>
 *  <li><code>snowball</code> - This format allows for multiple words specified on each 
 *      line, and trailing comments may be specified using the vertical line ("&#124;"). 
 *      Blank lines are ignored.  See 
 *      {@link WordlistLoader#getSnowballWordSet WordlistLoader.getSnowballWordSet} 
 *      for details.
 *  </li>
 * </ul>
 *
 * @since 3.1
 * @lucene.spi {@value #NAME}
 */
public class StopFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {

  /** SPI name */
  public static final String NAME = "stop";

  public static final String FORMAT_WORDSET = "wordset";
  public static final String FORMAT_SNOWBALL = "snowball";
  
  private CharArraySet stopWords;
  private final String stopWordFiles;
  private final String format;
  private final boolean ignoreCase;
  
  /** Creates a new StopFilterFactory */
  public StopFilterFactory(Map<String,String> args) {
    super(args);
    stopWordFiles = get(args, "words");
    format = get(args, "format", (null == stopWordFiles ? null : FORMAT_WORDSET));
    ignoreCase = getBoolean(args, "ignoreCase", false);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public StopFilterFactory() {
    throw defaultCtorException();
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
      stopWords = new CharArraySet(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, ignoreCase);
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
    StopFilter stopFilter = new StopFilter(input,stopWords);
    return stopFilter;
  }
}
