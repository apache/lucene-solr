package org.apache.lucene.analysis.snowball;

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

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.analysis.tr.TurkishLowerCaseFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

import java.io.Reader;

/** Filters {@link StandardTokenizer} with {@link StandardFilter}, {@link
 * LowerCaseFilter}, {@link StopFilter} and {@link SnowballFilter}.
 *
 * Available stemmers are listed in org.tartarus.snowball.ext.  The name of a
 * stemmer is the part of the class name before "Stemmer", e.g., the stemmer in
 * {@link org.tartarus.snowball.ext.EnglishStemmer} is named "English".
 *
 * <p><b>NOTE</b>: This class uses the same {@link Version}
 * dependent settings as {@link StandardAnalyzer}, with the following addition:
 * <ul>
 *   <li> As of 3.1, uses {@link TurkishLowerCaseFilter} for Turkish language.
 * </ul>
 * </p>
 * @deprecated (3.1) Use the language-specific analyzer in modules/analysis instead. 
 * This analyzer will be removed in Lucene 5.0
 */
@Deprecated
public final class SnowballAnalyzer extends Analyzer {
  private String name;
  private CharArraySet stopSet;
  private final Version matchVersion;

  /** Builds the named analyzer with no stop words. */
  public SnowballAnalyzer(Version matchVersion, String name) {
    this.name = name;
    this.matchVersion = matchVersion;
  }

  /** Builds the named analyzer with the given stop words. */
  public SnowballAnalyzer(Version matchVersion, String name, CharArraySet stopWords) {
    this(matchVersion, name);
    stopSet = CharArraySet.unmodifiableSet(CharArraySet.copy(matchVersion,
        stopWords));
  }

  /** Constructs a {@link StandardTokenizer} filtered by a {@link
      StandardFilter}, a {@link LowerCaseFilter}, a {@link StopFilter},
      and a {@link SnowballFilter} */
  @Override
  public TokenStreamComponents createComponents(String fieldName, Reader reader) {
    Tokenizer tokenizer = new StandardTokenizer(matchVersion, reader);
    TokenStream result = new StandardFilter(matchVersion, tokenizer);
    // remove the possessive 's for english stemmers
    if (matchVersion.onOrAfter(Version.LUCENE_31) && 
        (name.equals("English") || name.equals("Porter") || name.equals("Lovins")))
      result = new EnglishPossessiveFilter(result);
    // Use a special lowercase filter for turkish, the stemmer expects it.
    if (matchVersion.onOrAfter(Version.LUCENE_31) && name.equals("Turkish"))
      result = new TurkishLowerCaseFilter(result);
    else
      result = new LowerCaseFilter(matchVersion, result);
    if (stopSet != null)
      result = new StopFilter(matchVersion,
                              result, stopSet);
    result = new SnowballFilter(result, name);
    return new TokenStreamComponents(tokenizer, result);
  }
}
