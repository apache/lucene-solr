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
package org.apache.lucene.analysis.en;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;

/**
 * Abstract parent class for analysis factories that accept a stopwords file as input.
 *
 * <p>Concrete implementations can leverage the following input attributes. All attributes are
 * optional:
 *
 * <ul>
 *   <li><code>ignoreCase</code> defaults to <code>false</code>
 *   <li><code>words</code> should be the name of a stopwords file to parse, if not specified the
 *       factory will use the value provided by {@link #createDefaultWords()} implementation in
 *       concrete subclass.
 *   <li><code>format</code> defines how the <code>words</code> file will be parsed, and defaults to
 *       <code>wordset</code>. If <code>words</code> is not specified, then <code>format</code> must
 *       not be specified.
 * </ul>
 *
 * <p>The valid values for the <code>format</code> option are:
 *
 * <ul>
 *   <li><code>wordset</code> - This is the default format, which supports one word per line
 *       (including any intra-word whitespace) and allows whole line comments beginning with the "#"
 *       character. Blank lines are ignored. See {@link WordlistLoader#getLines
 *       WordlistLoader.getLines} for details.
 *   <li><code>snowball</code> - This format allows for multiple words specified on each line, and
 *       trailing comments may be specified using the vertical line ("&#124;"). Blank lines are
 *       ignored. See {@link WordlistLoader#getSnowballWordSet WordlistLoader.getSnowballWordSet}
 *       for details.
 * </ul>
 */
public abstract class AbstractWordsFileFilterFactory extends TokenFilterFactory
    implements ResourceLoaderAware {

  public static final String FORMAT_WORDSET = "wordset";
  public static final String FORMAT_SNOWBALL = "snowball";

  private CharArraySet words;
  private final String wordFiles;
  private final String format;
  private final boolean ignoreCase;

  /** Initialize this factory via a set of key-value pairs. */
  public AbstractWordsFileFilterFactory(Map<String, String> args) {
    super(args);
    wordFiles = get(args, "words");
    format = get(args, "format", (null == wordFiles ? null : FORMAT_WORDSET));
    ignoreCase = getBoolean(args, "ignoreCase", false);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Initialize the set of stopwords provided via ResourceLoader, or using defaults. */
  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (wordFiles != null) {
      if (FORMAT_WORDSET.equalsIgnoreCase(format)) {
        words = getWordSet(loader, wordFiles, ignoreCase);
      } else if (FORMAT_SNOWBALL.equalsIgnoreCase(format)) {
        words = getSnowballWordSet(loader, wordFiles, ignoreCase);
      } else {
        throw new IllegalArgumentException(
            "Unknown 'format' specified for 'words' file: " + format);
      }
    } else {
      if (null != format) {
        throw new IllegalArgumentException(
            "'format' can not be specified w/o an explicit 'words' file: " + format);
      }
      words = createDefaultWords();
    }
  }

  /** Default word set implementation. */
  protected abstract CharArraySet createDefaultWords();

  public CharArraySet getWords() {
    return words;
  }

  public String getWordFiles() {
    return wordFiles;
  }

  public String getFormat() {
    return format;
  }

  public boolean isIgnoreCase() {
    return ignoreCase;
  }
}

