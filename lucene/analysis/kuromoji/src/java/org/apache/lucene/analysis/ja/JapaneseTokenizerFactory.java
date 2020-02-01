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
package org.apache.lucene.analysis.ja;


import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.ja.JapaneseTokenizer.Mode;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;

/**
 * Factory for {@link org.apache.lucene.analysis.ja.JapaneseTokenizer}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_ja" class="solr.TextField"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.JapaneseTokenizerFactory"
 *       mode="NORMAL"
 *       userDictionary="user.txt"
 *       userDictionaryEncoding="UTF-8"
 *       discardPunctuation="true"
 *       discardCompoundToken="false"
 *     /&gt;
 *     &lt;filter class="solr.JapaneseBaseFormFilterFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;
 * </pre>
 * <p>
 * Additional expert user parameters nBestCost and nBestExamples can be
 * used to include additional searchable tokens that those most likely
 * according to the statistical model. A typical use-case for this is to
 * improve recall and make segmentation more resilient to mistakes.
 * The feature can also be used to get a decompounding effect.
 * <p>
 * The nBestCost parameter specifies an additional Viterbi cost, and
 * when used, JapaneseTokenizer will include all tokens in Viterbi paths
 * that are within the nBestCost value of the best path.
 * <p>
 * Finding a good value for nBestCost can be difficult to do by hand. The
 * nBestExamples parameter can be used to find an nBestCost value based on
 * examples with desired segmentation outcomes.
 * <p>
 * For example, a value of /箱根山-箱根/成田空港-成田/ indicates that in
 * the texts, 箱根山 (Mt. Hakone) and 成田空港 (Narita Airport) we'd like
 * a cost that gives is us 箱根 (Hakone) and 成田 (Narita). Notice that
 * costs are estimated for each example individually, and the maximum
 * nBestCost found across all examples is used.
 * <p>
 * If both nBestCost and nBestExamples is used in a configuration,
 * the largest value of the two is used.
 * <p>
 * Parameters nBestCost and nBestExamples work with all tokenizer
 * modes, but it makes the most sense to use them with NORMAL mode.
 *
 * @since 3.6.0
 * @lucene.spi {@value #NAME}
 */
public class JapaneseTokenizerFactory extends TokenizerFactory implements ResourceLoaderAware {

  /** SPI name */
  public static final String NAME = "japanese";

  private static final String MODE = "mode";

  private static final String USER_DICT_PATH = "userDictionary";

  private static final String USER_DICT_ENCODING = "userDictionaryEncoding";

  private static final String DISCARD_PUNCTUATION = "discardPunctuation"; // Expert option

  private static final String DISCARD_COMPOUND_TOKEN = "discardCompoundToken"; // Expert option

  private static final String NBEST_COST = "nBestCost";

  private static final String NBEST_EXAMPLES = "nBestExamples";

  private UserDictionary userDictionary;

  private final Mode mode;
  private final boolean discardPunctuation;
  private final boolean discardCompoundToken;
  private final String userDictionaryPath;
  private final String userDictionaryEncoding;

  /* Example string for NBEST output.
   * its form as:
   *   nbestExamples := [ / ] example [ / example ]... [ / ]
   *   example := TEXT - TOKEN
   *   TEXT := input text
   *   TOKEN := token should be in nbest result
   * Ex. /箱根山-箱根/成田空港-成田/
   * When the result tokens are "箱根山", "成田空港" in NORMAL mode,
   * /箱根山-箱根/成田空港-成田/ requests "箱根" and "成田" to be in the result in NBEST output.
   */
  private final String nbestExamples;
  private int nbestCost = -1;

  /** Creates a new JapaneseTokenizerFactory */
  public JapaneseTokenizerFactory(Map<String,String> args) {
    super(args);
    mode = Mode.valueOf(get(args, MODE, JapaneseTokenizer.DEFAULT_MODE.toString()).toUpperCase(Locale.ROOT));
    userDictionaryPath = args.remove(USER_DICT_PATH);
    userDictionaryEncoding = args.remove(USER_DICT_ENCODING);
    discardPunctuation = getBoolean(args, DISCARD_PUNCTUATION, true);
    discardCompoundToken = getBoolean(args, DISCARD_COMPOUND_TOKEN, false);
    nbestCost = getInt(args, NBEST_COST, 0);
    nbestExamples = args.remove(NBEST_EXAMPLES);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (userDictionaryPath != null) {
      try (InputStream stream = loader.openResource(userDictionaryPath)) {
        String encoding = userDictionaryEncoding;
        if (encoding == null) {
          encoding = IOUtils.UTF_8;
        }
        CharsetDecoder decoder = Charset.forName(encoding).newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT);
        Reader reader = new InputStreamReader(stream, decoder);
        userDictionary = UserDictionary.open(reader);
      }
    } else {
      userDictionary = null;
    }
  }

  @Override
  public JapaneseTokenizer create(AttributeFactory factory) {
    JapaneseTokenizer t = new JapaneseTokenizer(factory, userDictionary, discardPunctuation, discardCompoundToken, mode);
    if (nbestExamples != null) {
      nbestCost = Math.max(nbestCost, t.calcNBestCost(nbestExamples));
    }
    t.setNBestCost(nbestCost);
    return t;
  }
}
