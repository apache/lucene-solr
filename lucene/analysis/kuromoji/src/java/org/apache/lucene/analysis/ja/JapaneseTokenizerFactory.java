package org.apache.lucene.analysis.ja;

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
import org.apache.lucene.util.AttributeSource.AttributeFactory;
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
 *     /&gt;
 *     &lt;filter class="solr.JapaneseBaseFormFilterFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;
 * </pre>
 */
public class JapaneseTokenizerFactory extends TokenizerFactory implements ResourceLoaderAware {
  private static final String MODE = "mode";
  
  private static final String USER_DICT_PATH = "userDictionary";
  
  private static final String USER_DICT_ENCODING = "userDictionaryEncoding";

  private static final String DISCARD_PUNCTUATION = "discardPunctuation"; // Expert option

  private UserDictionary userDictionary;

  private final Mode mode;
  private final boolean discardPunctuation;  
  private final String userDictionaryPath;
  private final String userDictionaryEncoding;

  /** Creates a new JapaneseTokenizerFactory */
  public JapaneseTokenizerFactory(Map<String,String> args) {
    super(args);
    mode = Mode.valueOf(get(args, MODE, JapaneseTokenizer.DEFAULT_MODE.toString()).toUpperCase(Locale.ROOT));
    userDictionaryPath = args.remove(USER_DICT_PATH);
    userDictionaryEncoding = args.remove(USER_DICT_ENCODING);
    discardPunctuation = getBoolean(args, DISCARD_PUNCTUATION, true);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }
  
  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (userDictionaryPath != null) {
      InputStream stream = loader.openResource(userDictionaryPath);
      String encoding = userDictionaryEncoding;
      if (encoding == null) {
        encoding = IOUtils.UTF_8;
      }
      CharsetDecoder decoder = Charset.forName(encoding).newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT);
      Reader reader = new InputStreamReader(stream, decoder);
      userDictionary = new UserDictionary(reader);
    } else {
      userDictionary = null;
    }
  }
  
  @Override
  public JapaneseTokenizer create(AttributeFactory factory, Reader input) {
    return new JapaneseTokenizer(factory, input, userDictionary, discardPunctuation, mode);
  }
}
