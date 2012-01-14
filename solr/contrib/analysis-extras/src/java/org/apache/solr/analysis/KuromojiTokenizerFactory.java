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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Locale;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.kuromoji.KuromojiTokenizer;
import org.apache.lucene.analysis.kuromoji.Segmenter;
import org.apache.lucene.analysis.kuromoji.Segmenter.Mode;
import org.apache.lucene.analysis.kuromoji.dict.UserDictionary;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.analysis.BaseTokenizerFactory;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.plugin.ResourceLoaderAware;

/**
 * Factory for {@link KuromojiTokenizer}.  
 * <pre class="prettyprint">
 * &lt;fieldType name="text_ja" class="solr.TextField"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.KuromojiTokenizerFactory"
 *       mode=NORMAL
 *       user-dictionary=user.txt
 *       user-dictionary-encoding=UTF-8
 *     /&gt;
 *     &lt;filter class="solr.KuromojiBaseFormFilterFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;
 * </pre>
 */
public class KuromojiTokenizerFactory extends BaseTokenizerFactory implements ResourceLoaderAware {
  private static final String MODE = "mode";
  
  private static final String USER_DICT_PATH = "user-dictionary";
  
  private static final String USER_DICT_ENCODING = "user-dictionary-encoding";

  private Segmenter segmenter;
  
  @Override
  public void inform(ResourceLoader loader) {
    Mode mode = args.get(MODE) != null ? Mode.valueOf(args.get(MODE).toUpperCase(Locale.ENGLISH)) : Mode.NORMAL;
    String userDictionaryPath = args.get(USER_DICT_PATH);
    try {
      if (userDictionaryPath != null) {
        InputStream stream = loader.openResource(userDictionaryPath);
        String encoding = args.get(USER_DICT_ENCODING);
        if (encoding == null) {
          encoding = IOUtils.UTF_8;
        }
        // note: we could allow for other encodings here as an argument
        CharsetDecoder decoder = Charset.forName(encoding).newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);
        Reader reader = new InputStreamReader(stream, decoder);
        this.segmenter = new Segmenter(new UserDictionary(reader), mode);
      } else {
        this.segmenter = new Segmenter(mode);
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }
  
  @Override
  public Tokenizer create(Reader input) {
    return new KuromojiTokenizer(segmenter, input);
  }
}