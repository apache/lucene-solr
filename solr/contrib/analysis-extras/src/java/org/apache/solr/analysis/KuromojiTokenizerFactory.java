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

import java.io.Reader;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.kuromoji.KuromojiTokenizer;
import org.apache.lucene.analysis.kuromoji.Segmenter;
import org.apache.lucene.analysis.kuromoji.Segmenter.Mode;
import org.apache.lucene.analysis.kuromoji.dict.UserDictionary;
import org.apache.solr.analysis.BaseTokenizerFactory;
import org.apache.solr.common.SolrException;

public class KuromojiTokenizerFactory extends BaseTokenizerFactory{
  private static final String MODE = "mode";
  
  private static final String USER_DICT_PATH = "user-dictionary";
  
  private Segmenter segmenter;
  
  @Override
  public void init(Map<String,String> args) {
    this.args = args;
    Mode mode = args.get(MODE) != null ? Mode.valueOf(args.get(MODE).toUpperCase(Locale.ENGLISH)) : Mode.NORMAL;
    String userDictionaryPath = args.get(USER_DICT_PATH);
    try {
      this.segmenter = new Segmenter(new UserDictionary(userDictionaryPath), mode);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }
  
  @Override
  public Tokenizer create(Reader input) {
    return new KuromojiTokenizer(segmenter, input);
  }
}