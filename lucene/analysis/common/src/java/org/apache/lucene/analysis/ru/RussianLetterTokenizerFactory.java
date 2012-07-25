package org.apache.lucene.analysis.ru;

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

import java.io.Reader;
import java.util.Map;

import org.apache.lucene.analysis.ru.RussianLetterTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory; // javadocs
import org.apache.lucene.analysis.util.InitializationException;
import org.apache.lucene.analysis.util.TokenizerFactory;

/** @deprecated Use {@link StandardTokenizerFactory} instead.
 *  This tokenizer has no Russian-specific functionality.
 */
@Deprecated
public class RussianLetterTokenizerFactory extends TokenizerFactory {

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    if (args.containsKey("charset"))
      throw new InitializationException(
          "The charset parameter is no longer supported.  "
          + "Please process your documents as Unicode instead.");
    assureMatchVersion();
  }

  public RussianLetterTokenizer create(Reader in) {
    return new RussianLetterTokenizer(luceneMatchVersion,in);
  }
}

