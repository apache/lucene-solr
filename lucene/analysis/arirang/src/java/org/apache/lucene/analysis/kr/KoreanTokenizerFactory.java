package org.apache.lucene.analysis.kr;

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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Version;

public class KoreanTokenizerFactory extends TokenizerFactory {

  private Version version;

  /**
   * Initialize this factory via a set of key-value pairs.
   */
  public KoreanTokenizerFactory(Map<String, String> args) {
    super(args);
  }

  @Override
  public Tokenizer create(AttributeSource.AttributeFactory factory, Reader input) {
    return new KoreanTokenizer(Version.LUCENE_50, factory, input);
  }
}
