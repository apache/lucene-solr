package org.apache.solr.uima.analysis;

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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.uima.UIMAAnnotationsTokenizer;
import org.apache.solr.analysis.BaseTokenizerFactory;

import java.io.Reader;
import java.util.Map;

/**
 * Solr {@link org.apache.solr.analysis.TokenizerFactory} for {@link UIMAAnnotationsTokenizer}
 */
public class UIMAAnnotationsTokenizerFactory extends BaseTokenizerFactory {

  private String descriptorPath;
  private String tokenType;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    descriptorPath = args.get("descriptorPath");
    tokenType = args.get("tokenType");
  }

  @Override
  public Tokenizer create(Reader input) {
    return new UIMAAnnotationsTokenizer(descriptorPath, tokenType, input);
  }
}
