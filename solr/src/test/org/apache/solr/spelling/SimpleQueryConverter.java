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
package org.apache.solr.spelling;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.TokenStream;

import java.util.Collection;
import java.util.HashSet;
import java.io.StringReader;
import java.io.IOException;


/**
 *
 * @since solr 1.3
 **/
class SimpleQueryConverter extends SpellingQueryConverter{
  @Override
  public Collection<Token> convert(String origQuery) {
    Collection<Token> result = new HashSet<Token>();
    WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer();
    TokenStream ts = analyzer.tokenStream("", new StringReader(origQuery));
    Token tok = null;
    try {
      while ((tok = ts.next()) != null){
        result.add(tok);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return result;
  }
}
