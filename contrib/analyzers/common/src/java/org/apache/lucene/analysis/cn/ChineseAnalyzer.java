package org.apache.lucene.analysis.cn;

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

import java.io.IOException;
import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

/**
 * An {@link Analyzer} that tokenizes text with {@link ChineseTokenizer} and
 * filters with {@link ChineseFilter}
 *
 */

public class ChineseAnalyzer extends Analyzer {

    public ChineseAnalyzer() {
    }

    /**
    * Creates a {@link TokenStream} which tokenizes all the text in the provided {@link Reader}.
    *
    * @return  A {@link TokenStream} built from a {@link ChineseTokenizer} 
    *   filtered with {@link ChineseFilter}.
    */
    public final TokenStream tokenStream(String fieldName, Reader reader) {
        TokenStream result = new ChineseTokenizer(reader);
        result = new ChineseFilter(result);
        return result;
    }
    
    private class SavedStreams {
      Tokenizer source;
      TokenStream result;
    };

    /**
    * Returns a (possibly reused) {@link TokenStream} which tokenizes all the text in the
    * provided {@link Reader}.
    * 
    * @return A {@link TokenStream} built from a {@link ChineseTokenizer} 
    *   filtered with {@link ChineseFilter}.
    */
    public final TokenStream reusableTokenStream(String fieldName, Reader reader)
      throws IOException {
      /* tokenStream() is final, no back compat issue */
      SavedStreams streams = (SavedStreams) getPreviousTokenStream();
      if (streams == null) {
        streams = new SavedStreams();
        streams.source = new ChineseTokenizer(reader);
        streams.result = new ChineseFilter(streams.source);
        setPreviousTokenStream(streams);
      } else {
        streams.source.reset(reader);
      }
      return streams.result;
    }
}