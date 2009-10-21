package org.apache.lucene.analysis.th;

/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * {@link Analyzer} for Thai language. It uses {@link java.text.BreakIterator} to break words.
 * @version 0.2
 */
public class ThaiAnalyzer extends Analyzer {
  
  public ThaiAnalyzer() {
    setOverridesTokenStreamMethod(ThaiAnalyzer.class);
  }
  
  public TokenStream tokenStream(String fieldName, Reader reader) {
	  TokenStream ts = new StandardTokenizer(reader);
    ts = new StandardFilter(ts);
    ts = new ThaiWordFilter(ts);
    ts = new StopFilter(false, ts, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
    return ts;
  }
  
  private class SavedStreams {
    Tokenizer source;
    TokenStream result;
  };
  
  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    if (overridesTokenStreamMethod) {
      // LUCENE-1678: force fallback to tokenStream() if we
      // have been subclassed and that subclass overrides
      // tokenStream but not reusableTokenStream
      return tokenStream(fieldName, reader);
    }
    
    SavedStreams streams = (SavedStreams) getPreviousTokenStream();
    if (streams == null) {
      streams = new SavedStreams();
      streams.source = new StandardTokenizer(reader);
      streams.result = new StandardFilter(streams.source);
      streams.result = new ThaiWordFilter(streams.result);
      streams.result = new StopFilter(false, streams.result, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
      setPreviousTokenStream(streams);
    } else {
      streams.source.reset(reader);
      streams.result.reset(); // reset the ThaiWordFilter's state
    }
    return streams.result;
  }
}
