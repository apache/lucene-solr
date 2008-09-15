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

import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Analyzer for Thai language. It uses java.text.BreakIterator to break words.
 * @version 0.2
 */
public class ThaiAnalyzer extends Analyzer {
  public TokenStream tokenStream(String fieldName, Reader reader) {
	  TokenStream ts = new StandardTokenizer(reader);
    ts = new StandardFilter(ts);
    ts = new ThaiWordFilter(ts);
    ts = new StopFilter(ts, StopAnalyzer.ENGLISH_STOP_WORDS);
    return ts;
  }
}
