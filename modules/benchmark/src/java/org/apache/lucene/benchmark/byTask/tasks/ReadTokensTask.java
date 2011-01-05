package org.apache.lucene.benchmark.byTask.tasks;

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
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;

/**
 * Simple task to test performance of tokenizers.  It just
 * creates a token stream for each field of the document and
 * read all tokens out of that stream.
 */
public class ReadTokensTask extends PerfTask {

  public ReadTokensTask(PerfRunData runData) {
    super(runData);
  }

  private int totalTokenCount = 0;
  
  // volatile data passed between setup(), doLogic(), tearDown().
  private Document doc = null;
  
  @Override
  public void setup() throws Exception {
    super.setup();
    DocMaker docMaker = getRunData().getDocMaker();
    doc = docMaker.makeDocument();
  }

  @Override
  protected String getLogMessage(int recsCount) {
    return "read " + recsCount + " docs; " + totalTokenCount + " tokens";
  }
  
  @Override
  public void tearDown() throws Exception {
    doc = null;
    super.tearDown();
  }

  @Override
  public int doLogic() throws Exception {
    List<Fieldable> fields = doc.getFields();
    Analyzer analyzer = getRunData().getAnalyzer();
    int tokenCount = 0;
    for(final Fieldable field : fields) {
      if (!field.isTokenized() || field instanceof NumericField) continue;
      
      final TokenStream stream;
      final TokenStream streamValue = field.tokenStreamValue();

      if (streamValue != null) 
        stream = streamValue;
      else {
        // the field does not have a TokenStream,
        // so we have to obtain one from the analyzer
        final Reader reader;			  // find or make Reader
        final Reader readerValue = field.readerValue();

        if (readerValue != null)
          reader = readerValue;
        else {
          String stringValue = field.stringValue();
          if (stringValue == null)
            throw new IllegalArgumentException("field must have either TokenStream, String or Reader value");
          stringReader.init(stringValue);
          reader = stringReader;
        }
        
        // Tokenize field
        stream = analyzer.reusableTokenStream(field.name(), reader);
      }

      // reset the TokenStream to the first token
      stream.reset();

      while(stream.incrementToken())
        tokenCount++;
    }
    totalTokenCount += tokenCount;
    return tokenCount;
  }

  /* Simple StringReader that can be reset to a new string;
   * we use this when tokenizing the string value from a
   * Field. */
  ReusableStringReader stringReader = new ReusableStringReader();

  private final static class ReusableStringReader extends Reader {
    int upto;
    int left;
    String s;
    void init(String s) {
      this.s = s;
      left = s.length();
      this.upto = 0;
    }
    @Override
    public int read(char[] c) {
      return read(c, 0, c.length);
    }
    @Override
    public int read(char[] c, int off, int len) {
      if (left > len) {
        s.getChars(upto, upto+len, c, off);
        upto += len;
        left -= len;
        return len;
      } else if (0 == left) {
        return -1;
      } else {
        s.getChars(upto, upto+left, c, off);
        int r = left;
        left = 0;
        upto = s.length();
        return r;
      }
    }
    @Override
    public void close() {}
  }
}
