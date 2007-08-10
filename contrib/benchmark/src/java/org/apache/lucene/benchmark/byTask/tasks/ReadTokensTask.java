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

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import java.text.NumberFormat;
import java.io.Reader;
import java.util.List;


/**
 * Simple task to test performance of tokenizers.  It just
 * creates a token stream for each field of the document and
 * read all tokens out of that stream.
 * <br>Relevant properties: <code>doc.tokenize.log.step</code>.
 */
public class ReadTokensTask extends PerfTask {

  /**
   * Default value for property <code>doc.tokenize.log.step<code> - indicating how often 
   * an "added N docs / M tokens" message should be logged.  
   */
  public static final int DEFAULT_DOC_LOG_STEP = 500;

  public ReadTokensTask(PerfRunData runData) {
    super(runData);
  }

  private int logStep = -1;
  int count = 0;
  int totalTokenCount = 0;
  
  // volatile data passed between setup(), doLogic(), tearDown().
  private Document doc = null;
  
  /*
   *  (non-Javadoc)
   * @see PerfTask#setup()
   */
  public void setup() throws Exception {
    super.setup();
    DocMaker docMaker = getRunData().getDocMaker();
    doc = docMaker.makeDocument();
  }

  /* (non-Javadoc)
   * @see PerfTask#tearDown()
   */
  public void tearDown() throws Exception {
    log(++count);
    doc = null;
    super.tearDown();
  }

  Token token = new Token();

  public int doLogic() throws Exception {
    List fields = doc.getFields();
    final int numField = fields.size();
    Analyzer analyzer = getRunData().getAnalyzer();
    int tokenCount = 0;
    for(int i=0;i<numField;i++) {
      final Field field = (Field) fields.get(i);
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

      while(stream.next(token) != null)
        tokenCount++;
    }
    totalTokenCount += tokenCount;
    return tokenCount;
  }

  private void log(int count) {
    if (logStep<0) {
      // init once per instance
      logStep = getRunData().getConfig().get("doc.tokenize.log.step", DEFAULT_DOC_LOG_STEP);
    }
    if (logStep>0 && (count%logStep)==0) {
      double seconds = (System.currentTimeMillis() - getRunData().getStartTimeMillis())/1000.0;
      NumberFormat nf = NumberFormat.getInstance();
      nf.setMaximumFractionDigits(2);
      System.out.println("--> "+nf.format(seconds) + " sec: " + Thread.currentThread().getName()+" processed (add) "+count+" docs" + "; " + totalTokenCount + " tokens");
    }
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
    public int read(char[] c) {
      return read(c, 0, c.length);
    }
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
    public void close() {};
  }
}
