package org.apache.lucene.search.suggest;

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


import java.io.*;

import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.TermFreqIterator;


/**
 * Dictionary represented by a text file.
 * 
 * <p/>Format allowed: 1 string per line, optionally with a tab-separated integer value:<br/>
 * word1 TAB 100<br/>
 * word2 word3 TAB 101<br/>
 * word4 word5 TAB 102<br/>
 */
public class FileDictionary implements Dictionary {

  private BufferedReader in;
  private String line;
  private boolean hasNextCalled;

  public FileDictionary(InputStream dictFile) {
    in = new BufferedReader(new InputStreamReader(dictFile));
  }

  /**
   * Creates a dictionary based on a reader.
   */
  public FileDictionary(Reader reader) {
    in = new BufferedReader(reader);
  }

  public TermFreqIterator getWordsIterator() {
    return new fileIterator();
  }

  final class fileIterator implements TermFreqIterator {
    private float curFreq;
    
    public String next() {
      if (!hasNextCalled) {
        hasNext();
      }
      hasNextCalled = false;
      return line;
    }
    
    public float freq() {
      return curFreq;
    }

    public boolean hasNext() {
      hasNextCalled = true;
      try {
        line = in.readLine();
        if (line != null) {
          String[] fields = line.split("\t");
          if (fields.length > 1) {
            curFreq = Float.parseFloat(fields[1]);
            line = fields[0];
          } else {
            curFreq = 1;
          }
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      return (line != null) ? true : false;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

}
