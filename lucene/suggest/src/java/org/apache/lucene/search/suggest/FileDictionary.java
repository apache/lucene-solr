package org.apache.lucene.search.suggest;

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


import java.io.*;
import java.util.Comparator;

import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.TermFreqIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;


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
  private boolean done = false;

  /**
   * Creates a dictionary based on an inputstream.
   * <p>
   * NOTE: content is treated as UTF-8
   */
  public FileDictionary(InputStream dictFile) {
    in = new BufferedReader(IOUtils.getDecodingReader(dictFile, IOUtils.CHARSET_UTF_8));
  }

  /**
   * Creates a dictionary based on a reader.
   */
  public FileDictionary(Reader reader) {
    in = new BufferedReader(reader);
  }

  @Override
  public TermFreqIterator getWordsIterator() {
    return new FileIterator();
  }

  final class FileIterator implements TermFreqIterator {
    private long curFreq;
    private final BytesRef spare = new BytesRef();
    
   
    @Override
    public long weight() {
      return curFreq;
    }

    @Override
    public BytesRef next() throws IOException {
      if (done) {
        return null;
      }
      line = in.readLine();
      if (line != null) {
        String[] fields = line.split("\t");
        if (fields.length > 1) {
          // keep reading floats for bw compat
          try {
            curFreq = Long.parseLong(fields[1]);
          } catch (NumberFormatException e) {
            curFreq = (long)Double.parseDouble(fields[1]);
          }
          spare.copyChars(fields[0]);
        } else {
          spare.copyChars(line);
          curFreq = 1;
        }
        return spare;
      } else {
        done = true;
        IOUtils.close(in);
        return null;
      }
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return null;
    }
  }

}
