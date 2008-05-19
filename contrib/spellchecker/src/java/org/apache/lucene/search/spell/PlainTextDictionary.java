package org.apache.lucene.search.spell;

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


import java.util.Iterator;
import java.io.*;


/**
 * Dictionary represented by a text file.
 * 
 * <p/>Format allowed: 1 word per line:<br/>
 * word1<br/>
 * word2<br/>
 * word3<br/>
 */
public class PlainTextDictionary implements Dictionary {

  private BufferedReader in;
  private String line;
  private boolean hasNextCalled;

  public PlainTextDictionary(File file) throws FileNotFoundException {
    in = new BufferedReader(new FileReader(file));
  }

  public PlainTextDictionary(InputStream dictFile) {
    in = new BufferedReader(new InputStreamReader(dictFile));
  }

  /**
   * Creates a dictionary based on a reader.
   */
  public PlainTextDictionary(Reader reader) {
    in = new BufferedReader(reader);
  }

  public Iterator getWordsIterator() {
    return new fileIterator();
  }

  final class fileIterator implements Iterator {
    public Object next() {
      if (!hasNextCalled) {
        hasNext();
      }
      hasNextCalled = false;
      return line;
    }

    public boolean hasNext() {
      hasNextCalled = true;
      try {
        line = in.readLine();
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
