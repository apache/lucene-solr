package org.apache.lucene.search.spell;

/**
 * Copyright 2002-2005 The Apache Software Foundation
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


import java.util.Iterator;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.*;


/**
 * Dictionary represented by a file text.
 * 
 * <p/>Format allowed: 1 word per line:<br/>
 * word1<br/>
 * word2<br/>
 * word3<br/>
 *
 * @author Nicolas Maisonneuve
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
        ex.printStackTrace();
        line = null;
        return false;
      }
      return (line != null) ? true : false;
    }


    public void remove() {
    }
  }

}
