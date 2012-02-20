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

package org.apache.lucene.spatial.base.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;

public abstract class LineReader<T> implements Iterator<T> {

  private int count = 0;
  private int lineNumber = 0;
  private BufferedReader reader;
  private String nextLine;

  public abstract T parseLine( String line );

  protected void readComment( String line ) {

  }

  public LineReader(InputStream in) throws IOException {
    reader = new BufferedReader(
        new InputStreamReader( in, "UTF-8" ) );
    next();
  }

  public LineReader(Reader r) throws IOException {
    if (r instanceof BufferedReader) {
      reader = (BufferedReader) r;
    } else {
      reader = new BufferedReader(r);
    }
    next();
  }

  public LineReader(File f) throws IOException {
    reader = new BufferedReader(new InputStreamReader(new FileInputStream(f), "UTF-8"));
    next();
  }

  @Override
  public boolean hasNext() {
    return nextLine != null;
  }

  @Override
  public T next() {
    T val = null;
    if (nextLine != null) {
      val = parseLine(nextLine);
      count++;
    }

    if (reader != null) {
      try {
        while( reader != null ) {
          nextLine = reader.readLine();
          lineNumber++;
          if (nextLine == null ) {
            reader.close();
            reader = null;
          }
          else if( nextLine.startsWith( "#" ) ) {
            readComment( nextLine );
          }
          else {
            nextLine = nextLine.trim();
            if( nextLine.length() > 0 ) {
              break;
            }
          }
        }
      } catch (IOException ioe) {
        throw new RuntimeException("IOException thrown while reading/closing reader", ioe);
      }
    }
    return val;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  public int getLineNumber() {
    return lineNumber;
  }

  public int getCount() {
    return count;
  }
}
