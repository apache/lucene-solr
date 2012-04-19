package org.apache.lucene.analysis.core;

import java.io.IOException;
import java.nio.CharBuffer;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharStream;

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

public class TestBugInSomething extends BaseTokenStreamTestCase {
  
  CharStream wrappedStream = new CharStream() {

    @Override
    public void mark(int readAheadLimit) throws IOException {
      throw new UnsupportedOperationException("mark(int)");
    }

    @Override
    public boolean markSupported() {
      throw new UnsupportedOperationException("markSupported()");
    }

    @Override
    public int read() throws IOException {
      throw new UnsupportedOperationException("read()");
    }

    @Override
    public int read(char[] cbuf) throws IOException {
      throw new UnsupportedOperationException("read(char[])");
    }

    @Override
    public int read(CharBuffer target) throws IOException {
      throw new UnsupportedOperationException("read(CharBuffer)");
    }

    @Override
    public boolean ready() throws IOException {
      throw new UnsupportedOperationException("ready()");
    }

    @Override
    public void reset() throws IOException {
      throw new UnsupportedOperationException("reset()");
    }

    @Override
    public long skip(long n) throws IOException {
      throw new UnsupportedOperationException("skip(long)");
    }

    @Override
    public int correctOffset(int currentOff) {
      throw new UnsupportedOperationException("correctOffset(int)");
    }

    @Override
    public void close() throws IOException {
      throw new UnsupportedOperationException("close()");
    }

    @Override
    public int read(char[] arg0, int arg1, int arg2) throws IOException {
      throw new UnsupportedOperationException("read(char[], int, int)");
    }
  };
  
  public void testWrapping() throws Exception {
    CharStream cs = new TestRandomChains.CheckThatYouDidntReadAnythingReaderWrapper(wrappedStream);
    try {
      cs.mark(1);
      fail();
    } catch (Exception e) {
      assertEquals("mark(int)", e.getMessage());
    }
    
    try {
      cs.markSupported();
      fail();
    } catch (Exception e) {
      assertEquals("markSupported()", e.getMessage());
    }
    
    try {
      cs.read();
      fail();
    } catch (Exception e) {
      assertEquals("read()", e.getMessage());
    }
    
    try {
      cs.read(new char[0]);
      fail();
    } catch (Exception e) {
      assertEquals("read(char[])", e.getMessage());
    }
    
    try {
      cs.read(CharBuffer.wrap(new char[0]));
      fail();
    } catch (Exception e) {
      assertEquals("read(CharBuffer)", e.getMessage());
    }
    
    try {
      cs.reset();
      fail();
    } catch (Exception e) {
      assertEquals("reset()", e.getMessage());
    }
    
    try {
      cs.skip(1);
      fail();
    } catch (Exception e) {
      assertEquals("skip(long)", e.getMessage());
    }
    
    try {
      cs.correctOffset(1);
      fail();
    } catch (Exception e) {
      assertEquals("correctOffset(int)", e.getMessage());
    }
    
    try {
      cs.close();
      fail();
    } catch (Exception e) {
      assertEquals("close()", e.getMessage());
    }
    
    try {
      cs.read(new char[0], 0, 0);
      fail();
    } catch (Exception e) {
      assertEquals("read(char[], int, int)", e.getMessage());
    }
  }
}
