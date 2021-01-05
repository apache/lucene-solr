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
package org.apache.lucene.search.spell;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.IOUtils;

/**
 * Dictionary represented by a text file.
 *
 * <p>Format allowed: 1 word per line:<br>
 * word1<br>
 * word2<br>
 * word3<br>
 */
public class PlainTextDictionary implements Dictionary {

  private BufferedReader in;

  /**
   * Creates a dictionary based on a Path.
   *
   * <p>NOTE: content is treated as UTF-8
   */
  public PlainTextDictionary(Path path) throws IOException {
    in = Files.newBufferedReader(path, StandardCharsets.UTF_8);
  }

  /**
   * Creates a dictionary based on an inputstream.
   *
   * <p>NOTE: content is treated as UTF-8
   */
  public PlainTextDictionary(InputStream dictFile) {
    in = new BufferedReader(IOUtils.getDecodingReader(dictFile, StandardCharsets.UTF_8));
  }

  /** Creates a dictionary based on a reader. */
  public PlainTextDictionary(Reader reader) {
    in = new BufferedReader(reader);
  }

  @Override
  public InputIterator getEntryIterator() throws IOException {
    return new InputIterator.InputIteratorWrapper(new FileIterator());
  }

  final class FileIterator implements BytesRefIterator {
    private boolean done = false;
    private final BytesRefBuilder spare = new BytesRefBuilder();

    @Override
    public BytesRef next() throws IOException {
      if (done) {
        return null;
      }
      boolean success = false;
      BytesRef result;
      try {
        String line;
        if ((line = in.readLine()) != null) {
          spare.copyChars(line);
          result = spare.get();
        } else {
          done = true;
          IOUtils.close(in);
          result = null;
        }
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(in);
        }
      }
      return result;
    }
  }
}
