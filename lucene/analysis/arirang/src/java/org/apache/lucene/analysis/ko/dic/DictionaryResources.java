package org.apache.lucene.analysis.ko.dic;

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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import org.apache.lucene.util.IOUtils;

/**
 * file utility class
 */
public class DictionaryResources {
  
  public static final String FILE_JOSA = "josa.dic";
  public static final String FILE_EOMI = "eomi.dic";  
  public static final String FILE_UNCOMPOUNDS = "uncompounds.dic";
  
  public static final String FILE_SYLLABLE_DAT = "syllable.dat";
  public static final String FILE_HANJA_IDX = "hanja.idx";
  public static final String FILE_HANJA_DAT = "hanja.dat";
  public static final String FILE_WORDS_DAT = "words.dat";
  public static final int DATA_VERSION = 0;

  private DictionaryResources() {}

  /**
   * Get the contents of a <code>Reader</code> invoking {@link LineProcessor}
   * for each line, removing comment lines starting with '!'.
   * @param file the name of the dictionary resource to read
   * @param processor lines are reported to this interface
   * @throws IOException if an I/O error occurs
   */
  public static void readLines(String file, LineProcessor processor) throws IOException {
    InputStream in = null;
    try {
      in = DictionaryResources.class.getResourceAsStream(file);
      if (in == null)
        throw new FileNotFoundException(file);
      readLines(IOUtils.getDecodingReader(in, IOUtils.CHARSET_UTF_8), processor);
    } finally {
      IOUtils.closeWhileHandlingException(in);
    }
  }

  private static void readLines(Reader input, LineProcessor processor) throws IOException {
    BufferedReader reader = new BufferedReader(input);
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.startsWith("!") || line.startsWith("\uFEFF!")) { // Skip comment lines starting with '!'
        continue;
      }
      if (line.isEmpty()) {
        continue;
      }
      processor.processLine(line);
    }
  }
}
