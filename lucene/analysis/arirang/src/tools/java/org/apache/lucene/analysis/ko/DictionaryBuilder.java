package org.apache.lucene.analysis.ko;

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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class DictionaryBuilder {
  public static void main(String args[]) throws Exception {
    String FILES_AS_IS[] = { "compounds.dic", "dictionary.dic", "eomi.dic", "extension.dic", "josa.dic", "mapHanja.dic",
                       "prefix.dic", "suffix.dic", "syllable.dic", "uncompounds.dic" };
    File inputDir = new File(args[0]);
    File outputDir = new File(args[1]);
    for (String file : FILES_AS_IS) {
      File in = new File(inputDir, file);
      File out = new File(outputDir, file);
      copyAsIs(in, out);
    }
  }
  
  static void copyAsIs(File in, File out) throws Exception {
    InputStream r = new BufferedInputStream(new FileInputStream(in));
    OutputStream w = new BufferedOutputStream(new FileOutputStream(out));
    int c;
    while ((c = r.read()) != -1) {
      w.write(c);
    }
    r.close();
    w.close();
  }
}
