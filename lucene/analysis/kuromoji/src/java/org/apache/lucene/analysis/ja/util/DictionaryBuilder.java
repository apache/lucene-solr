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
package org.apache.lucene.analysis.ja.util;


import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DictionaryBuilder {

  public enum DictionaryFormat { IPADIC, UNIDIC }

  private DictionaryBuilder() {
  }

  public static void build(DictionaryFormat format, Path inputDir, Path outputDir, String encoding, boolean normalizeEntry) throws IOException {
    new TokenInfoDictionaryBuilder(format, encoding, normalizeEntry)
        .build(inputDir)
        .write(outputDir);

    new UnknownDictionaryBuilder(encoding)
        .build(inputDir)
        .write(outputDir);

    ConnectionCostsBuilder.build(inputDir.resolve("matrix.def"))
        .write(outputDir);
  }

  public static void main(String[] args) throws IOException {
    DictionaryFormat format = DictionaryFormat.valueOf(args[0].toUpperCase());
    String inputDirName = args[1];
    String outputDirName = args[2];
    String inputEncoding = args[3];
    boolean normalizeEntries = Boolean.parseBoolean(args[4]);
    DictionaryBuilder.build(format, Paths.get(inputDirName), Paths.get(outputDirName), inputEncoding, normalizeEntries);
  }
}
