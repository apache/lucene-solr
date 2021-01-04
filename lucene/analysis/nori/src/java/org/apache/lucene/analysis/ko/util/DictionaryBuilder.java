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
package org.apache.lucene.analysis.ko.util;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Tool to build dictionaries. */
public class DictionaryBuilder {

  private DictionaryBuilder() {}

  public static void build(Path inputDir, Path outputDir, String encoding, boolean normalizeEntry)
      throws IOException {
    // Build TokenInfo Dictionary
    new TokenInfoDictionaryBuilder(encoding, normalizeEntry).build(inputDir).write(outputDir);

    // Build Unknown Word Dictionary
    new UnknownDictionaryBuilder(encoding).build(inputDir).write(outputDir);

    // Build Connection Cost
    ConnectionCostsBuilder.build(inputDir.resolve("matrix.def")).write(outputDir);
  }

  public static void main(String[] args) throws IOException {
    String inputDirname = args[0];
    String outputDirname = args[1];
    String inputEncoding = args[2];
    boolean normalizeEntries = Boolean.parseBoolean(args[3]);
    DictionaryBuilder.build(
        Paths.get(inputDirname), Paths.get(outputDirname), inputEncoding, normalizeEntries);
  }
}
