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
import java.io.LineNumberReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.analysis.ko.dict.CharacterDefinition;

class UnknownDictionaryBuilder {
  private static final String NGRAM_DICTIONARY_ENTRY = "NGRAM,1798,3559,3677,SY,*,*,*,*,*,*,*";

  private String encoding;

  UnknownDictionaryBuilder(String encoding) {
    this.encoding = encoding;
  }

  public UnknownDictionaryWriter build(Path dir) throws IOException {
    UnknownDictionaryWriter unkDictionary = readDictionaryFile(dir.resolve("unk.def"));  //Should be only one file
    readCharacterDefinition(dir.resolve("char.def"), unkDictionary);
    return unkDictionary;
  }

  private UnknownDictionaryWriter readDictionaryFile(Path path) throws IOException {
    return readDictionaryFile(path, encoding);
  }

  private UnknownDictionaryWriter readDictionaryFile(Path path, String encoding) throws IOException {
    UnknownDictionaryWriter dictionary = new UnknownDictionaryWriter(5 * 1024 * 1024);

    List<String[]> lines = new ArrayList<>();
    try (Reader reader = Files.newBufferedReader(path, Charset.forName(encoding));
         LineNumberReader lineReader = new LineNumberReader(reader)) {

      dictionary.put(CSVUtil.parse(NGRAM_DICTIONARY_ENTRY));

      String line;
      while ((line = lineReader.readLine()) != null) {
        // note: unk.def only has 10 fields, it simplifies the writer to just append empty reading and pronunciation,
        // even though the unknown dictionary returns hardcoded null here.
        final String[] parsed = CSVUtil.parse(line + ",*,*"); // Probably we don't need to validate entry
        lines.add(parsed);
      }
    }

    lines.sort(Comparator.comparingInt(entry -> CharacterDefinition.lookupCharacterClass(entry[0])));

    for (String[] entry : lines) {
      dictionary.put(entry);
    }

    return dictionary;
  }

  private void readCharacterDefinition(Path path, UnknownDictionaryWriter dictionary) throws IOException {
    try (Reader reader = Files.newBufferedReader(path, Charset.forName(encoding));
         LineNumberReader lineReader = new LineNumberReader(reader)) {

      String line;
      while ((line = lineReader.readLine()) != null) {
        line = line.replaceAll("^\\s", "");
        line = line.replaceAll("\\s*#.*", "");
        line = line.replaceAll("\\s+", " ");

        // Skip empty line or comment line
        if (line.length() == 0) {
          continue;
        }

        if (line.startsWith("0x")) {  // Category mapping
          String[] values = line.split(" ", 2);  // Split only first space

          if (!values[0].contains("..")) {
            int cp = Integer.decode(values[0]);
            dictionary.putCharacterCategory(cp, values[1]);
          } else {
            String[] codePoints = values[0].split("\\.\\.");
            int cpFrom = Integer.decode(codePoints[0]);
            int cpTo = Integer.decode(codePoints[1]);

            for (int i = cpFrom; i <= cpTo; i++) {
              dictionary.putCharacterCategory(i, values[1]);
            }
          }
        } else {  // Invoke definition
          String[] values = line.split(" "); // Consecutive space is merged above
          String characterClassName = values[0];
          int invoke = Integer.parseInt(values[1]);
          int group = Integer.parseInt(values[2]);
          int length = Integer.parseInt(values[3]);
          dictionary.putInvokeDefinition(characterClassName, invoke, group, length);
        }
      }
    }
  }
}
