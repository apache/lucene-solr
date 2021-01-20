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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.PositiveIntOutputs;

class TokenInfoDictionaryBuilder {

  /**
   * Internal word id - incrementally assigned as entries are read and added. This will be byte
   * offset of dictionary file
   */
  private int offset = 0;

  private String encoding;
  private Normalizer.Form normalForm;

  TokenInfoDictionaryBuilder(String encoding, boolean normalizeEntries) {
    this.encoding = encoding;
    normalForm = normalizeEntries ? Normalizer.Form.NFKC : null;
  }

  public TokenInfoDictionaryWriter build(Path dir) throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      List<Path> csvFiles =
          files
              .filter(path -> path.getFileName().toString().endsWith(".csv"))
              .sorted()
              .collect(Collectors.toList());
      return buildDictionary(csvFiles);
    }
  }

  private TokenInfoDictionaryWriter buildDictionary(List<Path> csvFiles) throws IOException {
    TokenInfoDictionaryWriter dictionary = new TokenInfoDictionaryWriter(10 * 1024 * 1024);
    // all lines in the file
    List<String[]> lines = new ArrayList<>(400000);
    for (Path path : csvFiles) {
      try (BufferedReader reader = Files.newBufferedReader(path, Charset.forName(encoding))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] entry = CSVUtil.parse(line);

          if (entry.length < 12) {
            throw new IllegalArgumentException(
                "Entry in CSV is not valid (12 field values expected): " + line);
          }

          // NFKC normalize dictionary entry
          if (normalForm != null) {
            String[] normalizedEntry = new String[entry.length];
            for (int i = 0; i < entry.length; i++) {
              normalizedEntry[i] = Normalizer.normalize(entry[i], normalForm);
            }
            lines.add(normalizedEntry);
          } else {
            lines.add(entry);
          }
        }
      }
    }

    // sort by term: we sorted the files already and use a stable sort.
    lines.sort(Comparator.comparing(left -> left[0]));

    PositiveIntOutputs fstOutput = PositiveIntOutputs.getSingleton();
    FSTCompiler<Long> fstCompiler = new FSTCompiler<>(FST.INPUT_TYPE.BYTE2, fstOutput);
    IntsRefBuilder scratch = new IntsRefBuilder();
    long ord = -1; // first ord will be 0
    String lastValue = null;

    // build token info dictionary
    for (String[] entry : lines) {
      String surfaceForm = entry[0].trim();
      if (surfaceForm.isEmpty()) {
        continue;
      }
      int next = dictionary.put(entry);

      if (next == offset) {
        throw new IllegalStateException("Failed to process line: " + Arrays.toString(entry));
      }

      if (!surfaceForm.equals(lastValue)) {
        // new word to add to fst
        ord++;
        lastValue = surfaceForm;
        scratch.grow(surfaceForm.length());
        scratch.setLength(surfaceForm.length());
        for (int i = 0; i < surfaceForm.length(); i++) {
          scratch.setIntAt(i, surfaceForm.charAt(i));
        }
        fstCompiler.add(scratch.get(), ord);
      }
      dictionary.addMapping((int) ord, offset);
      offset = next;
    }
    dictionary.setFST(fstCompiler.compile());
    return dictionary;
  }
}
