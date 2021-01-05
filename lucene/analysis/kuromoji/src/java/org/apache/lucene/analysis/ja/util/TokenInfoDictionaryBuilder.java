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
import org.apache.lucene.analysis.ja.util.DictionaryBuilder.DictionaryFormat;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.PositiveIntOutputs;

/** */
class TokenInfoDictionaryBuilder {

  private final String encoding;
  private final Normalizer.Form normalForm;
  private final DictionaryFormat format;

  /**
   * Internal word id - incrementally assigned as entries are read and added. This will be byte
   * offset of dictionary file
   */
  private int offset = 0;

  public TokenInfoDictionaryBuilder(
      DictionaryFormat format, String encoding, boolean normalizeEntries) {
    this.format = format;
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
    Charset cs = Charset.forName(encoding);
    // all lines in the file
    List<String[]> lines = new ArrayList<>(400000);
    for (Path path : csvFiles) {
      try (BufferedReader reader = Files.newBufferedReader(path, cs)) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] entry = CSVUtil.parse(line);

          if (entry.length < 13) {
            throw new IllegalArgumentException(
                "Entry in CSV is not valid (13 field values expected): " + line);
          }

          lines.add(formatEntry(entry));

          if (normalForm != null) {
            if (Normalizer.isNormalized(entry[0], normalForm)) {
              continue;
            }
            String[] normalizedEntry = new String[entry.length];
            for (int i = 0; i < entry.length; i++) {
              normalizedEntry[i] = Normalizer.normalize(entry[i], normalForm);
            }
            lines.add(formatEntry(normalizedEntry));
          }
        }
      }
    }

    // sort by term: we sorted the files already and use a stable sort.
    lines.sort(Comparator.comparing(entry -> entry[0]));

    PositiveIntOutputs fstOutput = PositiveIntOutputs.getSingleton();
    FSTCompiler<Long> fstCompiler = new FSTCompiler<>(FST.INPUT_TYPE.BYTE2, fstOutput);
    IntsRefBuilder scratch = new IntsRefBuilder();
    long ord = -1; // first ord will be 0
    String lastValue = null;

    // build token info dictionary
    for (String[] entry : lines) {
      int next = dictionary.put(entry);

      if (next == offset) {
        throw new IllegalStateException("Failed to process line: " + Arrays.toString(entry));
      }

      String token = entry[0];
      if (!token.equals(lastValue)) {
        // new word to add to fst
        ord++;
        lastValue = token;
        scratch.grow(token.length());
        scratch.setLength(token.length());
        for (int i = 0; i < token.length(); i++) {
          scratch.setIntAt(i, (int) token.charAt(i));
        }
        fstCompiler.add(scratch.get(), ord);
      }
      dictionary.addMapping((int) ord, offset);
      offset = next;
    }
    dictionary.setFST(fstCompiler.compile());
    return dictionary;
  }

  /*
   * IPADIC features
   *
   * 0   - surface
   * 1   - left cost
   * 2   - right cost
   * 3   - word cost
   * 4-9 - pos
   * 10  - base form
   * 11  - reading
   * 12  - pronounciation
   *
   * UniDic features
   *
   * 0   - surface
   * 1   - left cost
   * 2   - right cost
   * 3   - word cost
   * 4-9 - pos
   * 10  - base form reading
   * 11  - base form
   * 12  - surface form
   * 13  - surface reading
   */

  private String[] formatEntry(String[] features) {
    if (this.format == DictionaryFormat.IPADIC) {
      return features;
    } else {
      String[] features2 = new String[13];
      features2[0] = features[0];
      features2[1] = features[1];
      features2[2] = features[2];
      features2[3] = features[3];
      features2[4] = features[4];
      features2[5] = features[5];
      features2[6] = features[6];
      features2[7] = features[7];
      features2[8] = features[8];
      features2[9] = features[9];
      features2[10] = features[11];

      // If the surface reading is non-existent, use surface form for reading and pronunciation.
      // This happens with punctuation in UniDic and there are possibly other cases as well
      if (features[13].length() == 0) {
        features2[11] = features[0];
        features2[12] = features[0];
      } else {
        features2[11] = features[13];
        features2[12] = features[13];
      }
      return features2;
    }
  }
}
