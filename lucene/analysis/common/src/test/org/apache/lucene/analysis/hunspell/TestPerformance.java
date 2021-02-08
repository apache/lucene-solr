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
package org.apache.lucene.analysis.hunspell;

import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Assume;
import org.junit.AssumptionViolatedException;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A test that runs various Hunspell APIs on real dictionaries and relatively large corpora for
 * specific languages and prints the execution times. The dictionaries should be set up as in {@link
 * TestAllDictionaries}, the corpora should be in files named {@code langCode.txt} (e.g. {@code
 * en.txt}) in a directory specified in {@code -Dhunspell.corpora=...}
 */
@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public class TestPerformance extends LuceneTestCase {
  private static Path corporaDir;

  @BeforeClass
  public static void resolveCorpora() {
    String dir = System.getProperty("hunspell.corpora");
    Assume.assumeFalse("Requires test word corpora at -Dhunspell.corpora=...", dir == null);
    corporaDir = Paths.get(dir);
  }

  @Test
  public void en() throws Exception {
    checkPerformance("en", 500_000);
  }

  @Test
  public void de() throws Exception {
    checkPerformance("de", 100_000);
  }

  @Test
  public void fr() throws Exception {
    checkPerformance("fr", 20_000);
  }

  private void checkPerformance(String code, int wordCount) throws Exception {
    Path aff = findAffFile(code);

    Dictionary dictionary = TestAllDictionaries.loadDictionary(aff);
    System.out.println("Loaded " + aff);

    List<String> words = loadWords(code, wordCount, dictionary);

    Stemmer stemmer = new Stemmer(dictionary);
    SpellChecker speller = new SpellChecker(dictionary);
    measure(
        "Stemming " + code,
        blackHole -> {
          for (String word : words) {
            blackHole.accept(stemmer.stem(word));
          }
        });
    measure(
        "Spellchecking " + code,
        blackHole -> {
          for (String word : words) {
            blackHole.accept(speller.spell(word));
          }
        });
    System.out.println();
  }

  private Path findAffFile(String code) throws IOException {
    return TestAllDictionaries.findAllAffixFiles()
        .filter(
            path -> {
              String parentName = path.getParent().getFileName().toString();
              return code.equals(Dictionary.extractLanguageCode(parentName));
            })
        .findFirst()
        .orElseThrow(
            () -> new AssumptionViolatedException("Ignored, cannot find aff/dic for: " + code));
  }

  private List<String> loadWords(String code, int wordCount, Dictionary dictionary)
      throws IOException {
    Path dataPath = corporaDir.resolve(code + ".txt");
    if (!Files.isReadable(dataPath)) {
      throw new AssumptionViolatedException("Missing text corpora at: " + dataPath);
    }

    List<String> words = new ArrayList<>();
    try (InputStream stream = Files.newInputStream(dataPath)) {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
      while (true) {
        String line = reader.readLine();
        if (line == null) break;

        for (String token : line.split("[^a-zA-Z" + Pattern.quote(dictionary.wordChars) + "]+")) {
          String word = stripPunctuation(token);
          if (word != null) {
            words.add(word);
            if (words.size() == wordCount) {
              return words;
            }
          }
        }
      }
    }
    return words;
  }

  private void measure(String what, Iteration iteration) {
    Consumer<Object> consumer =
        o -> {
          if (o == null) {
            throw new AssertionError();
          }
        };

    // warmup
    for (int i = 0; i < 2; i++) {
      iteration.run(consumer);
    }

    List<Long> times = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      long start = System.currentTimeMillis();
      iteration.run(consumer);
      times.add(System.currentTimeMillis() - start);
    }
    System.out.println(
        what
            + ": average "
            + times.stream().mapToLong(Long::longValue).average().orElseThrow()
            + ", all times = "
            + times);
  }

  private interface Iteration {
    void run(Consumer<Object> blackHole);
  }

  static String stripPunctuation(String token) {
    int start = 0;
    int end = token.length();
    while (start < end && isPunctuation(token.charAt(start))) start++;
    while (start < end - 1 && isPunctuation(token.charAt(end - 1))) end--;
    return start < end ? token.substring(start, end) : null;
  }

  private static boolean isPunctuation(char c) {
    return ".!?,\"'’‘".indexOf(c) >= 0;
  }
}
