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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
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
    checkAnalysisPerformance("en", 1_200_000);
  }

  @Test
  public void en_suggest() throws Exception {
    checkSuggestionPerformance("en", 3_000);
  }

  @Test
  public void ru() throws Exception {
    checkAnalysisPerformance("ru", 400_000);
  }

  @Test
  public void ru_suggest() throws Exception {
    checkSuggestionPerformance("ru", 1000);
  }

  @Test
  public void de() throws Exception {
    checkAnalysisPerformance("de", 300_000);
  }

  @Test
  public void de_suggest() throws Exception {
    checkSuggestionPerformance("de", 60);
  }

  @Test
  public void fr() throws Exception {
    checkAnalysisPerformance("fr", 100_000);
  }

  @Test
  public void fr_suggest() throws Exception {
    checkSuggestionPerformance("fr", 120);
  }

  private Dictionary loadDictionary(String code) throws IOException, ParseException {
    Path aff = findAffFile(code);
    Dictionary dictionary = TestAllDictionaries.loadDictionary(aff);
    System.out.println("Loaded " + aff);
    return dictionary;
  }

  private void checkAnalysisPerformance(String code, int wordCount) throws Exception {
    Dictionary dictionary = loadDictionary(code);

    List<String> words = loadWords(code, wordCount, dictionary);
    List<String> halfWords = words.subList(0, words.size() / 2);

    Stemmer stemmer = new Stemmer(dictionary);
    Hunspell speller = new Hunspell(dictionary, TimeoutPolicy.NO_TIMEOUT, () -> {});
    int cpus = Runtime.getRuntime().availableProcessors();
    ExecutorService executor =
        Executors.newFixedThreadPool(cpus, new NamedThreadFactory("hunspellStemming-"));

    try {
      measure("Stemming " + code, blackHole -> stemWords(words, stemmer, blackHole));

      measure(
          "Multi-threaded stemming " + code,
          blackHole -> {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < cpus; i++) {
              Stemmer localStemmer = new Stemmer(dictionary);
              futures.add(executor.submit(() -> stemWords(halfWords, localStemmer, blackHole)));
            }
            try {
              for (Future<?> future : futures) {
                future.get();
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });

      measure(
          "Spellchecking " + code,
          blackHole -> {
            for (String word : words) {
              blackHole.accept(speller.spell(word));
            }
          });
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
    }

    System.out.println();
  }

  private void stemWords(List<String> words, Stemmer stemmer, Consumer<Object> blackHole) {
    for (String word : words) {
      blackHole.accept(stemmer.stem(word));
    }
  }

  private void checkSuggestionPerformance(String code, int wordCount) throws Exception {
    Dictionary dictionary = loadDictionary(code);
    Hunspell speller = new Hunspell(dictionary, TimeoutPolicy.THROW_EXCEPTION, () -> {});
    List<String> words =
        loadWords(code, wordCount, dictionary).stream()
            .distinct()
            .filter(w -> hasQuickSuggestions(speller, w))
            .collect(Collectors.toList());
    System.out.println("Checking " + words.size() + " misspelled words");

    measure(
        "Suggestions for " + code,
        blackHole -> {
          for (String word : words) {
            blackHole.accept(speller.suggest(word));
          }
        });
    System.out.println();
  }

  private boolean hasQuickSuggestions(Hunspell speller, String word) {
    if (speller.spell(word)) {
      return false;
    }

    long start = System.nanoTime();
    try {
      speller.suggest(word);
    } catch (
        @SuppressWarnings("unused")
        SuggestionTimeoutException e) {
      System.out.println("Timeout happened for " + word + ", skipping");
      return false;
    }
    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    if (elapsed > Hunspell.SUGGEST_TIME_LIMIT * 4 / 5) {
      System.out.println(elapsed + "ms for " + word + ", too close to time limit, skipping");
    }
    return true;
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

        for (String token :
            line.split("[^\\p{IsLetter}" + Pattern.quote(dictionary.wordChars) + "]+")) {
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
            + times.stream().mapToLong(Long::longValue).average().orElseThrow(AssertionError::new)
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
