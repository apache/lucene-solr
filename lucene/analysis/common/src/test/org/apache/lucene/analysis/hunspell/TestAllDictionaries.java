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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.lucene.util.RamUsageTester;
import org.junit.Assume;
import org.junit.Ignore;

/**
 * Loads all dictionaries from the directory specified in {@code -Dhunspell.dictionaries=...} and
 * prints their memory usage. All *.aff files are traversed directly inside the given directory or
 * in its immediate subdirectories. Each *.aff file must have a same-named sibling *.dic file. For
 * examples of such directories, refer to the {@link org.apache.lucene.analysis.hunspell package
 * documentation}
 */
@Ignore("enable manually")
@SuppressSysoutChecks(bugUrl = "prints important memory utilization stats per dictionary")
public class TestAllDictionaries extends LuceneTestCase {

  static Stream<Path> findAllAffixFiles() throws IOException {
    String dicDir = System.getProperty("hunspell.dictionaries");
    Assume.assumeFalse("Missing -Dhunspell.dictionaries=...", dicDir == null);
    return Files.walk(Path.of(dicDir), 2).filter(f -> f.toString().endsWith(".aff"));
  }

  static Dictionary loadDictionary(Path aff) throws IOException, ParseException {
    String affPath = aff.toString();
    Path dic = Path.of(affPath.substring(0, affPath.length() - 4) + ".dic");
    assert Files.exists(dic) : dic;
    try (InputStream dictionary = Files.newInputStream(dic);
        InputStream affix = Files.newInputStream(aff);
        BaseDirectoryWrapper tempDir = newDirectory()) {
      return new Dictionary(tempDir, "dictionary", affix, dictionary);
    }
  }

  public void testDictionariesLoadSuccessfully() throws Exception {
    int failures = 0;
    for (Path aff : findAllAffixFiles().collect(Collectors.toList())) {
      try {
        System.out.println(aff + "\t" + memoryUsage(loadDictionary(aff)));
      } catch (Throwable e) {
        failures++;
        System.err.println("While checking " + aff + ":");
        e.printStackTrace();
      }
    }
    assertEquals(failures + " failures!", 0, failures);
  }

  private static String memoryUsage(Dictionary dic) {
    return RamUsageTester.humanSizeOf(dic)
        + "\t("
        + "words="
        + RamUsageTester.humanSizeOf(dic.words)
        + ", "
        + "flags="
        + RamUsageTester.humanSizeOf(dic.flagLookup)
        + ", "
        + "strips="
        + RamUsageTester.humanSizeOf(dic.stripData)
        + ", "
        + "conditions="
        + RamUsageTester.humanSizeOf(dic.patterns)
        + ", "
        + "affixData="
        + RamUsageTester.humanSizeOf(dic.affixData)
        + ", "
        + "prefixes="
        + RamUsageTester.humanSizeOf(dic.prefixes)
        + ", "
        + "suffixes="
        + RamUsageTester.humanSizeOf(dic.suffixes)
        + ")";
  }
}
