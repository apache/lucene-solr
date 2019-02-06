/*
 * This software was produced for the U. S. Government
 * under Contract No. W15P7T-11-C-F600, and is
 * subject to the Rights in Noncommercial Computer Software
 * and Noncommercial Computer Software Documentation
 * Clause 252.227-7014 (JUN 1995)
 *
 * Copyright 2013 The MITRE Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.tagger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Randomly generate taggable text and verify via simple tag algorithm.
 */
@Repeat(iterations = 10)
public class RandomizedTaggerTest extends TaggerTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tagger.xml", "schema-tagger.xml");
  }

  @Test
  public void test() throws Exception {
    final Random R = random();

    Set<String> names = new HashSet<>();
    //random list of single-word names
    final int NUM_SINGLES = 4;//RandomInts.randomIntBetween(R, 1, 5);
    for (int i = 0; i < NUM_SINGLES; i++) {
      if (i == 0)//first is a big string (perhaps triggers bugs related to growing buffers)
        names.add(randomStringOfLength(16, 32));
      else
        names.add(randomString());
    }

    //add random list of multi-word names, partially including existing names
    final int NUM_MULTI = 10;
    for (int i = 0; i < NUM_MULTI; i++) {
      final int numWords = RandomNumbers.randomIntBetween(R, 2, 4);
      StringBuilder buf = new StringBuilder();
      for (int j = 0; j < numWords; j++) {
        if (j != 0)
          buf.append(' ');
        if (R.nextBoolean()) {//new likely non-existent word
          buf.append(randomString());
        } else {//existing word (possible multi-word from prev iteration)
          buf.append(RandomPicks.randomFrom(R, names));
        }
      }
      names.add(buf.toString());
    }

    // BUILD NAMES
    buildNames(names.toArray(new String[names.size()]));

    // QUERY LOOP
    for (int tTries = 0; tTries < 10 * RANDOM_MULTIPLIER; tTries++) {
      // Build up random input, similar to multi-word random names above
      StringBuilder input = new StringBuilder();
      final int INPUT_WORD_LEN = 20;
      input.append(' ');//must start with space based on assertBruteForce logic
      for (int i = 0; i < INPUT_WORD_LEN; i++) {
        if (R.nextBoolean()) {//new likely non-existent word
          input.append(randomString());
        } else {//existing word (possible multi-word from prev iteration)
          input.append(RandomPicks.randomFrom(R, NAMES));
        }
        input.append(' ');//must end with a space
      }

      boolean madeIt = false;
      try {
        assertBruteForce(input.toString());
        madeIt = true;
      } finally {
        if (!madeIt) {
          System.out.println("Reproduce with:");
          System.out.print(" buildNames(");
          for (int i = 0; i < NAMES.size(); i++) {
            if (i != 0)
              System.out.print(',');
            System.out.print('"');
            System.out.print(NAMES.get(i));
            System.out.print('"');
          }
          System.out.println(");");
          System.out.println(" assertBruteForce(\"" + input+"\");");
        }
      }
    }

  }

  private void assertBruteForce(String input) throws Exception {
    assert input.matches(" .* ");
    baseParams.set("overlaps", "ALL");

    //loop through NAMES and find all tag offsets
    List<TestTag> testTags = new ArrayList<>();
    for (String name : NAMES) {
      String spaceName = " "+name+" ";
      int off = 0;
      while (true) {
        int idx = input.indexOf(spaceName, off);
        if (idx < 0)
          break;
        testTags.add(new TestTag(idx + 1, idx + 1 + name.length(), name, name));
        off = idx + 1;
      }
    }

    //assert
    assertTags(reqDoc(input), testTags.toArray(new TestTag[testTags.size()]));
  }

  private String randomString() { return randomStringOfLength(1, 1); }

  private String randomStringOfLength(int min, int max) {
    return RandomStrings.randomAsciiLettersOfLengthBetween(random(), min, max).toLowerCase(Locale.ROOT);
  }

}
