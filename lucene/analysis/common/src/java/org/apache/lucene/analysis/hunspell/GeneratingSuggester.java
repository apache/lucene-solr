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

import static org.apache.lucene.analysis.hunspell.Dictionary.AFFIX_APPEND;
import static org.apache.lucene.analysis.hunspell.Dictionary.AFFIX_FLAG;
import static org.apache.lucene.analysis.hunspell.Dictionary.AFFIX_STRIP_ORD;
import static org.apache.lucene.analysis.hunspell.Dictionary.FLAG_UNSET;
import static org.apache.lucene.analysis.hunspell.Dictionary.HIDDEN_FLAG;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.FST;

/**
 * A class that traverses the entire dictionary and applies affix rules to check if those yield
 * correct suggestions similar enough to the given misspelled word
 */
class GeneratingSuggester {
  private static final int MAX_ROOTS = 100;
  private static final int MAX_WORDS = 100;
  private static final int MAX_GUESSES = 200;
  private static final int MAX_ROOT_LENGTH_DIFF = 4;
  private final Dictionary dictionary;
  private final Hunspell speller;

  GeneratingSuggester(Hunspell speller) {
    this.dictionary = speller.dictionary;
    this.speller = speller;
  }

  List<String> suggest(String word, WordCase originalCase, Set<String> prevSuggestions) {
    List<Weighted<Root<String>>> roots = findSimilarDictionaryEntries(word, originalCase);
    List<Weighted<String>> expanded = expandRoots(word, roots);
    TreeSet<Weighted<String>> bySimilarity = rankBySimilarity(word, expanded);
    return getMostRelevantSuggestions(bySimilarity, prevSuggestions);
  }

  private List<Weighted<Root<String>>> findSimilarDictionaryEntries(
      String word, WordCase originalCase) {
    Comparator<Weighted<Root<String>>> natural = Comparator.naturalOrder();
    PriorityQueue<Weighted<Root<String>>> roots = new PriorityQueue<>(natural.reversed());
    EntryFilter filter = new EntryFilter(dictionary);
    boolean ignoreTitleCaseRoots = originalCase == WordCase.LOWER && !dictionary.hasLanguage("de");
    TrigramAutomaton automaton =
        new TrigramAutomaton(word) {
          @Override
          char transformChar(char c) {
            return dictionary.caseFold(c);
          }
        };

    dictionary.words.processAllWords(
        Math.max(1, word.length() - 4),
        word.length() + 4,
        (rootChars, forms) -> {
          speller.checkCanceled.run();

          assert rootChars.length > 0;
          if (Math.abs(rootChars.length - word.length()) > MAX_ROOT_LENGTH_DIFF) {
            assert rootChars.length < word.length(); // processAllWords takes care of longer keys
            return;
          }

          int suitable = filter.findSuitableFormIndex(forms, 0);
          if (suitable < 0) return;

          if (ignoreTitleCaseRoots
              && Character.isUpperCase(rootChars.charAt(0))
              && WordCase.caseOf(rootChars) == WordCase.TITLE) {
            return;
          }

          int sc = automaton.ngramScore(rootChars);
          if (sc == 0) {
            return; // no common characters at all, don't suggest this root
          }

          sc += commonPrefix(word, rootChars) - longerWorsePenalty(word.length(), rootChars.length);

          if (roots.size() == MAX_ROOTS && sc < roots.peek().score) {
            return;
          }

          String root = rootChars.toString();
          do {
            roots.add(new Weighted<>(new Root<>(root, forms.ints[forms.offset + suitable]), sc));
            suitable = filter.findSuitableFormIndex(forms, suitable + filter.formStep);
          } while (suitable > 0);
          while (roots.size() > MAX_ROOTS) {
            roots.poll();
          }
        });

    return roots.stream().sorted().collect(Collectors.toList());
  }

  private static class EntryFilter {
    private final int formStep;
    private final FlagEnumerator.Lookup flagLookup;
    private final char[] excludeFlags;

    EntryFilter(Dictionary dic) {
      formStep = dic.formStep();
      flagLookup = dic.flagLookup;

      Character[] flags = {HIDDEN_FLAG, dic.noSuggest, dic.forbiddenword, dic.onlyincompound};
      excludeFlags =
          Dictionary.toSortedCharArray(
              Stream.of(flags).filter(c -> c != FLAG_UNSET).collect(Collectors.toSet()));
    }

    int findSuitableFormIndex(IntsRef forms, int start) {
      for (int i = start; i < forms.length; i += formStep) {
        if (!flagLookup.hasAnyFlag(forms.ints[forms.offset + i], excludeFlags)) {
          return i;
        }
      }
      return -1;
    }
  }

  private List<Weighted<String>> expandRoots(
      String misspelled, List<Weighted<Root<String>>> roots) {
    int thresh = calcThreshold(misspelled);

    TreeSet<Weighted<String>> expanded = new TreeSet<>();
    for (Weighted<Root<String>> weighted : roots) {
      for (String guess : expandRoot(weighted.word, misspelled)) {
        String lower = dictionary.toLowerCase(guess);
        int sc =
            anyMismatchNgram(misspelled.length(), misspelled, lower, false)
                + commonPrefix(misspelled, guess);
        if (sc > thresh) {
          expanded.add(new Weighted<>(guess, sc));
        }
      }
    }
    return expanded.stream().limit(MAX_GUESSES).collect(Collectors.toList());
  }

  // find minimum threshold for a passable suggestion
  // mangle original word three different ways
  // and score them to generate a minimum acceptable score
  private static int calcThreshold(String word) {
    int thresh = 0;
    for (int sp = 1; sp < 4; sp++) {
      char[] mw = word.toCharArray();
      for (int k = sp; k < word.length(); k += 4) {
        mw[k] = '*';
      }

      thresh += anyMismatchNgram(word.length(), word, new String(mw), false);
    }
    return thresh / 3 - 1;
  }

  private List<String> expandRoot(Root<String> root, String misspelled) {
    List<char[]> crossProducts = new ArrayList<>();
    Set<String> result = new LinkedHashSet<>();

    if (!dictionary.hasFlag(root.entryId, dictionary.needaffix)) {
      result.add(root.word);
    }

    char[] wordChars = root.word.toCharArray();

    // suffixes
    processAffixes(
        false,
        misspelled,
        (suffixLength, suffixId) -> {
          int stripLength = affixStripLength(suffixId);
          if (!hasCompatibleFlags(root, suffixId)
              || !checkAffixCondition(suffixId, wordChars, 0, wordChars.length - stripLength)) {
            return;
          }

          String suffix = misspelled.substring(misspelled.length() - suffixLength);
          String withSuffix = root.word.substring(0, root.word.length() - stripLength) + suffix;
          result.add(withSuffix);
          if (dictionary.isCrossProduct(suffixId)) {
            crossProducts.add(withSuffix.toCharArray());
          }
        });

    // cross-product prefixes
    processAffixes(
        true,
        misspelled,
        (prefixLength, prefixId) -> {
          if (!dictionary.hasFlag(root.entryId, dictionary.affixData(prefixId, AFFIX_FLAG))
              || !dictionary.isCrossProduct(prefixId)) {
            return;
          }

          int stripLength = affixStripLength(prefixId);
          String prefix = misspelled.substring(0, prefixLength);
          for (char[] suffixed : crossProducts) {
            int stemLength = suffixed.length - stripLength;
            if (checkAffixCondition(prefixId, suffixed, stripLength, stemLength)) {
              result.add(prefix + new String(suffixed, stripLength, stemLength));
            }
          }
        });

    // pure prefixes
    processAffixes(
        true,
        misspelled,
        (prefixLength, prefixId) -> {
          int stripLength = affixStripLength(prefixId);
          int stemLength = wordChars.length - stripLength;
          if (hasCompatibleFlags(root, prefixId)
              && checkAffixCondition(prefixId, wordChars, stripLength, stemLength)) {
            String prefix = misspelled.substring(0, prefixLength);
            result.add(prefix + root.word.substring(stripLength));
          }
        });

    return result.stream().limit(MAX_WORDS).collect(Collectors.toList());
  }

  private void processAffixes(boolean prefixes, String word, AffixProcessor processor) {
    FST<IntsRef> fst = prefixes ? dictionary.prefixes : dictionary.suffixes;
    if (fst == null) return;

    FST.Arc<IntsRef> arc = fst.getFirstArc(new FST.Arc<>());
    if (arc.isFinal()) {
      processAffixIds(0, arc.nextFinalOutput(), processor);
    }

    FST.BytesReader reader = fst.getBytesReader();

    IntsRef output = fst.outputs.getNoOutput();
    int length = word.length();
    int step = prefixes ? 1 : -1;
    int limit = prefixes ? length : -1;
    for (int i = prefixes ? 0 : length - 1; i != limit; i += step) {
      output = Dictionary.nextArc(fst, arc, reader, output, word.charAt(i));
      if (output == null) {
        break;
      }

      if (arc.isFinal()) {
        IntsRef affixIds = fst.outputs.add(output, arc.nextFinalOutput());
        processAffixIds(prefixes ? i + 1 : length - i, affixIds, processor);
      }
    }
  }

  private void processAffixIds(int affixLength, IntsRef affixIds, AffixProcessor processor) {
    for (int j = 0; j < affixIds.length; j++) {
      processor.processAffix(affixLength, affixIds.ints[affixIds.offset + j]);
    }
  }

  private interface AffixProcessor {
    void processAffix(int affixLength, int affixId);
  }

  private boolean hasCompatibleFlags(Root<?> root, int affixId) {
    if (!dictionary.hasFlag(root.entryId, dictionary.affixData(affixId, AFFIX_FLAG))) {
      return false;
    }

    int append = dictionary.affixData(affixId, AFFIX_APPEND);
    return !dictionary.hasFlag(append, dictionary.needaffix)
        && !dictionary.hasFlag(append, dictionary.circumfix)
        && !dictionary.hasFlag(append, dictionary.onlyincompound);
  }

  private boolean checkAffixCondition(int suffixId, char[] word, int offset, int length) {
    if (length < 0) return false;
    int condition = dictionary.getAffixCondition(suffixId);
    return condition == 0 || dictionary.patterns.get(condition).acceptsStem(word, offset, length);
  }

  private int affixStripLength(int affixId) {
    char stripOrd = dictionary.affixData(affixId, AFFIX_STRIP_ORD);
    return dictionary.stripOffsets[stripOrd + 1] - dictionary.stripOffsets[stripOrd];
  }

  private TreeSet<Weighted<String>> rankBySimilarity(String word, List<Weighted<String>> expanded) {
    double fact = (10.0 - dictionary.maxDiff) / 5.0;
    TreeSet<Weighted<String>> bySimilarity = new TreeSet<>();
    for (Weighted<String> weighted : expanded) {
      String guess = weighted.word;
      String lower = dictionary.toLowerCase(guess);
      if (lower.equals(word)) {
        bySimilarity.add(new Weighted<>(guess, weighted.score + 2000));
        break;
      }

      int re = anyMismatchNgram(2, word, lower, true) + anyMismatchNgram(2, lower, word, true);

      int score =
          2 * lcs(word, lower)
              - Math.abs(word.length() - lower.length())
              + commonCharacterPositionScore(word, lower)
              + commonPrefix(word, lower)
              + anyMismatchNgram(4, word, lower, false)
              + re
              + (re < (word.length() + lower.length()) * fact ? -1000 : 0);
      bySimilarity.add(new Weighted<>(guess, score));
    }
    return bySimilarity;
  }

  private List<String> getMostRelevantSuggestions(
      TreeSet<Weighted<String>> bySimilarity, Set<String> prevSuggestions) {
    List<String> result = new ArrayList<>();
    boolean hasExcellent = false;
    for (Weighted<String> weighted : bySimilarity) {
      if (weighted.score > 1000) {
        hasExcellent = true;
      } else if (hasExcellent) {
        break; // leave only excellent suggestions, if any
      }

      boolean bad = weighted.score < -100;
      // keep the best ngram suggestions, unless in ONLYMAXDIFF mode
      if (bad && (!result.isEmpty() || dictionary.onlyMaxDiff)) {
        break;
      }

      if (prevSuggestions.stream().noneMatch(weighted.word::contains)
          && result.stream().noneMatch(weighted.word::contains)
          && speller.checkWord(weighted.word)) {
        result.add(weighted.word);
        if (result.size() >= dictionary.maxNGramSuggestions) {
          break;
        }
      }

      if (bad) {
        break;
      }
    }
    return result;
  }

  static int commonPrefix(CharSequence s1, CharSequence s2) {
    int i = 0;
    int limit = Math.min(s1.length(), s2.length());
    while (i < limit && s1.charAt(i) == s2.charAt(i)) {
      i++;
    }
    return i;
  }

  // generate an n-gram score comparing s1 and s2
  static int ngramScore(int n, String s1, String s2, boolean weighted) {
    int l1 = s1.length();
    int score = 0;
    int[] lastStarts = new int[l1];
    for (int j = 1; j <= n; j++) {
      int ns = 0;
      for (int i = 0; i <= (l1 - j); i++) {
        if (lastStarts[i] >= 0) {
          int pos = indexOfSubstring(s2, lastStarts[i], s1, i, j);
          lastStarts[i] = pos;
          if (pos >= 0) {
            ns++;
            continue;
          }
        }
        if (weighted) {
          ns--;
          if (i == 0 || i == l1 - j) {
            ns--; // side weight
          }
        }
      }
      score = score + ns;
      if (ns < 2 && !weighted) {
        break;
      }
    }
    return score;
  }

  // NGRAM_LONGER_WORSE flag in Hunspell
  private static int longerWorsePenalty(int length1, int length2) {
    return Math.max((length2 - length1) - 2, 0);
  }

  // NGRAM_ANY_MISMATCH flag in Hunspell
  private static int anyMismatchNgram(int n, String s1, String s2, boolean weighted) {
    return ngramScore(n, s1, s2, weighted) - Math.max(Math.abs(s2.length() - s1.length()) - 2, 0);
  }

  private static int indexOfSubstring(
      String haystack, int haystackPos, String needle, int needlePos, int len) {
    char c = needle.charAt(needlePos);
    int limit = haystack.length() - len;
    for (int i = haystackPos; i <= limit; i++) {
      if (haystack.charAt(i) == c
          && haystack.regionMatches(i + 1, needle, needlePos + 1, len - 1)) {
        return i;
      }
    }
    return -1;
  }

  private static int lcs(String s1, String s2) {
    int[] lengths = new int[s2.length() + 1];

    for (int i = 1; i <= s1.length(); i++) {
      int prev = 0;
      for (int j = 1; j <= s2.length(); j++) {
        int cur = lengths[j];
        lengths[j] =
            s1.charAt(i - 1) == s2.charAt(j - 1) ? prev + 1 : Math.max(cur, lengths[j - 1]);
        prev = cur;
      }
    }
    return lengths[s2.length()];
  }

  private static int commonCharacterPositionScore(String s1, String s2) {
    int num = 0;
    int diffPos1 = -1;
    int diffPos2 = -1;
    int diff = 0;
    int i;
    for (i = 0; i < s1.length() && i < s2.length(); ++i) {
      if (s1.charAt(i) == s2.charAt(i)) {
        num++;
      } else {
        if (diff == 0) diffPos1 = i;
        else if (diff == 1) diffPos2 = i;
        diff++;
      }
    }
    int commonScore = num > 0 ? 1 : 0;
    if (diff == 2
        && i == s1.length()
        && i == s2.length()
        && s1.charAt(diffPos1) == s2.charAt(diffPos2)
        && s1.charAt(diffPos2) == s2.charAt(diffPos1)) {
      return commonScore + 10;
    }
    return commonScore;
  }

  private static class Weighted<T extends Comparable<T>> implements Comparable<Weighted<T>> {
    final T word;
    final int score;

    Weighted(T word, int score) {
      this.word = word;
      this.score = score;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Weighted)) return false;
      @SuppressWarnings("unchecked")
      Weighted<T> that = (Weighted<T>) o;
      return score == that.score && word.equals(that.word);
    }

    @Override
    public int hashCode() {
      return Objects.hash(word, score);
    }

    @Override
    public String toString() {
      return word + "(" + score + ")";
    }

    @Override
    public int compareTo(Weighted<T> o) {
      int cmp = Integer.compare(score, o.score);
      return cmp != 0 ? -cmp : word.compareTo(o.word);
    }
  }
}
