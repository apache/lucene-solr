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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.OfflineSorter.ByteSequencesReader;
import org.apache.lucene.util.OfflineSorter.ByteSequencesWriter;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.fst.CharSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.IntSequenceOutputs;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.Util;

/** In-memory structure for the dictionary (.dic) and affix (.aff) data of a hunspell dictionary. */
public class Dictionary {

  static final char[] NOFLAGS = new char[0];

  static final char FLAG_UNSET = (char) 0;
  private static final int DEFAULT_FLAGS = 65510;
  private static final char HIDDEN_FLAG = (char) 65511; // called 'ONLYUPCASEFLAG' in Hunspell

  // TODO: really for suffixes we should reverse the automaton and run them backwards
  private static final String PREFIX_CONDITION_REGEX_PATTERN = "%s.*";
  private static final String SUFFIX_CONDITION_REGEX_PATTERN = ".*%s";
  static final String DEFAULT_ENCODING = StandardCharsets.ISO_8859_1.name();

  FST<IntsRef> prefixes;
  FST<IntsRef> suffixes;
  Breaks breaks = Breaks.DEFAULT;

  // all condition checks used by prefixes and suffixes. these are typically re-used across
  // many affix stripping rules. so these are deduplicated, to save RAM.
  ArrayList<CharacterRunAutomaton> patterns = new ArrayList<>();

  // the entries in the .dic file, mapping to their set of flags.
  // the fst output is the ordinal list for flagLookup
  FST<IntsRef> words;
  // the list of unique flagsets (wordforms). theoretically huge, but practically
  // small (e.g. for polish this is 756), otherwise humans wouldn't be able to deal with it either.
  BytesRefHash flagLookup = new BytesRefHash();

  // the list of unique strip affixes.
  char[] stripData;
  int[] stripOffsets;

  // 4 chars per affix, each char representing an unsigned 2-byte integer
  char[] affixData = new char[32];
  private int currentAffix = 0;

  // offsets in affixData
  static final int AFFIX_FLAG = 0;
  static final int AFFIX_STRIP_ORD = 1;
  static final int AFFIX_CONDITION = 2;
  static final int AFFIX_APPEND = 3;

  // Default flag parsing strategy
  FlagParsingStrategy flagParsingStrategy = new SimpleFlagParsingStrategy();

  // AF entries
  private String[] aliases;
  private int aliasCount = 0;

  // AM entries
  private String[] morphAliases;
  private int morphAliasCount = 0;

  // st: morphological entries (either directly, or aliased from AM)
  private String[] stemExceptions = new String[8];
  private int stemExceptionCount = 0;
  // we set this during sorting, so we know to add an extra FST output.
  // when set, some words have exceptional stems, and the last entry is a pointer to stemExceptions
  boolean hasStemExceptions;

  boolean ignoreCase;
  boolean checkSharpS;
  boolean complexPrefixes;
  // if no affixes have continuation classes, no need to do 2-level affix stripping
  boolean twoStageAffix;

  char circumfix;
  char keepcase;
  char needaffix;
  char forbiddenword;
  char onlyincompound, compoundBegin, compoundMiddle, compoundEnd, compoundPermit;
  boolean checkCompoundCase;
  int compoundMin = 3, compoundMax = Integer.MAX_VALUE;
  List<CompoundRule> compoundRules; // nullable

  // ignored characters (dictionary, affix, inputs)
  private char[] ignore;

  // FSTs used for ICONV/OCONV, output ord pointing to replacement text
  FST<CharsRef> iconv;
  FST<CharsRef> oconv;

  boolean needsInputCleaning;
  boolean needsOutputCleaning;

  // true if we can strip suffixes "down to nothing"
  boolean fullStrip;

  // language declaration of the dictionary
  String language;
  // true if case algorithms should use alternate (Turkish/Azeri) mapping
  private boolean alternateCasing;

  /**
   * Creates a new Dictionary containing the information read from the provided InputStreams to
   * hunspell affix and dictionary files. You have to close the provided InputStreams yourself.
   *
   * @param tempDir Directory to use for offline sorting
   * @param tempFileNamePrefix prefix to use to generate temp file names
   * @param affix InputStream for reading the hunspell affix file (won't be closed).
   * @param dictionary InputStream for reading the hunspell dictionary file (won't be closed).
   * @throws IOException Can be thrown while reading from the InputStreams
   * @throws ParseException Can be thrown if the content of the files does not meet expected formats
   */
  public Dictionary(
      Directory tempDir, String tempFileNamePrefix, InputStream affix, InputStream dictionary)
      throws IOException, ParseException {
    this(tempDir, tempFileNamePrefix, affix, Collections.singletonList(dictionary), false);
  }

  /**
   * Creates a new Dictionary containing the information read from the provided InputStreams to
   * hunspell affix and dictionary files. You have to close the provided InputStreams yourself.
   *
   * @param tempDir Directory to use for offline sorting
   * @param tempFileNamePrefix prefix to use to generate temp file names
   * @param affix InputStream for reading the hunspell affix file (won't be closed).
   * @param dictionaries InputStream for reading the hunspell dictionary files (won't be closed).
   * @throws IOException Can be thrown while reading from the InputStreams
   * @throws ParseException Can be thrown if the content of the files does not meet expected formats
   */
  public Dictionary(
      Directory tempDir,
      String tempFileNamePrefix,
      InputStream affix,
      List<InputStream> dictionaries,
      boolean ignoreCase)
      throws IOException, ParseException {
    this.ignoreCase = ignoreCase;
    this.needsInputCleaning = ignoreCase;
    this.needsOutputCleaning = false; // set if we have an OCONV
    flagLookup.add(new BytesRef()); // no flags -> ord 0

    Path tempPath = getDefaultTempDir(); // TODO: make this configurable?
    Path aff = Files.createTempFile(tempPath, "affix", "aff");
    OutputStream out = new BufferedOutputStream(Files.newOutputStream(aff));
    InputStream aff1 = null;
    InputStream aff2 = null;
    boolean success = false;
    try {
      // copy contents of affix stream to temp file
      final byte[] buffer = new byte[1024 * 8];
      int len;
      while ((len = affix.read(buffer)) > 0) {
        out.write(buffer, 0, len);
      }
      out.close();

      // pass 1: get encoding
      aff1 = new BufferedInputStream(Files.newInputStream(aff));
      String encoding = getDictionaryEncoding(aff1);

      // pass 2: parse affixes
      CharsetDecoder decoder = getJavaEncoding(encoding);
      aff2 = new BufferedInputStream(Files.newInputStream(aff));
      readAffixFile(aff2, decoder);

      // read dictionary entries
      IndexOutput unsorted = mergeDictionaries(tempDir, tempFileNamePrefix, dictionaries, decoder);
      String sortedFile = sortWordsOffline(tempDir, tempFileNamePrefix, unsorted);
      words = readSortedDictionaries(tempDir, sortedFile);
      aliases = null; // no longer needed
      morphAliases = null; // no longer needed
      success = true;
    } finally {
      IOUtils.closeWhileHandlingException(out, aff1, aff2);
      if (success) {
        Files.delete(aff);
      } else {
        IOUtils.deleteFilesIgnoringExceptions(aff);
      }
    }
  }

  int formStep() {
    return hasStemExceptions ? 2 : 1;
  }

  /** Looks up Hunspell word forms from the dictionary */
  IntsRef lookupWord(char[] word, int offset, int length) {
    return lookup(words, word, offset, length);
  }

  // only for testing
  IntsRef lookupPrefix(char[] word) {
    return lookup(prefixes, word, 0, word.length);
  }

  // only for testing
  IntsRef lookupSuffix(char[] word) {
    return lookup(suffixes, word, 0, word.length);
  }

  IntsRef lookup(FST<IntsRef> fst, char[] word, int offset, int length) {
    if (fst == null) {
      return null;
    }
    final FST.BytesReader bytesReader = fst.getBytesReader();
    final FST.Arc<IntsRef> arc = fst.getFirstArc(new FST.Arc<>());
    // Accumulate output as we go
    final IntsRef NO_OUTPUT = fst.outputs.getNoOutput();
    IntsRef output = NO_OUTPUT;

    int l = offset + length;
    try {
      for (int i = offset, cp; i < l; i += Character.charCount(cp)) {
        cp = Character.codePointAt(word, i, l);
        if (fst.findTargetArc(cp, arc, arc, bytesReader) == null) {
          return null;
        } else if (arc.output() != NO_OUTPUT) {
          output = fst.outputs.add(output, arc.output());
        }
      }
      if (fst.findTargetArc(FST.END_LABEL, arc, arc, bytesReader) == null) {
        return null;
      } else if (arc.output() != NO_OUTPUT) {
        return fst.outputs.add(output, arc.output());
      } else {
        return output;
      }
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }
  }

  /**
   * Reads the affix file through the provided InputStream, building up the prefix and suffix maps
   *
   * @param affixStream InputStream to read the content of the affix file from
   * @param decoder CharsetDecoder to decode the content of the file
   * @throws IOException Can be thrown while reading from the InputStream
   */
  private void readAffixFile(InputStream affixStream, CharsetDecoder decoder)
      throws IOException, ParseException {
    TreeMap<String, List<Integer>> prefixes = new TreeMap<>();
    TreeMap<String, List<Integer>> suffixes = new TreeMap<>();
    Map<String, Integer> seenPatterns = new HashMap<>();

    // zero condition -> 0 ord
    seenPatterns.put(".*", 0);
    patterns.add(null);

    // zero strip -> 0 ord
    Map<String, Integer> seenStrips = new LinkedHashMap<>();
    seenStrips.put("", 0);

    LineNumberReader reader = new LineNumberReader(new InputStreamReader(affixStream, decoder));
    String line;
    while ((line = reader.readLine()) != null) {
      // ignore any BOM marker on first line
      if (reader.getLineNumber() == 1 && line.startsWith("\uFEFF")) {
        line = line.substring(1);
      }
      line = line.trim();
      if (line.isEmpty()) continue;

      String firstWord = line.split("\\s")[0];
      if ("AF".equals(firstWord)) {
        parseAlias(line);
      } else if ("AM".equals(firstWord)) {
        parseMorphAlias(line);
      } else if ("PFX".equals(firstWord)) {
        parseAffix(
            prefixes, line, reader, PREFIX_CONDITION_REGEX_PATTERN, seenPatterns, seenStrips);
      } else if ("SFX".equals(firstWord)) {
        parseAffix(
            suffixes, line, reader, SUFFIX_CONDITION_REGEX_PATTERN, seenPatterns, seenStrips);
      } else if ("FLAG".equals(firstWord)) {
        // Assume that the FLAG line comes before any prefix or suffixes
        // Store the strategy so it can be used when parsing the dic file
        flagParsingStrategy = getFlagParsingStrategy(line);
      } else if (line.equals("COMPLEXPREFIXES")) {
        complexPrefixes =
            true; // 2-stage prefix+1-stage suffix instead of 2-stage suffix+1-stage prefix
      } else if ("CIRCUMFIX".equals(firstWord)) {
        circumfix = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("KEEPCASE".equals(firstWord)) {
        keepcase = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("NEEDAFFIX".equals(firstWord) || "PSEUDOROOT".equals(firstWord)) {
        needaffix = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("ONLYINCOMPOUND".equals(firstWord)) {
        onlyincompound = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("CHECKSHARPS".equals(firstWord)) {
        checkSharpS = true;
      } else if ("IGNORE".equals(firstWord)) {
        ignore = singleArgument(reader, line).toCharArray();
        Arrays.sort(ignore);
        needsInputCleaning = true;
      } else if ("ICONV".equals(firstWord) || "OCONV".equals(firstWord)) {
        int num = Integer.parseInt(singleArgument(reader, line));
        FST<CharsRef> res = parseConversions(reader, num);
        if (line.startsWith("I")) {
          iconv = res;
          needsInputCleaning |= iconv != null;
        } else {
          oconv = res;
          needsOutputCleaning |= oconv != null;
        }
      } else if ("FULLSTRIP".equals(firstWord)) {
        fullStrip = true;
      } else if ("LANG".equals(firstWord)) {
        language = singleArgument(reader, line);
        int underscore = language.indexOf("_");
        String langCode = underscore < 0 ? language : language.substring(0, underscore);
        alternateCasing = langCode.equals("tr") || langCode.equals("az");
      } else if ("BREAK".equals(firstWord)) {
        breaks = parseBreaks(reader, line);
      } else if ("FORBIDDENWORD".equals(firstWord)) {
        forbiddenword = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("COMPOUNDMIN".equals(firstWord)) {
        compoundMin = Math.max(1, Integer.parseInt(singleArgument(reader, line)));
      } else if ("COMPOUNDWORDMAX".equals(firstWord)) {
        compoundMax = Math.max(1, Integer.parseInt(singleArgument(reader, line)));
      } else if ("COMPOUNDRULE".equals(firstWord)) {
        compoundRules = parseCompoundRules(reader, Integer.parseInt(singleArgument(reader, line)));
      } else if ("COMPOUNDBEGIN".equals(firstWord)) {
        compoundBegin = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("COMPOUNDMIDDLE".equals(firstWord)) {
        compoundMiddle = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("COMPOUNDEND".equals(firstWord)) {
        compoundEnd = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("COMPOUNDPERMITFLAG".equals(firstWord)) {
        compoundPermit = flagParsingStrategy.parseFlag(singleArgument(reader, line));
      } else if ("CHECKCOMPOUNDCASE".equals(firstWord)) {
        checkCompoundCase = true;
      }
    }

    this.prefixes = affixFST(prefixes);
    this.suffixes = affixFST(suffixes);

    int totalChars = 0;
    for (String strip : seenStrips.keySet()) {
      totalChars += strip.length();
    }
    stripData = new char[totalChars];
    stripOffsets = new int[seenStrips.size() + 1];
    int currentOffset = 0;
    int currentIndex = 0;
    for (String strip : seenStrips.keySet()) {
      stripOffsets[currentIndex++] = currentOffset;
      strip.getChars(0, strip.length(), stripData, currentOffset);
      currentOffset += strip.length();
    }
    assert currentIndex == seenStrips.size();
    stripOffsets[currentIndex] = currentOffset;
  }

  private String singleArgument(LineNumberReader reader, String line) throws ParseException {
    return splitBySpace(reader, line, 2)[1];
  }

  private String[] splitBySpace(LineNumberReader reader, String line, int expectedParts)
      throws ParseException {
    String[] parts = line.split("\\s+");
    if (parts.length < expectedParts
        || parts.length > expectedParts && !parts[expectedParts].startsWith("#")) {
      throw new ParseException("Invalid syntax", reader.getLineNumber());
    }
    return parts;
  }

  private List<CompoundRule> parseCompoundRules(LineNumberReader reader, int num)
      throws IOException, ParseException {
    List<CompoundRule> compoundRules = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      compoundRules.add(new CompoundRule(singleArgument(reader, reader.readLine()), this));
    }
    return compoundRules;
  }

  private Breaks parseBreaks(LineNumberReader reader, String line)
      throws IOException, ParseException {
    Set<String> starting = new LinkedHashSet<>();
    Set<String> ending = new LinkedHashSet<>();
    Set<String> middle = new LinkedHashSet<>();
    int num = Integer.parseInt(singleArgument(reader, line));
    for (int i = 0; i < num; i++) {
      String breakStr = singleArgument(reader, reader.readLine());
      if (breakStr.startsWith("^")) {
        starting.add(breakStr.substring(1));
      } else if (breakStr.endsWith("$")) {
        ending.add(breakStr.substring(0, breakStr.length() - 1));
      } else {
        middle.add(breakStr);
      }
    }
    return new Breaks(starting, ending, middle);
  }

  private FST<IntsRef> affixFST(TreeMap<String, List<Integer>> affixes) throws IOException {
    IntSequenceOutputs outputs = IntSequenceOutputs.getSingleton();
    FSTCompiler<IntsRef> fstCompiler = new FSTCompiler<>(FST.INPUT_TYPE.BYTE4, outputs);
    IntsRefBuilder scratch = new IntsRefBuilder();
    for (Map.Entry<String, List<Integer>> entry : affixes.entrySet()) {
      Util.toUTF32(entry.getKey(), scratch);
      List<Integer> entries = entry.getValue();
      IntsRef output = new IntsRef(entries.size());
      for (Integer c : entries) {
        output.ints[output.length++] = c;
      }
      fstCompiler.add(scratch.get(), output);
    }
    return fstCompiler.compile();
  }

  static String escapeDash(String re) {
    // we have to be careful, even though dash doesn't have a special meaning,
    // some dictionaries already escape it (e.g. pt_PT), so we don't want to nullify it
    StringBuilder escaped = new StringBuilder();
    for (int i = 0; i < re.length(); i++) {
      char c = re.charAt(i);
      if (c == '-') {
        escaped.append("\\-");
      } else {
        escaped.append(c);
        if (c == '\\' && i + 1 < re.length()) {
          escaped.append(re.charAt(i + 1));
          i++;
        }
      }
    }
    return escaped.toString();
  }

  /**
   * Parses a specific affix rule putting the result into the provided affix map
   *
   * @param affixes Map where the result of the parsing will be put
   * @param header Header line of the affix rule
   * @param reader BufferedReader to read the content of the rule from
   * @param conditionPattern {@link String#format(String, Object...)} pattern to be used to generate
   *     the condition regex pattern
   * @param seenPatterns map from condition -&gt; index of patterns, for deduplication.
   * @throws IOException Can be thrown while reading the rule
   */
  private void parseAffix(
      TreeMap<String, List<Integer>> affixes,
      String header,
      LineNumberReader reader,
      String conditionPattern,
      Map<String, Integer> seenPatterns,
      Map<String, Integer> seenStrips)
      throws IOException, ParseException {

    BytesRefBuilder scratch = new BytesRefBuilder();
    StringBuilder sb = new StringBuilder();
    String[] args = header.split("\\s+");

    boolean crossProduct = args[2].equals("Y");
    boolean isSuffix = conditionPattern.equals(SUFFIX_CONDITION_REGEX_PATTERN);

    int numLines = Integer.parseInt(args[3]);
    affixData = ArrayUtil.grow(affixData, currentAffix * 4 + numLines * 4);

    for (int i = 0; i < numLines; i++) {
      String line = reader.readLine();
      String[] ruleArgs = line.split("\\s+");

      // from the manpage: PFX flag stripping prefix [condition [morphological_fields...]]
      // condition is optional
      if (ruleArgs.length < 4) {
        throw new ParseException(
            "The affix file contains a rule with less than four elements: " + line,
            reader.getLineNumber());
      }

      char flag = flagParsingStrategy.parseFlag(ruleArgs[1]);
      String strip = ruleArgs[2].equals("0") ? "" : ruleArgs[2];
      String affixArg = ruleArgs[3];
      char[] appendFlags = null;

      // first: parse continuation classes out of affix
      int flagSep = affixArg.lastIndexOf('/');
      if (flagSep != -1) {
        String flagPart = affixArg.substring(flagSep + 1);
        affixArg = affixArg.substring(0, flagSep);

        if (aliasCount > 0) {
          flagPart = getAliasValue(Integer.parseInt(flagPart));
        }

        appendFlags = flagParsingStrategy.parseFlags(flagPart);
        Arrays.sort(appendFlags);
        twoStageAffix = true;
      }
      // zero affix -> empty string
      if ("0".equals(affixArg)) {
        affixArg = "";
      }

      String condition = ruleArgs.length > 4 ? ruleArgs[4] : ".";
      // at least the gascon affix file has this issue
      if (condition.startsWith("[") && condition.indexOf(']') == -1) {
        condition = condition + "]";
      }
      // "dash hasn't got special meaning" (we must escape it)
      if (condition.indexOf('-') >= 0) {
        condition = escapeDash(condition);
      }

      final String regex;
      if (".".equals(condition)) {
        regex = ".*"; // Zero condition is indicated by dot
      } else if (condition.equals(strip)) {
        regex = ".*"; // TODO: optimize this better:
        // if we remove 'strip' from condition, we don't have to append 'strip' to check it...!
        // but this is complicated...
      } else {
        regex = String.format(Locale.ROOT, conditionPattern, condition);
      }

      // deduplicate patterns
      Integer patternIndex = seenPatterns.get(regex);
      if (patternIndex == null) {
        patternIndex = patterns.size();
        if (patternIndex > Short.MAX_VALUE) {
          throw new UnsupportedOperationException(
              "Too many patterns, please report this to dev@lucene.apache.org");
        }
        seenPatterns.put(regex, patternIndex);
        CharacterRunAutomaton pattern =
            new CharacterRunAutomaton(new RegExp(regex, RegExp.NONE).toAutomaton());
        patterns.add(pattern);
      }

      Integer stripOrd = seenStrips.get(strip);
      if (stripOrd == null) {
        stripOrd = seenStrips.size();
        seenStrips.put(strip, stripOrd);
        if (stripOrd > Character.MAX_VALUE) {
          throw new UnsupportedOperationException(
              "Too many unique strips, please report this to dev@lucene.apache.org");
        }
      }

      if (appendFlags == null) {
        appendFlags = NOFLAGS;
      }

      encodeFlags(scratch, appendFlags);
      int appendFlagsOrd = flagLookup.add(scratch.get());
      if (appendFlagsOrd < 0) {
        // already exists in our hash
        appendFlagsOrd = (-appendFlagsOrd) - 1;
      } else if (appendFlagsOrd > Short.MAX_VALUE) {
        // this limit is probably flexible, but it's a good sanity check too
        throw new UnsupportedOperationException(
            "Too many unique append flags, please report this to dev@lucene.apache.org");
      }

      int dataStart = currentAffix * 4;
      affixData[dataStart + AFFIX_FLAG] = flag;
      affixData[dataStart + AFFIX_STRIP_ORD] = (char) stripOrd.intValue();
      // encode crossProduct into patternIndex
      int patternOrd = patternIndex << 1 | (crossProduct ? 1 : 0);
      affixData[dataStart + AFFIX_CONDITION] = (char) patternOrd;
      affixData[dataStart + AFFIX_APPEND] = (char) appendFlagsOrd;

      if (needsInputCleaning) {
        CharSequence cleaned = cleanInput(affixArg, sb);
        affixArg = cleaned.toString();
      }

      if (isSuffix) {
        affixArg = new StringBuilder(affixArg).reverse().toString();
      }

      affixes.computeIfAbsent(affixArg, __ -> new ArrayList<>()).add(currentAffix);
      currentAffix++;
    }
  }

  char affixData(int affixIndex, int offset) {
    return affixData[affixIndex * 4 + offset];
  }

  private FST<CharsRef> parseConversions(LineNumberReader reader, int num)
      throws IOException, ParseException {
    Map<String, String> mappings = new TreeMap<>();

    for (int i = 0; i < num; i++) {
      String[] parts = splitBySpace(reader, reader.readLine(), 3);
      if (mappings.put(parts[1], parts[2]) != null) {
        throw new IllegalStateException("duplicate mapping specified for: " + parts[1]);
      }
    }

    Outputs<CharsRef> outputs = CharSequenceOutputs.getSingleton();
    FSTCompiler<CharsRef> fstCompiler = new FSTCompiler<>(FST.INPUT_TYPE.BYTE2, outputs);
    IntsRefBuilder scratchInts = new IntsRefBuilder();
    for (Map.Entry<String, String> entry : mappings.entrySet()) {
      Util.toUTF16(entry.getKey(), scratchInts);
      fstCompiler.add(scratchInts.get(), new CharsRef(entry.getValue()));
    }

    return fstCompiler.compile();
  }

  /** pattern accepts optional BOM + SET + any whitespace */
  static final Pattern ENCODING_PATTERN = Pattern.compile("^(\u00EF\u00BB\u00BF)?SET\\s+");

  /**
   * Parses the encoding specified in the affix file readable through the provided InputStream
   *
   * @param affix InputStream for reading the affix file
   * @return Encoding specified in the affix file
   * @throws IOException Can be thrown while reading from the InputStream
   */
  static String getDictionaryEncoding(InputStream affix) throws IOException {
    final StringBuilder encoding = new StringBuilder();
    for (; ; ) {
      encoding.setLength(0);
      int ch;
      while ((ch = affix.read()) >= 0) {
        if (ch == '\n') {
          break;
        }
        if (ch != '\r') {
          encoding.append((char) ch);
        }
      }
      if (encoding.length() == 0
          || encoding.charAt(0) == '#'
          ||
          // this test only at the end as ineffective but would allow lines only containing spaces:
          encoding.toString().trim().length() == 0) {
        if (ch < 0) {
          return DEFAULT_ENCODING;
        }
        continue;
      }
      Matcher matcher = ENCODING_PATTERN.matcher(encoding);
      if (matcher.find()) {
        int last = matcher.end();
        return encoding.substring(last).trim();
      }
      return DEFAULT_ENCODING;
    }
  }

  static final Map<String, String> CHARSET_ALIASES =
      Map.of("microsoft-cp1251", "windows-1251", "TIS620-2533", "TIS-620");

  /**
   * Retrieves the CharsetDecoder for the given encoding. Note, This isn't perfect as I think
   * ISCII-DEVANAGARI and MICROSOFT-CP1251 etc are allowed...
   *
   * @param encoding Encoding to retrieve the CharsetDecoder for
   * @return CharSetDecoder for the given encoding
   */
  private CharsetDecoder getJavaEncoding(String encoding) {
    if ("ISO8859-14".equals(encoding)) {
      return new ISO8859_14Decoder();
    }
    String canon = CHARSET_ALIASES.get(encoding);
    if (canon != null) {
      encoding = canon;
    }
    Charset charset = Charset.forName(encoding);
    return charset.newDecoder().onMalformedInput(CodingErrorAction.REPLACE);
  }

  /**
   * Determines the appropriate {@link FlagParsingStrategy} based on the FLAG definition line taken
   * from the affix file
   *
   * @param flagLine Line containing the flag information
   * @return FlagParsingStrategy that handles parsing flags in the way specified in the FLAG
   *     definition
   */
  static FlagParsingStrategy getFlagParsingStrategy(String flagLine) {
    String[] parts = flagLine.split("\\s+");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Illegal FLAG specification: " + flagLine);
    }
    String flagType = parts[1];

    if ("num".equals(flagType)) {
      return new NumFlagParsingStrategy();
    } else if ("UTF-8".equals(flagType)) {
      return new SimpleFlagParsingStrategy();
    } else if ("long".equals(flagType)) {
      return new DoubleASCIIFlagParsingStrategy();
    }

    throw new IllegalArgumentException("Unknown flag type: " + flagType);
  }

  private static final char FLAG_SEPARATOR = 0x1f; // flag separator after escaping
  private static final char MORPH_SEPARATOR =
      0x1e; // separator for boundary of entry (may be followed by morph data)

  private String unescapeEntry(String entry) {
    StringBuilder sb = new StringBuilder();
    int end = morphBoundary(entry);
    for (int i = 0; i < end; i++) {
      char ch = entry.charAt(i);
      if (ch == '\\' && i + 1 < entry.length()) {
        sb.append(entry.charAt(i + 1));
        i++;
      } else if (ch == '/') {
        sb.append(FLAG_SEPARATOR);
      } else if (!shouldSkipEscapedChar(ch)) {
        sb.append(ch);
      }
    }
    sb.append(MORPH_SEPARATOR);
    if (end < entry.length()) {
      for (int i = end; i < entry.length(); i++) {
        char c = entry.charAt(i);
        if (!shouldSkipEscapedChar(c)) {
          sb.append(c);
        }
      }
    }
    return sb.toString();
  }

  private static boolean shouldSkipEscapedChar(char ch) {
    return ch == FLAG_SEPARATOR
        || ch == MORPH_SEPARATOR; // BINARY EXECUTABLES EMBEDDED IN ZULU DICTIONARIES!!!!!!!
  }

  static int morphBoundary(String line) {
    int end = indexOfSpaceOrTab(line, 0);
    if (end == -1) {
      return line.length();
    }
    while (end >= 0 && end < line.length()) {
      if (line.charAt(end) == '\t'
          || end + 3 < line.length()
              && Character.isLetter(line.charAt(end + 1))
              && Character.isLetter(line.charAt(end + 2))
              && line.charAt(end + 3) == ':') {
        break;
      }
      end = indexOfSpaceOrTab(line, end + 1);
    }
    if (end == -1) {
      return line.length();
    }
    return end;
  }

  static int indexOfSpaceOrTab(String text, int start) {
    int pos1 = text.indexOf('\t', start);
    int pos2 = text.indexOf(' ', start);
    if (pos1 >= 0 && pos2 >= 0) {
      return Math.min(pos1, pos2);
    } else {
      return Math.max(pos1, pos2);
    }
  }

  private IndexOutput mergeDictionaries(
      Directory tempDir,
      String tempFileNamePrefix,
      List<InputStream> dictionaries,
      CharsetDecoder decoder)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    IndexOutput unsorted = tempDir.createTempOutput(tempFileNamePrefix, "dat", IOContext.DEFAULT);
    try (ByteSequencesWriter writer = new ByteSequencesWriter(unsorted)) {
      for (InputStream dictionary : dictionaries) {
        BufferedReader lines = new BufferedReader(new InputStreamReader(dictionary, decoder));
        lines.readLine(); // first line is number of entries (approximately, sometimes)

        String line;
        while ((line = lines.readLine()) != null) {
          // wild and unpredictable code comment rules
          if (line.isEmpty()
              || line.charAt(0) == '/'
              || line.charAt(0) == '#'
              || line.charAt(0) == '\t') {
            continue;
          }
          line = unescapeEntry(line);
          // if we havent seen any stem exceptions, try to parse one
          if (!hasStemExceptions) {
            int morphStart = line.indexOf(MORPH_SEPARATOR);
            if (morphStart >= 0 && morphStart < line.length()) {
              hasStemExceptions = parseStemException(line.substring(morphStart + 1)) != null;
            }
          }

          writeNormalizedWordEntry(sb, writer, line);
        }
      }
      CodecUtil.writeFooter(unsorted);
    }
    return unsorted;
  }

  private void writeNormalizedWordEntry(
      StringBuilder reuse, ByteSequencesWriter writer, String line) throws IOException {
    int flagSep = line.indexOf(FLAG_SEPARATOR);
    int morphSep = line.indexOf(MORPH_SEPARATOR);
    assert morphSep > 0;
    assert morphSep > flagSep;
    int sep = flagSep < 0 ? morphSep : flagSep;

    CharSequence toWrite;
    if (needsInputCleaning) {
      cleanInput(line, sep, reuse);
      reuse.append(line, sep, line.length());
      toWrite = reuse;
    } else {
      toWrite = line;
    }

    String written = toWrite.toString();
    sep = written.length() - (line.length() - sep);
    writer.write(written.getBytes(StandardCharsets.UTF_8));

    WordCase wordCase = WordCase.caseOf(written, sep);
    if (wordCase == WordCase.MIXED || wordCase == WordCase.UPPER && flagSep > 0) {
      addHiddenCapitalizedWord(reuse, writer, written.substring(0, sep), written.substring(sep));
    }
  }

  private void addHiddenCapitalizedWord(
      StringBuilder reuse, ByteSequencesWriter writer, String word, String afterSep)
      throws IOException {
    reuse.setLength(0);
    reuse.append(Character.toUpperCase(word.charAt(0)));
    for (int i = 1; i < word.length(); i++) {
      reuse.append(caseFold(word.charAt(i)));
    }
    reuse.append(FLAG_SEPARATOR);
    flagParsingStrategy.appendFlag(HIDDEN_FLAG, reuse);
    reuse.append(afterSep, afterSep.charAt(0) == FLAG_SEPARATOR ? 1 : 0, afterSep.length());
    writer.write(reuse.toString().getBytes(StandardCharsets.UTF_8));
  }

  private String sortWordsOffline(
      Directory tempDir, String tempFileNamePrefix, IndexOutput unsorted) throws IOException {
    OfflineSorter sorter =
        new OfflineSorter(
            tempDir,
            tempFileNamePrefix,
            new Comparator<>() {
              final BytesRef scratch1 = new BytesRef();
              final BytesRef scratch2 = new BytesRef();

              private void initScratch(BytesRef o, BytesRef scratch) {
                scratch.bytes = o.bytes;
                scratch.offset = o.offset;
                scratch.length = o.length;

                for (int i = scratch.length - 1; i >= 0; i--) {
                  if (scratch.bytes[scratch.offset + i] == FLAG_SEPARATOR
                      || scratch.bytes[scratch.offset + i] == MORPH_SEPARATOR) {
                    scratch.length = i;
                    break;
                  }
                }
              }

              @Override
              public int compare(BytesRef o1, BytesRef o2) {
                initScratch(o1, scratch1);
                initScratch(o2, scratch2);

                int cmp = scratch1.compareTo(scratch2);
                if (cmp == 0) {
                  // tie break on whole row
                  return o1.compareTo(o2);
                } else {
                  return cmp;
                }
              }
            });

    String sorted;
    boolean success = false;
    try {
      sorted = sorter.sort(unsorted.getName());
      success = true;
    } finally {
      if (success) {
        tempDir.deleteFile(unsorted.getName());
      } else {
        IOUtils.deleteFilesIgnoringExceptions(tempDir, unsorted.getName());
      }
    }
    return sorted;
  }

  private FST<IntsRef> readSortedDictionaries(Directory tempDir, String sorted) throws IOException {
    boolean success = false;

    EntryGrouper grouper = new EntryGrouper();

    try (ByteSequencesReader reader =
        new ByteSequencesReader(tempDir.openChecksumInput(sorted, IOContext.READONCE), sorted)) {

      // TODO: the flags themselves can be double-chars (long) or also numeric
      // either way the trick is to encode them as char... but they must be parsed differently

      while (true) {
        BytesRef scratch = reader.next();
        if (scratch == null) {
          break;
        }

        String line = scratch.utf8ToString();
        String entry;
        char[] wordForm;
        int end;

        int flagSep = line.indexOf(FLAG_SEPARATOR);
        if (flagSep == -1) {
          wordForm = NOFLAGS;
          end = line.indexOf(MORPH_SEPARATOR);
          entry = line.substring(0, end);
        } else {
          end = line.indexOf(MORPH_SEPARATOR);
          String flagPart = line.substring(flagSep + 1, end);
          if (aliasCount > 0) {
            flagPart = getAliasValue(Integer.parseInt(flagPart));
          }

          wordForm = flagParsingStrategy.parseFlags(flagPart);
          Arrays.sort(wordForm);
          entry = line.substring(0, flagSep);
        }
        // we possibly have morphological data
        int stemExceptionID = 0;
        if (hasStemExceptions && end + 1 < line.length()) {
          String stemException = parseStemException(line.substring(end + 1));
          if (stemException != null) {
            stemExceptions = ArrayUtil.grow(stemExceptions, stemExceptionCount + 1);
            stemExceptionID =
                stemExceptionCount + 1; // we use '0' to indicate no exception for the form
            stemExceptions[stemExceptionCount++] = stemException;
          }
        }

        grouper.add(entry, wordForm, stemExceptionID);
      }

      // finalize last entry
      grouper.flushGroup();
      success = true;
      return grouper.words.compile();
    } finally {
      if (success) {
        tempDir.deleteFile(sorted);
      } else {
        IOUtils.deleteFilesIgnoringExceptions(tempDir, sorted);
      }
    }
  }

  boolean isDotICaseChangeDisallowed(char[] word) {
    return word[0] == 'İ' && !alternateCasing;
  }

  private class EntryGrouper {
    final FSTCompiler<IntsRef> words =
        new FSTCompiler<>(FST.INPUT_TYPE.BYTE4, IntSequenceOutputs.getSingleton());
    private final List<char[]> group = new ArrayList<>();
    private final List<Integer> stemExceptionIDs = new ArrayList<>();
    private final BytesRefBuilder flagsScratch = new BytesRefBuilder();
    private final IntsRefBuilder scratchInts = new IntsRefBuilder();
    private String currentEntry = null;

    void add(String entry, char[] flags, int stemExceptionID) throws IOException {
      if (!entry.equals(currentEntry)) {
        if (currentEntry != null) {
          if (entry.compareTo(currentEntry) < 0) {
            throw new IllegalArgumentException("out of order: " + entry + " < " + currentEntry);
          }
          flushGroup();
        }
        currentEntry = entry;
      }

      group.add(flags);
      if (hasStemExceptions) {
        stemExceptionIDs.add(stemExceptionID);
      }
    }

    void flushGroup() throws IOException {
      IntsRefBuilder currentOrds = new IntsRefBuilder();

      boolean hasNonHidden = false;
      for (char[] flags : group) {
        if (!hasHiddenFlag(flags)) {
          hasNonHidden = true;
          break;
        }
      }

      for (int i = 0; i < group.size(); i++) {
        char[] flags = group.get(i);
        if (hasNonHidden && hasHiddenFlag(flags)) {
          continue;
        }

        encodeFlags(flagsScratch, flags);
        int ord = flagLookup.add(flagsScratch.get());
        if (ord < 0) {
          ord = -ord - 1; // already exists in our hash
        }
        currentOrds.append(ord);
        if (hasStemExceptions) {
          currentOrds.append(stemExceptionIDs.get(i));
        }
      }

      Util.toUTF32(currentEntry, scratchInts);
      words.add(scratchInts.get(), currentOrds.get());

      group.clear();
      stemExceptionIDs.clear();
    }
  }

  static boolean hasHiddenFlag(char[] flags) {
    return hasFlag(flags, HIDDEN_FLAG);
  }

  char[] decodeFlags(int entryId, BytesRef b) {
    this.flagLookup.get(entryId, b);

    if (b.length == 0) {
      return CharsRef.EMPTY_CHARS;
    }
    int len = b.length >>> 1;
    char[] flags = new char[len];
    int upto = 0;
    int end = b.offset + b.length;
    for (int i = b.offset; i < end; i += 2) {
      flags[upto++] = (char) ((b.bytes[i] << 8) | (b.bytes[i + 1] & 0xff));
    }
    return flags;
  }

  private static void encodeFlags(BytesRefBuilder b, char[] flags) {
    int len = flags.length << 1;
    b.grow(len);
    b.clear();
    for (int flag : flags) {
      b.append((byte) ((flag >> 8) & 0xff));
      b.append((byte) (flag & 0xff));
    }
  }

  private void parseAlias(String line) {
    String[] ruleArgs = line.split("\\s+");
    if (aliases == null) {
      // first line should be the aliases count
      final int count = Integer.parseInt(ruleArgs[1]);
      aliases = new String[count];
    } else {
      // an alias can map to no flags
      String aliasValue = ruleArgs.length == 1 ? "" : ruleArgs[1];
      aliases[aliasCount++] = aliasValue;
    }
  }

  private String getAliasValue(int id) {
    try {
      return aliases[id - 1];
    } catch (IndexOutOfBoundsException ex) {
      throw new IllegalArgumentException("Bad flag alias number:" + id, ex);
    }
  }

  String getStemException(int id) {
    return stemExceptions[id - 1];
  }

  private void parseMorphAlias(String line) {
    if (morphAliases == null) {
      // first line should be the aliases count
      final int count = Integer.parseInt(line.substring(3));
      morphAliases = new String[count];
    } else {
      String arg = line.substring(2); // leave the space
      morphAliases[morphAliasCount++] = arg;
    }
  }

  private String parseStemException(String morphData) {
    // first see if it's an alias
    if (morphAliasCount > 0) {
      try {
        int alias = Integer.parseInt(morphData.trim());
        morphData = morphAliases[alias - 1];
      } catch (NumberFormatException e) {
        // fine
      }
    }
    // try to parse morph entry
    int index = morphData.indexOf(" st:");
    if (index < 0) {
      index = morphData.indexOf("\tst:");
    }
    if (index >= 0) {
      int endIndex = indexOfSpaceOrTab(morphData, index + 1);
      if (endIndex < 0) {
        endIndex = morphData.length();
      }
      return morphData.substring(index + 4, endIndex);
    }
    return null;
  }

  boolean isForbiddenWord(char[] word, int length, BytesRef scratch) {
    if (forbiddenword != FLAG_UNSET) {
      IntsRef forms = lookupWord(word, 0, length);
      return forms != null && hasFlag(forms, forbiddenword, scratch);
    }
    return false;
  }

  boolean hasFlag(IntsRef forms, char flag, BytesRef scratch) {
    int formStep = formStep();
    for (int i = 0; i < forms.length; i += formStep) {
      if (hasFlag(forms.ints[forms.offset + i], flag, scratch)) {
        return true;
      }
    }
    return false;
  }

  /** Abstraction of the process of parsing flags taken from the affix and dic files */
  abstract static class FlagParsingStrategy {

    /**
     * Parses the given String into a single flag
     *
     * @param rawFlag String to parse into a flag
     * @return Parsed flag
     */
    char parseFlag(String rawFlag) {
      char[] flags = parseFlags(rawFlag);
      if (flags.length != 1) {
        throw new IllegalArgumentException("expected only one flag, got: " + rawFlag);
      }
      return flags[0];
    }

    /**
     * Parses the given String into multiple flags
     *
     * @param rawFlags String to parse into flags
     * @return Parsed flags
     */
    abstract char[] parseFlags(String rawFlags);

    abstract void appendFlag(char flag, StringBuilder to);
  }

  /**
   * Simple implementation of {@link FlagParsingStrategy} that treats the chars in each String as a
   * individual flags. Can be used with both the ASCII and UTF-8 flag types.
   */
  private static class SimpleFlagParsingStrategy extends FlagParsingStrategy {
    @Override
    public char[] parseFlags(String rawFlags) {
      return rawFlags.toCharArray();
    }

    @Override
    void appendFlag(char flag, StringBuilder to) {
      to.append(flag);
    }
  }

  /**
   * Implementation of {@link FlagParsingStrategy} that assumes each flag is encoded in its
   * numerical form. In the case of multiple flags, each number is separated by a comma.
   */
  private static class NumFlagParsingStrategy extends FlagParsingStrategy {
    @Override
    public char[] parseFlags(String rawFlags) {
      String[] rawFlagParts = rawFlags.trim().split(",");
      char[] flags = new char[rawFlagParts.length];
      int upto = 0;

      for (String rawFlagPart : rawFlagParts) {
        // note, removing the trailing X/leading I for nepali... what is the rule here?!
        String replacement = rawFlagPart.replaceAll("[^0-9]", "");
        // note, ignoring empty flags (this happens in danish, for example)
        if (replacement.isEmpty()) {
          continue;
        }
        int flag = Integer.parseInt(replacement);
        if (flag == FLAG_UNSET || flag >= Character.MAX_VALUE) { // read default flags as well
          throw new IllegalArgumentException(
              "Num flags should be between 0 and " + DEFAULT_FLAGS + ", found " + flag);
        }
        flags[upto++] = (char) flag;
      }

      if (upto < flags.length) {
        flags = ArrayUtil.copyOfSubArray(flags, 0, upto);
      }
      return flags;
    }

    @Override
    void appendFlag(char flag, StringBuilder to) {
      to.append((int) flag);
      to.append(",");
    }
  }

  /**
   * Implementation of {@link FlagParsingStrategy} that assumes each flag is encoded as two ASCII
   * characters whose codes must be combined into a single character.
   */
  private static class DoubleASCIIFlagParsingStrategy extends FlagParsingStrategy {

    @Override
    public char[] parseFlags(String rawFlags) {
      if (rawFlags.length() == 0) {
        return new char[0];
      }

      StringBuilder builder = new StringBuilder();
      if (rawFlags.length() % 2 == 1) {
        throw new IllegalArgumentException(
            "Invalid flags (should be even number of characters): " + rawFlags);
      }
      for (int i = 0; i < rawFlags.length(); i += 2) {
        char f1 = rawFlags.charAt(i);
        char f2 = rawFlags.charAt(i + 1);
        if (f1 >= 256 || f2 >= 256) {
          throw new IllegalArgumentException(
              "Invalid flags (LONG flags must be double ASCII): " + rawFlags);
        }
        char combined = (char) (f1 << 8 | f2);
        builder.append(combined);
      }

      char[] flags = new char[builder.length()];
      builder.getChars(0, builder.length(), flags, 0);
      return flags;
    }

    @Override
    void appendFlag(char flag, StringBuilder to) {
      to.append((char) (flag >> 8));
      to.append((char) (flag & 0xff));
    }
  }

  boolean hasFlag(int entryId, char flag, BytesRef scratch) {
    return flag != FLAG_UNSET && hasFlag(decodeFlags(entryId, scratch), flag);
  }

  static boolean hasFlag(char[] flags, char flag) {
    return flag != FLAG_UNSET && Arrays.binarySearch(flags, flag) >= 0;
  }

  CharSequence cleanInput(CharSequence input, StringBuilder reuse) {
    return cleanInput(input, input.length(), reuse);
  }

  private CharSequence cleanInput(CharSequence input, int prefixLength, StringBuilder reuse) {
    reuse.setLength(0);

    for (int i = 0; i < prefixLength; i++) {
      char ch = input.charAt(i);

      if (ignore != null && Arrays.binarySearch(ignore, ch) >= 0) {
        continue;
      }

      if (ignoreCase && iconv == null) {
        // if we have no input conversion mappings, do this on-the-fly
        ch = caseFold(ch);
      }

      reuse.append(ch);
    }

    if (iconv != null) {
      try {
        applyMappings(iconv, reuse);
      } catch (IOException bogus) {
        throw new RuntimeException(bogus);
      }
      if (ignoreCase) {
        for (int i = 0; i < reuse.length(); i++) {
          reuse.setCharAt(i, caseFold(reuse.charAt(i)));
        }
      }
    }

    return reuse;
  }

  /** folds single character (according to LANG if present) */
  char caseFold(char c) {
    if (alternateCasing) {
      if (c == 'I') {
        return 'ı';
      } else if (c == 'İ') {
        return 'i';
      } else {
        return Character.toLowerCase(c);
      }
    } else {
      return Character.toLowerCase(c);
    }
  }

  // TODO: this could be more efficient!
  static void applyMappings(FST<CharsRef> fst, StringBuilder sb) throws IOException {
    final FST.BytesReader bytesReader = fst.getBytesReader();
    final FST.Arc<CharsRef> firstArc = fst.getFirstArc(new FST.Arc<>());
    final CharsRef NO_OUTPUT = fst.outputs.getNoOutput();

    // temporary stuff
    final FST.Arc<CharsRef> arc = new FST.Arc<>();
    int longestMatch;
    CharsRef longestOutput;

    for (int i = 0; i < sb.length(); i++) {
      arc.copyFrom(firstArc);
      CharsRef output = NO_OUTPUT;
      longestMatch = -1;
      longestOutput = null;

      for (int j = i; j < sb.length(); j++) {
        char ch = sb.charAt(j);
        if (fst.findTargetArc(ch, arc, arc, bytesReader) == null) {
          break;
        } else {
          output = fst.outputs.add(output, arc.output());
        }
        if (arc.isFinal()) {
          longestOutput = fst.outputs.add(output, arc.nextFinalOutput());
          longestMatch = j;
        }
      }

      if (longestMatch >= 0) {
        sb.delete(i, longestMatch + 1);
        sb.insert(i, longestOutput);
        i += (longestOutput.length - 1);
      }
    }
  }

  /** Returns true if this dictionary was constructed with the {@code ignoreCase} option */
  public boolean getIgnoreCase() {
    return ignoreCase;
  }

  private static Path DEFAULT_TEMP_DIR;

  /** Used by test framework */
  @SuppressWarnings("unused")
  public static void setDefaultTempDir(Path tempDir) {
    DEFAULT_TEMP_DIR = tempDir;
  }

  /**
   * Returns the default temporary directory. By default, java.io.tmpdir. If not accessible or not
   * available, an IOException is thrown
   */
  static synchronized Path getDefaultTempDir() throws IOException {
    if (DEFAULT_TEMP_DIR == null) {
      // Lazy init
      String tempDirPath = System.getProperty("java.io.tmpdir");
      if (tempDirPath == null) {
        throw new IOException("Java has no temporary folder property (java.io.tmpdir)?");
      }
      Path tempDirectory = Paths.get(tempDirPath);
      if (!Files.isWritable(tempDirectory)) {
        throw new IOException(
            "Java's temporary folder not present or writeable?: " + tempDirectory.toAbsolutePath());
      }
      DEFAULT_TEMP_DIR = tempDirectory;
    }

    return DEFAULT_TEMP_DIR;
  }

  /** Possible word breaks according to BREAK directives */
  static class Breaks {
    private static final Set<String> MINUS = Collections.singleton("-");
    static final Breaks DEFAULT = new Breaks(MINUS, MINUS, MINUS);
    final String[] starting, ending, middle;

    Breaks(Collection<String> starting, Collection<String> ending, Collection<String> middle) {
      this.starting = starting.toArray(new String[0]);
      this.ending = ending.toArray(new String[0]);
      this.middle = middle.toArray(new String[0]);
    }

    boolean isNotEmpty() {
      return middle.length > 0 || starting.length > 0 || ending.length > 0;
    }
  }
}
