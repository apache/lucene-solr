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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteArrayDataOutput;
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
import org.apache.lucene.util.OfflineSorter.ByteSequencesReader;
import org.apache.lucene.util.OfflineSorter.ByteSequencesWriter;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.CharSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.IntSequenceOutputs;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.Util;

/**
 * In-memory structure for the dictionary (.dic) and affix (.aff)
 * data of a hunspell dictionary.
 */
public class Dictionary {

  static final char[] NOFLAGS = new char[0];
  
  private static final String ALIAS_KEY = "AF";
  private static final String MORPH_ALIAS_KEY = "AM";
  private static final String PREFIX_KEY = "PFX";
  private static final String SUFFIX_KEY = "SFX";
  private static final String FLAG_KEY = "FLAG";
  private static final String COMPLEXPREFIXES_KEY = "COMPLEXPREFIXES";
  private static final String CIRCUMFIX_KEY = "CIRCUMFIX";
  private static final String IGNORE_KEY = "IGNORE";
  private static final String ICONV_KEY = "ICONV";
  private static final String OCONV_KEY = "OCONV";
  private static final String FULLSTRIP_KEY = "FULLSTRIP";
  private static final String LANG_KEY = "LANG";
  private static final String KEEPCASE_KEY = "KEEPCASE";
  private static final String NEEDAFFIX_KEY = "NEEDAFFIX";
  private static final String PSEUDOROOT_KEY = "PSEUDOROOT";
  private static final String ONLYINCOMPOUND_KEY = "ONLYINCOMPOUND";

  private static final String NUM_FLAG_TYPE = "num";
  private static final String UTF8_FLAG_TYPE = "UTF-8";
  private static final String LONG_FLAG_TYPE = "long";
  
  // TODO: really for suffixes we should reverse the automaton and run them backwards
  private static final String PREFIX_CONDITION_REGEX_PATTERN = "%s.*";
  private static final String SUFFIX_CONDITION_REGEX_PATTERN = ".*%s";

  FST<IntsRef> prefixes;
  FST<IntsRef> suffixes;
  
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
  
  // 8 bytes per affix
  byte[] affixData = new byte[64];
  private int currentAffix = 0;

  private FlagParsingStrategy flagParsingStrategy = new SimpleFlagParsingStrategy(); // Default flag parsing strategy

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
  
  private final Path tempPath = getDefaultTempDir(); // TODO: make this configurable?
  
  boolean ignoreCase;
  boolean complexPrefixes;
  boolean twoStageAffix; // if no affixes have continuation classes, no need to do 2-level affix stripping
  
  int circumfix = -1; // circumfix flag, or -1 if one is not defined
  int keepcase = -1;  // keepcase flag, or -1 if one is not defined
  int needaffix = -1; // needaffix flag, or -1 if one is not defined
  int onlyincompound = -1; // onlyincompound flag, or -1 if one is not defined
  
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
  boolean alternateCasing;

  /**
   * Creates a new Dictionary containing the information read from the provided InputStreams to hunspell affix
   * and dictionary files.
   * You have to close the provided InputStreams yourself.
   *
   * @param tempDir Directory to use for offline sorting
   * @param tempFileNamePrefix prefix to use to generate temp file names
   * @param affix InputStream for reading the hunspell affix file (won't be closed).
   * @param dictionary InputStream for reading the hunspell dictionary file (won't be closed).
   * @throws IOException Can be thrown while reading from the InputStreams
   * @throws ParseException Can be thrown if the content of the files does not meet expected formats
   */
  public Dictionary(Directory tempDir, String tempFileNamePrefix, InputStream affix, InputStream dictionary) throws IOException, ParseException {
    this(tempDir, tempFileNamePrefix, affix, Collections.singletonList(dictionary), false);
  }

  /**
   * Creates a new Dictionary containing the information read from the provided InputStreams to hunspell affix
   * and dictionary files.
   * You have to close the provided InputStreams yourself.
   *
   * @param tempDir Directory to use for offline sorting
   * @param tempFileNamePrefix prefix to use to generate temp file names
   * @param affix InputStream for reading the hunspell affix file (won't be closed).
   * @param dictionaries InputStream for reading the hunspell dictionary files (won't be closed).
   * @throws IOException Can be thrown while reading from the InputStreams
   * @throws ParseException Can be thrown if the content of the files does not meet expected formats
   */
  public Dictionary(Directory tempDir, String tempFileNamePrefix, InputStream affix, List<InputStream> dictionaries, boolean ignoreCase) throws IOException, ParseException {
    this.ignoreCase = ignoreCase;
    this.needsInputCleaning = ignoreCase;
    this.needsOutputCleaning = false; // set if we have an OCONV
    flagLookup.add(new BytesRef()); // no flags -> ord 0

    Path aff = Files.createTempFile(tempPath, "affix", "aff");
    OutputStream out = new BufferedOutputStream(Files.newOutputStream(aff));
    InputStream aff1 = null;
    InputStream aff2 = null;
    boolean success = false;
    try {
      // copy contents of affix stream to temp file
      final byte [] buffer = new byte [1024 * 8];
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
      IntSequenceOutputs o = IntSequenceOutputs.getSingleton();
      Builder<IntsRef> b = new Builder<>(FST.INPUT_TYPE.BYTE4, o);
      readDictionaryFiles(tempDir, tempFileNamePrefix, dictionaries, decoder, b);
      words = b.finish();
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

  /**
   * Looks up Hunspell word forms from the dictionary
   */
  IntsRef lookupWord(char word[], int offset, int length) {
    return lookup(words, word, offset, length);
  }

  // only for testing
  IntsRef lookupPrefix(char word[], int offset, int length) {
    return lookup(prefixes, word, offset, length);
  }

  // only for testing
  IntsRef lookupSuffix(char word[], int offset, int length) {
    return lookup(suffixes, word, offset, length);
  }
  
  IntsRef lookup(FST<IntsRef> fst, char word[], int offset, int length) {
    if (fst == null) {
      return null;
    }
    final FST.BytesReader bytesReader = fst.getBytesReader();
    final FST.Arc<IntsRef> arc = fst.getFirstArc(new FST.Arc<IntsRef>());
    // Accumulate output as we go
    final IntsRef NO_OUTPUT = fst.outputs.getNoOutput();
    IntsRef output = NO_OUTPUT;
    
    int l = offset + length;
    try {
      for (int i = offset, cp = 0; i < l; i += Character.charCount(cp)) {
        cp = Character.codePointAt(word, i, l);
        if (fst.findTargetArc(cp, arc, arc, bytesReader) == null) {
          return null;
        } else if (arc.output != NO_OUTPUT) {
          output = fst.outputs.add(output, arc.output);
        }
      }
      if (fst.findTargetArc(FST.END_LABEL, arc, arc, bytesReader) == null) {
        return null;
      } else if (arc.output != NO_OUTPUT) {
        return fst.outputs.add(output, arc.output);
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
  private void readAffixFile(InputStream affixStream, CharsetDecoder decoder) throws IOException, ParseException {
    TreeMap<String, List<Integer>> prefixes = new TreeMap<>();
    TreeMap<String, List<Integer>> suffixes = new TreeMap<>();
    Map<String,Integer> seenPatterns = new HashMap<>();
    
    // zero condition -> 0 ord
    seenPatterns.put(".*", 0);
    patterns.add(null);
    
    // zero strip -> 0 ord
    Map<String,Integer> seenStrips = new LinkedHashMap<>();
    seenStrips.put("", 0);

    LineNumberReader reader = new LineNumberReader(new InputStreamReader(affixStream, decoder));
    String line = null;
    while ((line = reader.readLine()) != null) {
      // ignore any BOM marker on first line
      if (reader.getLineNumber() == 1 && line.startsWith("\uFEFF")) {
        line = line.substring(1);
      }
      if (line.startsWith(ALIAS_KEY)) {
        parseAlias(line);
      } else if (line.startsWith(MORPH_ALIAS_KEY)) {
        parseMorphAlias(line);
      } else if (line.startsWith(PREFIX_KEY)) {
        parseAffix(prefixes, line, reader, PREFIX_CONDITION_REGEX_PATTERN, seenPatterns, seenStrips);
      } else if (line.startsWith(SUFFIX_KEY)) {
        parseAffix(suffixes, line, reader, SUFFIX_CONDITION_REGEX_PATTERN, seenPatterns, seenStrips);
      } else if (line.startsWith(FLAG_KEY)) {
        // Assume that the FLAG line comes before any prefix or suffixes
        // Store the strategy so it can be used when parsing the dic file
        flagParsingStrategy = getFlagParsingStrategy(line);
      } else if (line.equals(COMPLEXPREFIXES_KEY)) {
        complexPrefixes = true; // 2-stage prefix+1-stage suffix instead of 2-stage suffix+1-stage prefix
      } else if (line.startsWith(CIRCUMFIX_KEY)) {
        String parts[] = line.split("\\s+");
        if (parts.length != 2) {
          throw new ParseException("Illegal CIRCUMFIX declaration", reader.getLineNumber());
        }
        circumfix = flagParsingStrategy.parseFlag(parts[1]);
      } else if (line.startsWith(KEEPCASE_KEY)) {
        String parts[] = line.split("\\s+");
        if (parts.length != 2) {
          throw new ParseException("Illegal KEEPCASE declaration", reader.getLineNumber());
        }
        keepcase = flagParsingStrategy.parseFlag(parts[1]);
      } else if (line.startsWith(NEEDAFFIX_KEY) || line.startsWith(PSEUDOROOT_KEY)) {
        String parts[] = line.split("\\s+");
        if (parts.length != 2) {
          throw new ParseException("Illegal NEEDAFFIX declaration", reader.getLineNumber());
        }
        needaffix = flagParsingStrategy.parseFlag(parts[1]);
      } else if (line.startsWith(ONLYINCOMPOUND_KEY)) {
        String parts[] = line.split("\\s+");
        if (parts.length != 2) {
          throw new ParseException("Illegal ONLYINCOMPOUND declaration", reader.getLineNumber());
        }
        onlyincompound = flagParsingStrategy.parseFlag(parts[1]);
      } else if (line.startsWith(IGNORE_KEY)) {
        String parts[] = line.split("\\s+");
        if (parts.length != 2) {
          throw new ParseException("Illegal IGNORE declaration", reader.getLineNumber());
        }
        ignore = parts[1].toCharArray();
        Arrays.sort(ignore);
        needsInputCleaning = true;
      } else if (line.startsWith(ICONV_KEY) || line.startsWith(OCONV_KEY)) {
        String parts[] = line.split("\\s+");
        String type = parts[0];
        if (parts.length != 2) {
          throw new ParseException("Illegal " + type + " declaration", reader.getLineNumber());
        }
        int num = Integer.parseInt(parts[1]);
        FST<CharsRef> res = parseConversions(reader, num);
        if (type.equals("ICONV")) {
          iconv = res;
          needsInputCleaning |= iconv != null;
        } else {
          oconv = res;
          needsOutputCleaning |= oconv != null;
        }
      } else if (line.startsWith(FULLSTRIP_KEY)) {
        fullStrip = true;
      } else if (line.startsWith(LANG_KEY)) {
        language = line.substring(LANG_KEY.length()).trim();
        alternateCasing = "tr_TR".equals(language) || "az_AZ".equals(language);
      }
    }
    
    this.prefixes = affixFST(prefixes);
    this.suffixes = affixFST(suffixes);
    
    int totalChars = 0;
    for (String strip : seenStrips.keySet()) {
      totalChars += strip.length();
    }
    stripData = new char[totalChars];
    stripOffsets = new int[seenStrips.size()+1];
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
  
  private FST<IntsRef> affixFST(TreeMap<String,List<Integer>> affixes) throws IOException {
    IntSequenceOutputs outputs = IntSequenceOutputs.getSingleton();
    Builder<IntsRef> builder = new Builder<>(FST.INPUT_TYPE.BYTE4, outputs);
    IntsRefBuilder scratch = new IntsRefBuilder();
    for (Map.Entry<String,List<Integer>> entry : affixes.entrySet()) {
      Util.toUTF32(entry.getKey(), scratch);
      List<Integer> entries = entry.getValue();
      IntsRef output = new IntsRef(entries.size());
      for (Integer c : entries) {
        output.ints[output.length++] = c;
      }
      builder.add(scratch.get(), output);
    }
    return builder.finish();
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
          escaped.append(re.charAt(i+1));
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
   * @param conditionPattern {@link String#format(String, Object...)} pattern to be used to generate the condition regex
   *                         pattern
   * @param seenPatterns map from condition -&gt; index of patterns, for deduplication.
   * @throws IOException Can be thrown while reading the rule
   */
  private void parseAffix(TreeMap<String,List<Integer>> affixes,
                          String header,
                          LineNumberReader reader,
                          String conditionPattern,
                          Map<String,Integer> seenPatterns,
                          Map<String,Integer> seenStrips) throws IOException, ParseException {
    
    BytesRefBuilder scratch = new BytesRefBuilder();
    StringBuilder sb = new StringBuilder();
    String args[] = header.split("\\s+");

    boolean crossProduct = args[2].equals("Y");
    boolean isSuffix = conditionPattern == SUFFIX_CONDITION_REGEX_PATTERN;
    
    int numLines = Integer.parseInt(args[3]);
    affixData = ArrayUtil.grow(affixData, (currentAffix << 3) + (numLines << 3));
    ByteArrayDataOutput affixWriter = new ByteArrayDataOutput(affixData, currentAffix << 3, numLines << 3);
    
    for (int i = 0; i < numLines; i++) {
      assert affixWriter.getPosition() == currentAffix << 3;
      String line = reader.readLine();
      String ruleArgs[] = line.split("\\s+");

      // from the manpage: PFX flag stripping prefix [condition [morphological_fields...]]
      // condition is optional
      if (ruleArgs.length < 4) {
          throw new ParseException("The affix file contains a rule with less than four elements: " + line, reader.getLineNumber());
      }
      
      char flag = flagParsingStrategy.parseFlag(ruleArgs[1]);
      String strip = ruleArgs[2].equals("0") ? "" : ruleArgs[2];
      String affixArg = ruleArgs[3];
      char appendFlags[] = null;
      
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
          throw new UnsupportedOperationException("Too many patterns, please report this to dev@lucene.apache.org");          
        }
        seenPatterns.put(regex, patternIndex);
        CharacterRunAutomaton pattern = new CharacterRunAutomaton(new RegExp(regex, RegExp.NONE).toAutomaton());
        patterns.add(pattern);
      }
      
      Integer stripOrd = seenStrips.get(strip);
      if (stripOrd == null) {
        stripOrd = seenStrips.size();
        seenStrips.put(strip, stripOrd);
        if (stripOrd > Character.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many unique strips, please report this to dev@lucene.apache.org");
        }
      }

      if (appendFlags == null) {
        appendFlags = NOFLAGS;
      }
      
      encodeFlags(scratch, appendFlags);
      int appendFlagsOrd = flagLookup.add(scratch.get());
      if (appendFlagsOrd < 0) {
        // already exists in our hash
        appendFlagsOrd = (-appendFlagsOrd)-1;
      } else if (appendFlagsOrd > Short.MAX_VALUE) {
        // this limit is probably flexible, but it's a good sanity check too
        throw new UnsupportedOperationException("Too many unique append flags, please report this to dev@lucene.apache.org");
      }
      
      affixWriter.writeShort((short)flag);
      affixWriter.writeShort((short)stripOrd.intValue());
      // encode crossProduct into patternIndex
      int patternOrd = patternIndex.intValue() << 1 | (crossProduct ? 1 : 0);
      affixWriter.writeShort((short)patternOrd);
      affixWriter.writeShort((short)appendFlagsOrd);
      
      if (needsInputCleaning) {
        CharSequence cleaned = cleanInput(affixArg, sb);
        affixArg = cleaned.toString();
      }
      
      if (isSuffix) {
        affixArg = new StringBuilder(affixArg).reverse().toString();
      }
      
      List<Integer> list = affixes.get(affixArg);
      if (list == null) {
        list = new ArrayList<>();
        affixes.put(affixArg, list);
      }
      list.add(currentAffix);
      currentAffix++;
    }
  }
  
  private FST<CharsRef> parseConversions(LineNumberReader reader, int num) throws IOException, ParseException {
    Map<String,String> mappings = new TreeMap<>();
    
    for (int i = 0; i < num; i++) {
      String line = reader.readLine();
      String parts[] = line.split("\\s+");
      if (parts.length != 3) {
        throw new ParseException("invalid syntax: " + line, reader.getLineNumber());
      }
      if (mappings.put(parts[1], parts[2]) != null) {
        throw new IllegalStateException("duplicate mapping specified for: " + parts[1]);
      }
    }
    
    Outputs<CharsRef> outputs = CharSequenceOutputs.getSingleton();
    Builder<CharsRef> builder = new Builder<>(FST.INPUT_TYPE.BYTE2, outputs);
    IntsRefBuilder scratchInts = new IntsRefBuilder();
    for (Map.Entry<String,String> entry : mappings.entrySet()) {
      Util.toUTF16(entry.getKey(), scratchInts);
      builder.add(scratchInts.get(), new CharsRef(entry.getValue()));
    }
    
    return builder.finish();
  }
  
  /** pattern accepts optional BOM + SET + any whitespace */
  final static Pattern ENCODING_PATTERN = Pattern.compile("^(\u00EF\u00BB\u00BF)?SET\\s+");

  /**
   * Parses the encoding specified in the affix file readable through the provided InputStream
   *
   * @param affix InputStream for reading the affix file
   * @return Encoding specified in the affix file
   * @throws IOException Can be thrown while reading from the InputStream
   * @throws ParseException Thrown if the first non-empty non-comment line read from the file does not adhere to the format {@code SET <encoding>}
   */
  static String getDictionaryEncoding(InputStream affix) throws IOException, ParseException {
    final StringBuilder encoding = new StringBuilder();
    for (;;) {
      encoding.setLength(0);
      int ch;
      while ((ch = affix.read()) >= 0) {
        if (ch == '\n') {
          break;
        }
        if (ch != '\r') {
          encoding.append((char)ch);
        }
      }
      if (
          encoding.length() == 0 || encoding.charAt(0) == '#' ||
          // this test only at the end as ineffective but would allow lines only containing spaces:
          encoding.toString().trim().length() == 0
      ) {
        if (ch < 0) {
          throw new ParseException("Unexpected end of affix file.", 0);
        }
        continue;
      }
      Matcher matcher = ENCODING_PATTERN.matcher(encoding);
      if (matcher.find()) {
        int last = matcher.end();
        return encoding.substring(last).trim();
      }
    }
  }

  static final Map<String,String> CHARSET_ALIASES;
  static {
    Map<String,String> m = new HashMap<>();
    m.put("microsoft-cp1251", "windows-1251");
    m.put("TIS620-2533", "TIS-620");
    CHARSET_ALIASES = Collections.unmodifiableMap(m);
  }
  
  /**
   * Retrieves the CharsetDecoder for the given encoding.  Note, This isn't perfect as I think ISCII-DEVANAGARI and
   * MICROSOFT-CP1251 etc are allowed...
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
   * Determines the appropriate {@link FlagParsingStrategy} based on the FLAG definition line taken from the affix file
   *
   * @param flagLine Line containing the flag information
   * @return FlagParsingStrategy that handles parsing flags in the way specified in the FLAG definition
   */
  static FlagParsingStrategy getFlagParsingStrategy(String flagLine) {
    String parts[] = flagLine.split("\\s+");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Illegal FLAG specification: " + flagLine);
    }
    String flagType = parts[1];

    if (NUM_FLAG_TYPE.equals(flagType)) {
      return new NumFlagParsingStrategy();
    } else if (UTF8_FLAG_TYPE.equals(flagType)) {
      return new SimpleFlagParsingStrategy();
    } else if (LONG_FLAG_TYPE.equals(flagType)) {
      return new DoubleASCIIFlagParsingStrategy();
    }

    throw new IllegalArgumentException("Unknown flag type: " + flagType);
  }

  final char FLAG_SEPARATOR = 0x1f; // flag separator after escaping
  final char MORPH_SEPARATOR = 0x1e; // separator for boundary of entry (may be followed by morph data)
  
  String unescapeEntry(String entry) {
    StringBuilder sb = new StringBuilder();
    int end = morphBoundary(entry);
    for (int i = 0; i < end; i++) {
      char ch = entry.charAt(i);
      if (ch == '\\' && i+1 < entry.length()) {
        sb.append(entry.charAt(i+1));
        i++;
      } else if (ch == '/') {
        sb.append(FLAG_SEPARATOR);
      } else if (ch == MORPH_SEPARATOR || ch == FLAG_SEPARATOR) {
        // BINARY EXECUTABLES EMBEDDED IN ZULU DICTIONARIES!!!!!!!
      } else {
        sb.append(ch);
      }
    }
    sb.append(MORPH_SEPARATOR);
    if (end < entry.length()) {
      for (int i = end; i < entry.length(); i++) {
        char c = entry.charAt(i);
        if (c == FLAG_SEPARATOR || c == MORPH_SEPARATOR) {
          // BINARY EXECUTABLES EMBEDDED IN ZULU DICTIONARIES!!!!!!!
        } else {
          sb.append(c);
        }
      }
    }
    return sb.toString();
  }
  
  static int morphBoundary(String line) {
    int end = indexOfSpaceOrTab(line, 0);
    if (end == -1) {
      return line.length();
    }
    while (end >= 0 && end < line.length()) {
      if (line.charAt(end) == '\t' ||
          end+3 < line.length() && 
          Character.isLetter(line.charAt(end+1)) && 
          Character.isLetter(line.charAt(end+2)) &&
          line.charAt(end+3) == ':') {
        break;
      }
      end = indexOfSpaceOrTab(line, end+1);
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

  /**
   * Reads the dictionary file through the provided InputStreams, building up the words map
   *
   * @param dictionaries InputStreams to read the dictionary file through
   * @param decoder CharsetDecoder used to decode the contents of the file
   * @throws IOException Can be thrown while reading from the file
   */
  private void readDictionaryFiles(Directory tempDir, String tempFileNamePrefix, List<InputStream> dictionaries, CharsetDecoder decoder, Builder<IntsRef> words) throws IOException {
    BytesRefBuilder flagsScratch = new BytesRefBuilder();
    IntsRefBuilder scratchInts = new IntsRefBuilder();
    
    StringBuilder sb = new StringBuilder();

    IndexOutput unsorted = tempDir.createTempOutput(tempFileNamePrefix, "dat", IOContext.DEFAULT);
    try (ByteSequencesWriter writer = new ByteSequencesWriter(unsorted)) {
      for (InputStream dictionary : dictionaries) {
        BufferedReader lines = new BufferedReader(new InputStreamReader(dictionary, decoder));
        String line = lines.readLine(); // first line is number of entries (approximately, sometimes)
        
        while ((line = lines.readLine()) != null) {
          // wild and unpredictable code comment rules
          if (line.isEmpty() || line.charAt(0) == '/' || line.charAt(0) == '#' || line.charAt(0) == '\t') {
            continue;
          }
          line = unescapeEntry(line);
          // if we havent seen any stem exceptions, try to parse one
          if (hasStemExceptions == false) {
            int morphStart = line.indexOf(MORPH_SEPARATOR);
            if (morphStart >= 0 && morphStart < line.length()) {
              hasStemExceptions = parseStemException(line.substring(morphStart+1)) != null;
            }
          }
          if (needsInputCleaning) {
            int flagSep = line.indexOf(FLAG_SEPARATOR);
            if (flagSep == -1) {
              flagSep = line.indexOf(MORPH_SEPARATOR);
            }
            if (flagSep == -1) {
              CharSequence cleansed = cleanInput(line, sb);
              writer.write(cleansed.toString().getBytes(StandardCharsets.UTF_8));
            } else {
              String text = line.substring(0, flagSep);
              CharSequence cleansed = cleanInput(text, sb);
              if (cleansed != sb) {
                sb.setLength(0);
                sb.append(cleansed);
              }
              sb.append(line.substring(flagSep));
              writer.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            }
          } else {
            writer.write(line.getBytes(StandardCharsets.UTF_8));
          }
        }
      }
      CodecUtil.writeFooter(unsorted);
    }

    OfflineSorter sorter = new OfflineSorter(tempDir, tempFileNamePrefix, new Comparator<BytesRef>() {
      BytesRef scratch1 = new BytesRef();
      BytesRef scratch2 = new BytesRef();
      
      @Override
      public int compare(BytesRef o1, BytesRef o2) {
        scratch1.bytes = o1.bytes;
        scratch1.offset = o1.offset;
        scratch1.length = o1.length;
        
        for (int i = scratch1.length - 1; i >= 0; i--) {
          if (scratch1.bytes[scratch1.offset + i] == FLAG_SEPARATOR || scratch1.bytes[scratch1.offset + i] == MORPH_SEPARATOR) {
            scratch1.length = i;
            break;
          }
        }
        
        scratch2.bytes = o2.bytes;
        scratch2.offset = o2.offset;
        scratch2.length = o2.length;
        
        for (int i = scratch2.length - 1; i >= 0; i--) {
          if (scratch2.bytes[scratch2.offset + i] == FLAG_SEPARATOR || scratch2.bytes[scratch2.offset + i] == MORPH_SEPARATOR) {
            scratch2.length = i;
            break;
          }
        }
        
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
    
    boolean success2 = false;
    
    try (ByteSequencesReader reader = new ByteSequencesReader(tempDir.openChecksumInput(sorted, IOContext.READONCE), sorted)) {
    
      // TODO: the flags themselves can be double-chars (long) or also numeric
      // either way the trick is to encode them as char... but they must be parsed differently
    
      String currentEntry = null;
      IntsRefBuilder currentOrds = new IntsRefBuilder();

      while (true) {
        BytesRef scratch = reader.next();
        if (scratch == null) {
          break;
        }
        
        String line = scratch.utf8ToString();
        String entry;
        char wordForm[];
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
        if (hasStemExceptions && end+1 < line.length()) {
          String stemException = parseStemException(line.substring(end+1));
          if (stemException != null) {
            if (stemExceptionCount == stemExceptions.length) {
              int newSize = ArrayUtil.oversize(stemExceptionCount+1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
              stemExceptions = Arrays.copyOf(stemExceptions, newSize);
            }
            stemExceptionID = stemExceptionCount+1; // we use '0' to indicate no exception for the form
            stemExceptions[stemExceptionCount++] = stemException;
          }
        }

        int cmp = currentEntry == null ? 1 : entry.compareTo(currentEntry);
        if (cmp < 0) {
          throw new IllegalArgumentException("out of order: " + entry + " < " + currentEntry);
        } else {
          encodeFlags(flagsScratch, wordForm);
          int ord = flagLookup.add(flagsScratch.get());
          if (ord < 0) {
            // already exists in our hash
            ord = (-ord)-1;
          }
          // finalize current entry, and switch "current" if necessary
          if (cmp > 0 && currentEntry != null) {
            Util.toUTF32(currentEntry, scratchInts);
            words.add(scratchInts.get(), currentOrds.get());
          }
          // swap current
          if (cmp > 0 || currentEntry == null) {
            currentEntry = entry;
            currentOrds = new IntsRefBuilder(); // must be this way
          }
          if (hasStemExceptions) {
            currentOrds.append(ord);
            currentOrds.append(stemExceptionID);
          } else {
            currentOrds.append(ord);
          }
        }
      }
    
      // finalize last entry
      Util.toUTF32(currentEntry, scratchInts);
      words.add(scratchInts.get(), currentOrds.get());
      success2 = true;
    } finally {
      if (success2) {
        tempDir.deleteFile(sorted);
      } else {
        IOUtils.deleteFilesIgnoringExceptions(tempDir, sorted);
      }
    }
  }
  
  static char[] decodeFlags(BytesRef b) {
    if (b.length == 0) {
      return CharsRef.EMPTY_CHARS;
    }
    int len = b.length >>> 1;
    char flags[] = new char[len];
    int upto = 0;
    int end = b.offset + b.length;
    for (int i = b.offset; i < end; i += 2) {
      flags[upto++] = (char)((b.bytes[i] << 8) | (b.bytes[i+1] & 0xff));
    }
    return flags;
  }
  
  static void encodeFlags(BytesRefBuilder b, char flags[]) {
    int len = flags.length << 1;
    b.grow(len);
    b.clear();
    for (int i = 0; i < flags.length; i++) {
      int flag = flags[i];
      b.append((byte) ((flag >> 8) & 0xff));
      b.append((byte) (flag & 0xff));
    }
  }

  private void parseAlias(String line) {
    String ruleArgs[] = line.split("\\s+");
    if (aliases == null) {
      //first line should be the aliases count
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
    return stemExceptions[id-1];
  }
  
  private void parseMorphAlias(String line) {
    if (morphAliases == null) {
      //first line should be the aliases count
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
        morphData = morphAliases[alias-1];
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
      int endIndex = indexOfSpaceOrTab(morphData, index+1);
      if (endIndex < 0) {
        endIndex = morphData.length();
      }
      return morphData.substring(index+4, endIndex);
    }
    return null;
  }

  /**
   * Abstraction of the process of parsing flags taken from the affix and dic files
   */
  static abstract class FlagParsingStrategy {

    /**
     * Parses the given String into a single flag
     *
     * @param rawFlag String to parse into a flag
     * @return Parsed flag
     */
    char parseFlag(String rawFlag) {
      char flags[] = parseFlags(rawFlag);
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
  }

  /**
   * Simple implementation of {@link FlagParsingStrategy} that treats the chars in each String as a individual flags.
   * Can be used with both the ASCII and UTF-8 flag types.
   */
  private static class SimpleFlagParsingStrategy extends FlagParsingStrategy {
    @Override
    public char[] parseFlags(String rawFlags) {
      return rawFlags.toCharArray();
    }
  }

  /**
   * Implementation of {@link FlagParsingStrategy} that assumes each flag is encoded in its numerical form.  In the case
   * of multiple flags, each number is separated by a comma.
   */
  private static class NumFlagParsingStrategy extends FlagParsingStrategy {
    @Override
    public char[] parseFlags(String rawFlags) {
      String[] rawFlagParts = rawFlags.trim().split(",");
      char[] flags = new char[rawFlagParts.length];
      int upto = 0;
      
      for (int i = 0; i < rawFlagParts.length; i++) {
        // note, removing the trailing X/leading I for nepali... what is the rule here?! 
        String replacement = rawFlagParts[i].replaceAll("[^0-9]", "");
        // note, ignoring empty flags (this happens in danish, for example)
        if (replacement.isEmpty()) {
          continue;
        }
        flags[upto++] = (char) Integer.parseInt(replacement);
      }

      if (upto < flags.length) {
        flags = Arrays.copyOf(flags, upto);
      }
      return flags;
    }
  }

  /**
   * Implementation of {@link FlagParsingStrategy} that assumes each flag is encoded as two ASCII characters whose codes
   * must be combined into a single character.
   */
  private static class DoubleASCIIFlagParsingStrategy extends FlagParsingStrategy {

    @Override
    public char[] parseFlags(String rawFlags) {
      if (rawFlags.length() == 0) {
        return new char[0];
      }

      StringBuilder builder = new StringBuilder();
      if (rawFlags.length() % 2 == 1) {
        throw new IllegalArgumentException("Invalid flags (should be even number of characters): " + rawFlags);
      }
      for (int i = 0; i < rawFlags.length(); i+=2) {
        char f1 = rawFlags.charAt(i);
        char f2 = rawFlags.charAt(i+1);
        if (f1 >= 256 || f2 >= 256) {
          throw new IllegalArgumentException("Invalid flags (LONG flags must be double ASCII): " + rawFlags);
        }
        char combined = (char) (f1 << 8 | f2);
        builder.append(combined);
      }
      
      char flags[] = new char[builder.length()];
      builder.getChars(0, builder.length(), flags, 0);
      return flags;
    }
  }
  
  static boolean hasFlag(char flags[], char flag) {
    return Arrays.binarySearch(flags, flag) >= 0;
  }
  
  CharSequence cleanInput(CharSequence input, StringBuilder reuse) {
    reuse.setLength(0);
    
    for (int i = 0; i < input.length(); i++) {
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
    final FST.Arc<CharsRef> firstArc = fst.getFirstArc(new FST.Arc<CharsRef>());
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
          output = fst.outputs.add(output, arc.output);
        }
        if (arc.isFinal()) {
          longestOutput = fst.outputs.add(output, arc.nextFinalOutput);
          longestMatch = j;
        }
      }
      
      if (longestMatch >= 0) {
        sb.delete(i, longestMatch+1);
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
  public static void setDefaultTempDir(Path tempDir) {
    DEFAULT_TEMP_DIR = tempDir;
  }

  /**
   * Returns the default temporary directory. By default, java.io.tmpdir. If not accessible
   * or not available, an IOException is thrown
   */
  synchronized static Path getDefaultTempDir() throws IOException {
    if (DEFAULT_TEMP_DIR == null) {
      // Lazy init
      String tempDirPath = System.getProperty("java.io.tmpdir");
      if (tempDirPath == null)  {
        throw new IOException("Java has no temporary folder property (java.io.tmpdir)?");
      }
      Path tempDirectory = Paths.get(tempDirPath);
      if (Files.isWritable(tempDirectory) == false) {
        throw new IOException("Java's temporary folder not present or writeable?: " 
                              + tempDirectory.toAbsolutePath());
      }
      DEFAULT_TEMP_DIR = tempDirectory;
    }

    return DEFAULT_TEMP_DIR;
  }
}
