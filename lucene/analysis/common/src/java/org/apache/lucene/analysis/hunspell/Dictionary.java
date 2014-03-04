package org.apache.lucene.analysis.hunspell;

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

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.OfflineSorter.ByteSequencesReader;
import org.apache.lucene.util.OfflineSorter.ByteSequencesWriter;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.CharSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.IntSequenceOutputs;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.Util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * In-memory structure for the dictionary (.dic) and affix (.aff)
 * data of a hunspell dictionary.
 */
public class Dictionary {

  static final char[] NOFLAGS = new char[0];
  
  private static final String ALIAS_KEY = "AF";
  private static final String PREFIX_KEY = "PFX";
  private static final String SUFFIX_KEY = "SFX";
  private static final String FLAG_KEY = "FLAG";
  private static final String COMPLEXPREFIXES_KEY = "COMPLEXPREFIXES";
  private static final String CIRCUMFIX_KEY = "CIRCUMFIX";
  private static final String IGNORE_KEY = "IGNORE";
  private static final String ICONV_KEY = "ICONV";
  private static final String OCONV_KEY = "OCONV";

  private static final String NUM_FLAG_TYPE = "num";
  private static final String UTF8_FLAG_TYPE = "UTF-8";
  private static final String LONG_FLAG_TYPE = "long";
  
  private static final String PREFIX_CONDITION_REGEX_PATTERN = "%s.*";
  private static final String SUFFIX_CONDITION_REGEX_PATTERN = ".*%s";

  FST<IntsRef> prefixes;
  FST<IntsRef> suffixes;
  
  // all Patterns used by prefixes and suffixes. these are typically re-used across
  // many affix stripping rules. so these are deduplicated, to save RAM.
  // TODO: maybe don't use Pattern for the condition check...
  // TODO: when we cut over Affix to FST, just store integer index to this.
  ArrayList<Pattern> patterns = new ArrayList<Pattern>();
  
  // the entries in the .dic file, mapping to their set of flags.
  // the fst output is the ordinal list for flagLookup
  FST<IntsRef> words;
  // the list of unique flagsets (wordforms). theoretically huge, but practically
  // small (e.g. for polish this is 756), otherwise humans wouldn't be able to deal with it either.
  BytesRefHash flagLookup = new BytesRefHash();
  
  // the list of unique strip affixes.
  BytesRefHash stripLookup = new BytesRefHash();
  
  // 8 bytes per affix
  byte[] affixData = new byte[64];
  private int currentAffix = 0;

  private FlagParsingStrategy flagParsingStrategy = new SimpleFlagParsingStrategy(); // Default flag parsing strategy

  private String[] aliases;
  private int aliasCount = 0;
  
  private final File tempDir = OfflineSorter.defaultTempDir(); // TODO: make this configurable?
  
  boolean ignoreCase;
  boolean complexPrefixes;
  
  int circumfix = -1; // circumfix flag, or -1 if one is not defined
  
  // ignored characters (dictionary, affix, inputs)
  private char[] ignore;
  
  // FSTs used for ICONV/OCONV, output ord pointing to replacement text
  FST<CharsRef> iconv;
  FST<CharsRef> oconv;
  
  boolean needsInputCleaning;
  boolean needsOutputCleaning;
  
  /**
   * Creates a new Dictionary containing the information read from the provided InputStreams to hunspell affix
   * and dictionary files.
   * You have to close the provided InputStreams yourself.
   *
   * @param affix InputStream for reading the hunspell affix file (won't be closed).
   * @param dictionary InputStream for reading the hunspell dictionary file (won't be closed).
   * @throws IOException Can be thrown while reading from the InputStreams
   * @throws ParseException Can be thrown if the content of the files does not meet expected formats
   */
  public Dictionary(InputStream affix, InputStream dictionary) throws IOException, ParseException {
    this(affix, Collections.singletonList(dictionary), false);
  }

  /**
   * Creates a new Dictionary containing the information read from the provided InputStreams to hunspell affix
   * and dictionary files.
   * You have to close the provided InputStreams yourself.
   *
   * @param affix InputStream for reading the hunspell affix file (won't be closed).
   * @param dictionaries InputStream for reading the hunspell dictionary files (won't be closed).
   * @throws IOException Can be thrown while reading from the InputStreams
   * @throws ParseException Can be thrown if the content of the files does not meet expected formats
   */
  public Dictionary(InputStream affix, List<InputStream> dictionaries, boolean ignoreCase) throws IOException, ParseException {
    this.ignoreCase = ignoreCase;
    this.needsInputCleaning = ignoreCase;
    this.needsOutputCleaning = false; // set if we have an OCONV
    // hungarian has thousands of AF before the SET, so a 32k buffer is needed 
    BufferedInputStream buffered = new BufferedInputStream(affix, 32768);
    buffered.mark(32768);
    String encoding = getDictionaryEncoding(buffered);
    buffered.reset();
    CharsetDecoder decoder = getJavaEncoding(encoding);
    readAffixFile(buffered, decoder);
    flagLookup.add(new BytesRef()); // no flags -> ord 0
    stripLookup.add(new BytesRef()); // no strip -> ord 0
    IntSequenceOutputs o = IntSequenceOutputs.getSingleton();
    Builder<IntsRef> b = new Builder<IntsRef>(FST.INPUT_TYPE.BYTE4, o);
    readDictionaryFiles(dictionaries, decoder, b);
    words = b.finish();
  }

  /**
   * Looks up Hunspell word forms from the dictionary
   */
  IntsRef lookupWord(char word[], int offset, int length) {
    return lookup(words, word, offset, length);
  }

  /**
   * Looks up HunspellAffix prefixes that have an append that matches the String created from the given char array, offset and length
   *
   * @param word Char array to generate the String from
   * @param offset Offset in the char array that the String starts at
   * @param length Length from the offset that the String is
   * @return List of HunspellAffix prefixes with an append that matches the String, or {@code null} if none are found
   */
  IntsRef lookupPrefix(char word[], int offset, int length) {
    return lookup(prefixes, word, offset, length);
  }

  /**
   * Looks up HunspellAffix suffixes that have an append that matches the String created from the given char array, offset and length
   *
   * @param word Char array to generate the String from
   * @param offset Offset in the char array that the String starts at
   * @param length Length from the offset that the String is
   * @return List of HunspellAffix suffixes with an append that matches the String, or {@code null} if none are found
   */
  IntsRef lookupSuffix(char word[], int offset, int length) {
    return lookup(suffixes, word, offset, length);
  }
  
  // TODO: this is pretty stupid, considering how the stemming algorithm works
  // we can speed it up to be significantly faster!
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
    TreeMap<String, List<Character>> prefixes = new TreeMap<String, List<Character>>();
    TreeMap<String, List<Character>> suffixes = new TreeMap<String, List<Character>>();
    Map<String,Integer> seenPatterns = new HashMap<String,Integer>();

    LineNumberReader reader = new LineNumberReader(new InputStreamReader(affixStream, decoder));
    String line = null;
    while ((line = reader.readLine()) != null) {
      if (line.startsWith(ALIAS_KEY)) {
        parseAlias(line);
      } else if (line.startsWith(PREFIX_KEY)) {
        parseAffix(prefixes, line, reader, PREFIX_CONDITION_REGEX_PATTERN, seenPatterns);
      } else if (line.startsWith(SUFFIX_KEY)) {
        parseAffix(suffixes, line, reader, SUFFIX_CONDITION_REGEX_PATTERN, seenPatterns);
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
      }
    }
    
    this.prefixes = affixFST(prefixes);
    this.suffixes = affixFST(suffixes);
  }
  
  private FST<IntsRef> affixFST(TreeMap<String,List<Character>> affixes) throws IOException {
    IntSequenceOutputs outputs = IntSequenceOutputs.getSingleton();
    Builder<IntsRef> builder = new Builder<IntsRef>(FST.INPUT_TYPE.BYTE4, outputs);
    
    IntsRef scratch = new IntsRef();
    for (Map.Entry<String,List<Character>> entry : affixes.entrySet()) {
      Util.toUTF32(entry.getKey(), scratch);
      List<Character> entries = entry.getValue();
      IntsRef output = new IntsRef(entries.size());
      for (Character c : entries) {
        output.ints[output.length++] = c;
      }
      builder.add(scratch, output);
    }
    return builder.finish();
  }

  /**
   * Parses a specific affix rule putting the result into the provided affix map
   * 
   * @param affixes Map where the result of the parsing will be put
   * @param header Header line of the affix rule
   * @param reader BufferedReader to read the content of the rule from
   * @param conditionPattern {@link String#format(String, Object...)} pattern to be used to generate the condition regex
   *                         pattern
   * @param seenPatterns map from condition -> index of patterns, for deduplication.
   * @throws IOException Can be thrown while reading the rule
   */
  private void parseAffix(TreeMap<String,List<Character>> affixes,
                          String header,
                          LineNumberReader reader,
                          String conditionPattern,
                          Map<String,Integer> seenPatterns) throws IOException, ParseException {
    
    BytesRef scratch = new BytesRef();
    StringBuilder sb = new StringBuilder();
    String args[] = header.split("\\s+");

    boolean crossProduct = args[2].equals("Y");
    
    int numLines = Integer.parseInt(args[3]);
    affixData = ArrayUtil.grow(affixData, (currentAffix << 3) + (numLines << 3));
    ByteArrayDataOutput affixWriter = new ByteArrayDataOutput(affixData, currentAffix << 3, numLines << 3);
    
    for (int i = 0; i < numLines; i++) {
      assert affixWriter.getPosition() == currentAffix << 3;
      String line = reader.readLine();
      String ruleArgs[] = line.split("\\s+");

      if (ruleArgs.length < 5) {
          throw new ParseException("The affix file contains a rule with less than five elements", reader.getLineNumber());
      }
      
      char flag = flagParsingStrategy.parseFlag(ruleArgs[1]);
      String strip = ruleArgs[2].equals("0") ? "" : ruleArgs[2];
      String affixArg = ruleArgs[3];
      char appendFlags[] = null;
      
      int flagSep = affixArg.lastIndexOf('/');
      if (flagSep != -1) {
        String flagPart = affixArg.substring(flagSep + 1);
        affixArg = affixArg.substring(0, flagSep);

        if (aliasCount > 0) {
          flagPart = getAliasValue(Integer.parseInt(flagPart));
        } 
        
        appendFlags = flagParsingStrategy.parseFlags(flagPart);
        Arrays.sort(appendFlags);
      }

      String condition = ruleArgs[4];
      // at least the gascon affix file has this issue
      if (condition.startsWith("[") && !condition.endsWith("]")) {
        condition = condition + "]";
      }
      // "dash hasn't got special meaning" (we must escape it)
      if (condition.indexOf('-') >= 0) {
        condition = condition.replace("-", "\\-");
      }

      String regex = String.format(Locale.ROOT, conditionPattern, condition);
      
      // deduplicate patterns
      Integer patternIndex = seenPatterns.get(regex);
      if (patternIndex == null) {
        patternIndex = patterns.size();
        if (patternIndex > Short.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many patterns, please report this to dev@lucene.apache.org");          
        }
        seenPatterns.put(regex, patternIndex);
        Pattern pattern = Pattern.compile(regex);
        patterns.add(pattern);
      }
      
      scratch.copyChars(strip);
      int stripOrd = stripLookup.add(scratch);
      if (stripOrd < 0) {
        // already exists in our hash
        stripOrd = (-stripOrd)-1;
      } else if (stripOrd > Character.MAX_VALUE) {
        throw new UnsupportedOperationException("Too many unique strips, please report this to dev@lucene.apache.org");
      }

      if (appendFlags == null) {
        appendFlags = NOFLAGS;
      }
      
      final int hashCode = encodeFlagsWithHash(scratch, appendFlags);
      int appendFlagsOrd = flagLookup.add(scratch, hashCode);
      if (appendFlagsOrd < 0) {
        // already exists in our hash
        appendFlagsOrd = (-appendFlagsOrd)-1;
      } else if (appendFlagsOrd > Short.MAX_VALUE) {
        // this limit is probably flexible, but its a good sanity check too
        throw new UnsupportedOperationException("Too many unique append flags, please report this to dev@lucene.apache.org");
      }
      
      affixWriter.writeShort((short)flag);
      affixWriter.writeShort((short)stripOrd);
      // encode crossProduct into patternIndex
      int patternOrd = patternIndex.intValue() << 1 | (crossProduct ? 1 : 0);
      affixWriter.writeShort((short)patternOrd);
      affixWriter.writeShort((short)appendFlagsOrd);
      
      if (needsInputCleaning) {
        CharSequence cleaned = cleanInput(affixArg, sb);
        affixArg = cleaned.toString();
      }
      
      List<Character> list = affixes.get(affixArg);
      if (list == null) {
        list = new ArrayList<Character>();
        affixes.put(affixArg, list);
      }
      
      list.add((char)currentAffix);
      currentAffix++;
    }
  }
  
  private FST<CharsRef> parseConversions(LineNumberReader reader, int num) throws IOException, ParseException {
    Map<String,String> mappings = new TreeMap<String,String>();
    
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
    Builder<CharsRef> builder = new Builder<CharsRef>(FST.INPUT_TYPE.BYTE2, outputs);
    IntsRef scratchInts = new IntsRef();
    for (Map.Entry<String,String> entry : mappings.entrySet()) {
      Util.toUTF16(entry.getKey(), scratchInts);
      builder.add(scratchInts, new CharsRef(entry.getValue()));
    }
    
    return builder.finish();
  }

  /**
   * Parses the encoding specified in the affix file readable through the provided InputStream
   *
   * @param affix InputStream for reading the affix file
   * @return Encoding specified in the affix file
   * @throws IOException Can be thrown while reading from the InputStream
   * @throws ParseException Thrown if the first non-empty non-comment line read from the file does not adhere to the format {@code SET <encoding>}
   */
  private String getDictionaryEncoding(InputStream affix) throws IOException, ParseException {
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
      if (encoding.length() > 4 && "SET ".equals(encoding.substring(0, 4))) {
        // cleanup the encoding string, too (whitespace)
        return encoding.substring(4).trim();
      }
    }
  }

  static final Map<String,String> CHARSET_ALIASES;
  static {
    Map<String,String> m = new HashMap<String,String>();
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
  private FlagParsingStrategy getFlagParsingStrategy(String flagLine) {
    String flagType = flagLine.substring(5);

    if (NUM_FLAG_TYPE.equals(flagType)) {
      return new NumFlagParsingStrategy();
    } else if (UTF8_FLAG_TYPE.equals(flagType)) {
      return new SimpleFlagParsingStrategy();
    } else if (LONG_FLAG_TYPE.equals(flagType)) {
      return new DoubleASCIIFlagParsingStrategy();
    }

    throw new IllegalArgumentException("Unknown flag type: " + flagType);
  }

  /**
   * Reads the dictionary file through the provided InputStreams, building up the words map
   *
   * @param dictionaries InputStreams to read the dictionary file through
   * @param decoder CharsetDecoder used to decode the contents of the file
   * @throws IOException Can be thrown while reading from the file
   */
  private void readDictionaryFiles(List<InputStream> dictionaries, CharsetDecoder decoder, Builder<IntsRef> words) throws IOException {
    BytesRef flagsScratch = new BytesRef();
    IntsRef scratchInts = new IntsRef();
    
    StringBuilder sb = new StringBuilder();
    
    File unsorted = File.createTempFile("unsorted", "dat", tempDir);
    ByteSequencesWriter writer = new ByteSequencesWriter(unsorted);
    boolean success = false;
    try {
      for (InputStream dictionary : dictionaries) {
        BufferedReader lines = new BufferedReader(new InputStreamReader(dictionary, decoder));
        String line = lines.readLine(); // first line is number of entries (approximately, sometimes)
        
        while ((line = lines.readLine()) != null) {
          if (needsInputCleaning) {
            int flagSep = line.lastIndexOf('/');
            if (flagSep == -1) {
              CharSequence cleansed = cleanInput(line, sb);
              writer.write(cleansed.toString().getBytes(IOUtils.CHARSET_UTF_8));
            } else {
              String text = line.substring(0, flagSep);
              CharSequence cleansed = cleanInput(text, sb);
              if (cleansed != sb) {
                sb.setLength(0);
                sb.append(cleansed);
              }
              sb.append(line.substring(flagSep));
              writer.write(sb.toString().getBytes(IOUtils.CHARSET_UTF_8));
            }
          } else {
            writer.write(line.getBytes(IOUtils.CHARSET_UTF_8));
          }
        }
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(writer);
      } else {
        IOUtils.closeWhileHandlingException(writer);
      }
    }
    File sorted = File.createTempFile("sorted", "dat", tempDir);
    
    OfflineSorter sorter = new OfflineSorter(new Comparator<BytesRef>() {
      BytesRef scratch1 = new BytesRef();
      BytesRef scratch2 = new BytesRef();
      
      @Override
      public int compare(BytesRef o1, BytesRef o2) {
        scratch1.bytes = o1.bytes;
        scratch1.offset = o1.offset;
        scratch1.length = o1.length;
        
        for (int i = scratch1.length - 1; i >= 0; i--) {
          if (scratch1.bytes[scratch1.offset + i] == '/') {
            scratch1.length = i;
            break;
          }
        }
        
        scratch2.bytes = o2.bytes;
        scratch2.offset = o2.offset;
        scratch2.length = o2.length;
        
        for (int i = scratch2.length - 1; i >= 0; i--) {
          if (scratch2.bytes[scratch2.offset + i] == '/') {
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
    sorter.sort(unsorted, sorted);
    unsorted.delete();
    
    ByteSequencesReader reader = new ByteSequencesReader(sorted);
    BytesRef scratchLine = new BytesRef();
    
    // TODO: the flags themselves can be double-chars (long) or also numeric
    // either way the trick is to encode them as char... but they must be parsed differently
    
    String currentEntry = null;
    IntsRef currentOrds = new IntsRef();
    
    String line;
    while (reader.read(scratchLine)) {
      line = scratchLine.utf8ToString();
      String entry;
      char wordForm[];
      
      int flagSep = line.lastIndexOf('/');
      if (flagSep == -1) {
        wordForm = NOFLAGS;
        entry = line;
      } else {
        // note, there can be comments (morph description) after a flag.
        // we should really look for any whitespace: currently just tab and space
        int end = line.indexOf('\t', flagSep);
        if (end == -1)
          end = line.length();
        int end2 = line.indexOf(' ', flagSep);
        if (end2 == -1)
          end2 = line.length();
        end = Math.min(end, end2);
        
        String flagPart = line.substring(flagSep + 1, end);
        if (aliasCount > 0) {
          flagPart = getAliasValue(Integer.parseInt(flagPart));
        } 
        
        wordForm = flagParsingStrategy.parseFlags(flagPart);
        Arrays.sort(wordForm);
        entry = line.substring(0, flagSep);
      }

      int cmp = currentEntry == null ? 1 : entry.compareTo(currentEntry);
      if (cmp < 0) {
        throw new IllegalArgumentException("out of order: " + entry + " < " + currentEntry);
      } else {
        final int hashCode = encodeFlagsWithHash(flagsScratch, wordForm);
        int ord = flagLookup.add(flagsScratch, hashCode);
        if (ord < 0) {
          // already exists in our hash
          ord = (-ord)-1;
        }
        // finalize current entry, and switch "current" if necessary
        if (cmp > 0 && currentEntry != null) {
          Util.toUTF32(currentEntry, scratchInts);
          words.add(scratchInts, currentOrds);
        }
        // swap current
        if (cmp > 0 || currentEntry == null) {
          currentEntry = entry;
          currentOrds = new IntsRef(); // must be this way
        }
        currentOrds.grow(currentOrds.length+1);
        currentOrds.ints[currentOrds.length++] = ord;
      }
    }
    
    // finalize last entry
    Util.toUTF32(currentEntry, scratchInts);
    words.add(scratchInts, currentOrds);
    
    reader.close();
    sorted.delete();
  }
  
  static char[] decodeFlags(BytesRef b) {
    int len = b.length >>> 1;
    char flags[] = new char[len];
    int upto = 0;
    int end = b.offset + b.length;
    for (int i = b.offset; i < end; i += 2) {
      flags[upto++] = (char)((b.bytes[i] << 8) | (b.bytes[i+1] & 0xff));
    }
    return flags;
  }
  
  static int encodeFlagsWithHash(BytesRef b, char flags[]) {
    int hash = 0;
    int len = flags.length << 1;
    b.grow(len);
    b.length = len;
    int upto = b.offset;
    for (int i = 0; i < flags.length; i++) {
      int flag = flags[i];
      hash = 31*hash + (b.bytes[upto++] = (byte) ((flag >> 8) & 0xff));
      hash = 31*hash + (b.bytes[upto++] = (byte) (flag & 0xff));
    }
    return hash;
  }

  private void parseAlias(String line) {
    String ruleArgs[] = line.split("\\s+");
    if (aliases == null) {
      //first line should be the aliases count
      final int count = Integer.parseInt(ruleArgs[1]);
      aliases = new String[count];
    } else {
      aliases[aliasCount++] = ruleArgs[1];
    }
  }
  
  private String getAliasValue(int id) {
    try {
      return aliases[id - 1];
    } catch (IndexOutOfBoundsException ex) {
      throw new IllegalArgumentException("Bad flag alias number:" + id, ex);
    }
  }

  /**
   * Abstraction of the process of parsing flags taken from the affix and dic files
   */
  private static abstract class FlagParsingStrategy {

    /**
     * Parses the given String into a single flag
     *
     * @param rawFlag String to parse into a flag
     * @return Parsed flag
     */
    char parseFlag(String rawFlag) {
      return parseFlags(rawFlag)[0];
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
   *
   * TODO (rmuir) test
   */
  private static class DoubleASCIIFlagParsingStrategy extends FlagParsingStrategy {

    @Override
    public char[] parseFlags(String rawFlags) {
      if (rawFlags.length() == 0) {
        return new char[0];
      }

      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < rawFlags.length(); i+=2) {
        char cookedFlag = (char) ((int) rawFlags.charAt(i) + (int) rawFlags.charAt(i + 1));
        builder.append(cookedFlag);
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
        ch = Character.toLowerCase(ch);
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
          reuse.setCharAt(i, Character.toLowerCase(reuse.charAt(i)));
        }
      }
    }
    
    return reuse;
  }
  
  // TODO: this could be more efficient!
  static void applyMappings(FST<CharsRef> fst, StringBuilder sb) throws IOException {
    final FST.BytesReader bytesReader = fst.getBytesReader();
    final FST.Arc<CharsRef> firstArc = fst.getFirstArc(new FST.Arc<CharsRef>());
    final CharsRef NO_OUTPUT = fst.outputs.getNoOutput();
    
    // temporary stuff
    final FST.Arc<CharsRef> arc = new FST.Arc<CharsRef>();
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
}
