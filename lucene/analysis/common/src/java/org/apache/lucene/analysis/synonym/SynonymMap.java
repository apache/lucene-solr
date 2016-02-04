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
package org.apache.lucene.analysis.synonym;


import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

/**
 * A map of synonyms, keys and values are phrases.
 * @lucene.experimental
 */
public class SynonymMap {
  /** for multiword support, you must separate words with this separator */
  public static final char WORD_SEPARATOR = 0;
  /** map&lt;input word, list&lt;ord&gt;&gt; */
  public final FST<BytesRef> fst;
  /** map&lt;ord, outputword&gt; */
  public final BytesRefHash words;
  /** maxHorizontalContext: maximum context we need on the tokenstream */
  public final int maxHorizontalContext;

  public SynonymMap(FST<BytesRef> fst, BytesRefHash words, int maxHorizontalContext) {
    this.fst = fst;
    this.words = words;
    this.maxHorizontalContext = maxHorizontalContext;
  }
  
  /**
   * Builds an FSTSynonymMap.
   * <p>
   * Call add() until you have added all the mappings, then call build() to get an FSTSynonymMap
   * @lucene.experimental
   */
  public static class Builder {
    private final HashMap<CharsRef,MapEntry> workingSet = new HashMap<>();
    private final BytesRefHash words = new BytesRefHash();
    private final BytesRefBuilder utf8Scratch = new BytesRefBuilder();
    private int maxHorizontalContext;
    private final boolean dedup;

    /** If dedup is true then identical rules (same input,
     *  same output) will be added only once. */
    public Builder(boolean dedup) {
      this.dedup = dedup;
    }

    private static class MapEntry {
      boolean includeOrig;
      // we could sort for better sharing ultimately, but it could confuse people
      ArrayList<Integer> ords = new ArrayList<>();
    }

    /** Sugar: just joins the provided terms with {@link
     *  SynonymMap#WORD_SEPARATOR}.  reuse and its chars
     *  must not be null. */
    public static CharsRef join(String[] words, CharsRefBuilder reuse) {
      int upto = 0;
      char[] buffer = reuse.chars();
      for (String word : words) {
        final int wordLen = word.length();
        final int needed = (0 == upto ? wordLen : 1 + upto + wordLen); // Add 1 for WORD_SEPARATOR
        if (needed > buffer.length) {
          reuse.grow(needed);
          buffer = reuse.chars();
        }
        if (upto > 0) {
          buffer[upto++] = SynonymMap.WORD_SEPARATOR;
        }

        word.getChars(0, wordLen, buffer, upto);
        upto += wordLen;
      }
      reuse.setLength(upto);
      return reuse.get();
    }
    


    /** only used for asserting! */
    private boolean hasHoles(CharsRef chars) {
      final int end = chars.offset + chars.length;
      for(int idx=chars.offset+1;idx<end;idx++) {
        if (chars.chars[idx] == SynonymMap.WORD_SEPARATOR && chars.chars[idx-1] == SynonymMap.WORD_SEPARATOR) {
          return true;
        }
      }
      if (chars.chars[chars.offset] == '\u0000') {
        return true;
      }
      if (chars.chars[chars.offset + chars.length - 1] == '\u0000') {
        return true;
      }

      return false;
    }

    // NOTE: while it's tempting to make this public, since
    // caller's parser likely knows the
    // numInput/numOutputWords, sneaky exceptions, much later
    // on, will result if these values are wrong; so we always
    // recompute ourselves to be safe:
    private void add(CharsRef input, int numInputWords, CharsRef output, int numOutputWords, boolean includeOrig) {
      // first convert to UTF-8
      if (numInputWords <= 0) {
        throw new IllegalArgumentException("numInputWords must be > 0 (got " + numInputWords + ")");
      }
      if (input.length <= 0) {
        throw new IllegalArgumentException("input.length must be > 0 (got " + input.length + ")");
      }
      if (numOutputWords <= 0) {
        throw new IllegalArgumentException("numOutputWords must be > 0 (got " + numOutputWords + ")");
      }
      if (output.length <= 0) {
        throw new IllegalArgumentException("output.length must be > 0 (got " + output.length + ")");
      }

      assert !hasHoles(input): "input has holes: " + input;
      assert !hasHoles(output): "output has holes: " + output;

      //System.out.println("fmap.add input=" + input + " numInputWords=" + numInputWords + " output=" + output + " numOutputWords=" + numOutputWords);
      utf8Scratch.copyChars(output.chars, output.offset, output.length);
      // lookup in hash
      int ord = words.add(utf8Scratch.get());
      if (ord < 0) {
        // already exists in our hash
        ord = (-ord)-1;
        //System.out.println("  output=" + output + " old ord=" + ord);
      } else {
        //System.out.println("  output=" + output + " new ord=" + ord);
      }
      
      MapEntry e = workingSet.get(input);
      if (e == null) {
        e = new MapEntry();
        workingSet.put(CharsRef.deepCopyOf(input), e); // make a copy, since we will keep around in our map    
      }
      
      e.ords.add(ord);
      e.includeOrig |= includeOrig;
      maxHorizontalContext = Math.max(maxHorizontalContext, numInputWords);
      maxHorizontalContext = Math.max(maxHorizontalContext, numOutputWords);
    }

    private int countWords(CharsRef chars) {
      int wordCount = 1;
      int upto = chars.offset;
      final int limit = chars.offset + chars.length;
      while(upto < limit) {
        if (chars.chars[upto++] == SynonymMap.WORD_SEPARATOR) {
          wordCount++;
        }
      }
      return wordCount;
    }
    
    /**
     * Add a phrase-&gt;phrase synonym mapping.
     * Phrases are character sequences where words are
     * separated with character zero (U+0000).  Empty words
     * (two U+0000s in a row) are not allowed in the input nor
     * the output!
     * 
     * @param input input phrase
     * @param output output phrase
     * @param includeOrig true if the original should be included
     */
    public void add(CharsRef input, CharsRef output, boolean includeOrig) {
      add(input, countWords(input), output, countWords(output), includeOrig);
    }
    
    /**
     * Builds an {@link SynonymMap} and returns it.
     */
    public SynonymMap build() throws IOException {
      ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
      // TODO: are we using the best sharing options?
      org.apache.lucene.util.fst.Builder<BytesRef> builder = 
        new org.apache.lucene.util.fst.Builder<>(FST.INPUT_TYPE.BYTE4, outputs);
      
      BytesRefBuilder scratch = new BytesRefBuilder();
      ByteArrayDataOutput scratchOutput = new ByteArrayDataOutput();

      final Set<Integer> dedupSet;

      if (dedup) {
        dedupSet = new HashSet<>();
      } else {
        dedupSet = null;
      }

      final byte[] spare = new byte[5];
      
      Set<CharsRef> keys = workingSet.keySet();
      CharsRef sortedKeys[] = keys.toArray(new CharsRef[keys.size()]);
      Arrays.sort(sortedKeys, CharsRef.getUTF16SortedAsUTF8Comparator());

      final IntsRefBuilder scratchIntsRef = new IntsRefBuilder();
      
      //System.out.println("fmap.build");
      for (int keyIdx = 0; keyIdx < sortedKeys.length; keyIdx++) {
        CharsRef input = sortedKeys[keyIdx];
        MapEntry output = workingSet.get(input);

        int numEntries = output.ords.size();
        // output size, assume the worst case
        int estimatedSize = 5 + numEntries * 5; // numEntries + one ord for each entry
        
        scratch.grow(estimatedSize);
        scratchOutput.reset(scratch.bytes());

        // now write our output data:
        int count = 0;
        for (int i = 0; i < numEntries; i++) {
          if (dedupSet != null) {
            // box once
            final Integer ent = output.ords.get(i);
            if (dedupSet.contains(ent)) {
              continue;
            }
            dedupSet.add(ent);
          }
          scratchOutput.writeVInt(output.ords.get(i));   
          count++;
        }

        final int pos = scratchOutput.getPosition();
        scratchOutput.writeVInt(count << 1 | (output.includeOrig ? 0 : 1));
        final int pos2 = scratchOutput.getPosition();
        final int vIntLen = pos2-pos;

        // Move the count + includeOrig to the front of the byte[]:
        System.arraycopy(scratch.bytes(), pos, spare, 0, vIntLen);
        System.arraycopy(scratch.bytes(), 0, scratch.bytes(), vIntLen, pos);
        System.arraycopy(spare, 0, scratch.bytes(), 0, vIntLen);

        if (dedupSet != null) {
          dedupSet.clear();
        }
        
        scratch.setLength(scratchOutput.getPosition());
        //System.out.println("  add input=" + input + " output=" + scratch + " offset=" + scratch.offset + " length=" + scratch.length + " count=" + count);
        builder.add(Util.toUTF32(input, scratchIntsRef), scratch.toBytesRef());
      }
      
      FST<BytesRef> fst = builder.finish();
      return new SynonymMap(fst, words, maxHorizontalContext);
    }
  }

  /**
   * Abstraction for parsing synonym files.
   *
   * @lucene.experimental
   */
  public static abstract class Parser extends Builder {

    private final Analyzer analyzer;

    public Parser(boolean dedup, Analyzer analyzer) {
      super(dedup);
      this.analyzer = analyzer;
    }

    /**
     * Parse the given input, adding synonyms to the inherited {@link Builder}.
     * @param in The input to parse
     */
    public abstract void parse(Reader in) throws IOException, ParseException;

    /** Sugar: analyzes the text with the analyzer and
     *  separates by {@link SynonymMap#WORD_SEPARATOR}.
     *  reuse and its chars must not be null. */
    public CharsRef analyze(String text, CharsRefBuilder reuse) throws IOException {
      try (TokenStream ts = analyzer.tokenStream("", text)) {
        CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
        PositionIncrementAttribute posIncAtt = ts.addAttribute(PositionIncrementAttribute.class);
        ts.reset();
        reuse.clear();
        while (ts.incrementToken()) {
          int length = termAtt.length();
          if (length == 0) {
            throw new IllegalArgumentException("term: " + text + " analyzed to a zero-length token");
          }
          if (posIncAtt.getPositionIncrement() != 1) {
            throw new IllegalArgumentException("term: " + text + " analyzed to a token with posinc != 1");
          }
          reuse.grow(reuse.length() + length + 1); /* current + word + separator */
          int end = reuse.length();
          if (reuse.length() > 0) {
            reuse.setCharAt(end++, SynonymMap.WORD_SEPARATOR);
            reuse.setLength(reuse.length() + 1);
          }
          System.arraycopy(termAtt.buffer(), 0, reuse.chars(), end, length);
          reuse.setLength(reuse.length() + length);
        }
        ts.end();
      }
      if (reuse.length() == 0) {
        throw new IllegalArgumentException("term: " + text + " was completely eliminated by analyzer");
      }
      return reuse.get();
    }
  }

}
