package org.apache.lucene.analysis.ko.dic;

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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;

public class DictionaryBuilder {
  public static void main(String args[]) throws Exception {
    String FILES_AS_IS[] = { 
      DictionaryResources.FILE_EOMI,
      DictionaryResources.FILE_JOSA,
      DictionaryResources.FILE_UNCOMPOUNDS
    };
    
    File inputDir = new File(args[0]);
    File outputDir = new File(args[1]);
    for (String file : FILES_AS_IS) {
      File in = new File(inputDir, file);
      File out = new File(outputDir, file);
      copyAsIs(in, out);
    }
    buildHanjaMap(inputDir, outputDir);
    buildSyllableDict(inputDir, outputDir);
    buildHangulDict(inputDir, outputDir);
  }
  
  static void copyAsIs(File in, File out) throws Exception {
    InputStream r = new BufferedInputStream(new FileInputStream(in));
    OutputStream w = new BufferedOutputStream(new FileOutputStream(out));
    int c;
    while ((c = r.read()) != -1) {
      w.write(c);
    }
    r.close();
    w.close();
  }
  
  static void buildHanjaMap(File inputDir, File outputDir) throws Exception {
    final int HANJA_START = 0x3400;
    final int IDX_SIZE = 0x10000 - HANJA_START;
    OutputStream idxStream = new BufferedOutputStream(new FileOutputStream(new File(outputDir, DictionaryResources.FILE_HANJA_IDX)));
    DataOutput idx = new OutputStreamDataOutput(idxStream);
    CodecUtil.writeHeader(idx, DictionaryResources.FILE_HANJA_IDX, DictionaryResources.DATA_VERSION);
    idx.writeVInt(PackedInts.VERSION_CURRENT);
    idx.writeVInt(1024);
    idx.writeVInt(IDX_SIZE+1); // CJK: first half of unicode, compat: at the end. but monotonic's blocking works here (?)
    MonotonicBlockPackedWriter idxArray = new MonotonicBlockPackedWriter(idx, 1024);
    
    OutputStream datStream = new BufferedOutputStream(new FileOutputStream(new File(outputDir, DictionaryResources.FILE_HANJA_DAT)));
    DataOutput dat = new OutputStreamDataOutput(datStream);
    CodecUtil.writeHeader(dat, DictionaryResources.FILE_HANJA_DAT, DictionaryResources.DATA_VERSION);
    char datArray[] = new char[256];
    File input = new File(inputDir, "mapHanja.dic");
    BufferedReader reader = new BufferedReader(IOUtils.getDecodingReader(input, IOUtils.CHARSET_UTF_8));
    int currentInput = -1;
    int currentOutput = 0;
    String line = null;
    while ((line = reader.readLine()) != null) {
      if (line.startsWith("!")) {
        continue;
      }
      int cp = line.charAt(0) - HANJA_START;
      while (currentInput < cp) {
        idxArray.add(currentOutput);
        currentInput++;
      }
      String mappings = line.substring(2);
      for (int i = 0; i < mappings.length(); i++) {
        if (currentOutput == datArray.length) {
          datArray = ArrayUtil.grow(datArray);
        }
        datArray[currentOutput] = mappings.charAt(i);
        currentOutput++;
      }
      currentInput = cp;
    }
    while (currentInput < IDX_SIZE) {
      idxArray.add(currentOutput);
      currentInput++;
    }
    idxArray.finish();
    dat.writeVInt(currentOutput);
    for (int i = 0; i < currentOutput; i++) {
      dat.writeShort((short) datArray[i]); 
    }
    idxStream.close();
    datStream.close();
  }

  static void buildSyllableDict(File inputDir, File outputDir) throws Exception {
    // Syllable features by index:
    //  0: JOSA1: 조사의 첫음절로 사용되는 음절 49개
    //  1: JOSA2: 조사의 두 번째 이상의 음절로 사용되는 음절 58개
    //  2: EOMI1: 어미의 첫음절로 사용되는 음절 72개
    //  3: EOMI2: 어미의 두 번째 이상의 음절로 사용되는 음절 105개
    //  4: YONG1: 1음절 용언에 사용되는 음절 362개
    //  5: YONG2: 2음절 용언의 마지막 음절로 사용되는 음절 316개
    //  6: YONG3: 3음절 이상 용언의 마지막 음절로 사용되는 음절 195개
    //  7: CHEON1: 1음절 체언에 사용되는 음절 680개
    //  8: CHEON2: 2음절 체언의 마지막 음절로 사용되는 음절 916개
    //  9: CHEON3: 3음절 체언의 마지막 음절로 사용되는 음절 800개
    // 10: CHEON4: 4음절 체언의 마지막 음절로 사용되는 음절 610개
    // 11: CHEON5: 5음절 이상 체언의 마지막 음절로 사용되는 음절 330개
    // 12: BUSA1: 1음절 부사의 마지막 음절로 사용되는 음절 191개
    // 13: BUSA2: 2음절 부사의 마지막 음절로 사용되는 음절 519개
    // 14: BUSA3: 3음절 부사의 마지막 음절로 사용되는 음절 139개
    // 15: BUSA4: 4음절 부사의 마지막 음절로 사용되는 음절 366개
    // 16: BUSA5: 5음절 부사의 마지막 음절로 사용되는 음절 79개
    // 17: PRONOUN: 대명사의 마지막 음절로 사용되는 음절 77개
    // 18: EXCLAM: 관형사와 감탄사의 마지막 음절로 사용되는 음절 241개
    // 19: YNPNA: (용언+'-ㄴ')에 의하여 생성되는 음절 129개
    // 20: YNPLA: (용언+'-ㄹ')에 의해 생성되는 음절 129개
    // 21: YNPMA: (용언+'-ㅁ')에 의해 생성되는 음절 129개
    // 22: YNPBA: (용언+'-ㅂ')에 의해 생성되는 음절 129개
    // 23: YNPAH: 모음으로 끝나는 음절 129개중 'ㅏ/ㅓ/ㅐ/ㅔ/ㅕ'로 끝나는 것이 선어말 어미 '-었-'과 결합할 때 생성되는 음절
    // 24: YNPOU: 모음 'ㅗ/ㅜ'로 끝나는 음절이 '아/어'로 시작되는 어미나 선어말 어미 '-었-'과 결합할 때 생성되는 음절
    // 25: YNPEI: 모음 'ㅣ'로 끝나는 용언이 '아/어'로 시작되는 어미나 선어말 어미 '-었-'과 결합할 때 생성되는 음절
    // 26: YNPOI: 모음 'ㅚ'로 끝나는 용언이 '아/어'로 시작되는 어미나 선어말 어미 '-었-'과 결합할 때 생성되는 음절
    // 27: YNPLN: 받침 'ㄹ'로 끝나는 용언이 어미 '-ㄴ'과 결합할 때 생성되는 음절
    // 28: IRRLO: '러' 불규칙(8개)에 의하여 생성되는 음절 : 러, 렀
    // 29: IRRPLE: '르' 불규칙(193개)에 의하여 생성되는 음절 
    // 30: IRROO: '우' 불규칙에 의하여 생성되는 음절 : 퍼, 펐
    // 31: IRROU: '어' 불규칙에 의하여 생성되는 음절 : 해, 했
    // 32: IRRDA: 'ㄷ' 불규칙(37개)에 의하여 생성되는 음절
    // 33: IRRBA: 'ㅂ' 불규칙(446개)에 의하여 생성되는 음절
    // 34: IRRSA: 'ㅅ' 불규칙(39개)에 의하여 생성되는 음절
    // 35: IRRHA: 'ㅎ' 불규칙(96개)에 의하여 생성되는 음절 
    // 36: PEND: 선어말 어미 : 시 셨 았 었 였 겠
    // 37: YNPEOMI: 용언이 어미와 결합할 때 생성되는 음절의 수 734개
    // 38: WD_SURF: 용언의 표층 형태로만 사용되는 음절 
    // 39: EOGAN: 어미 또는 어미의 변형으로 존재할 수 있는 음 (즉 IDX_EOMI 이거나 IDX_YNPNA 이후에 1이 있는 음절)
    
    OutputStream stream = new BufferedOutputStream(new FileOutputStream(new File(outputDir, DictionaryResources.FILE_SYLLABLE_DAT)));
    DataOutput out = new OutputStreamDataOutput(stream);
    CodecUtil.writeHeader(out, DictionaryResources.FILE_SYLLABLE_DAT, DictionaryResources.DATA_VERSION);
    
    int numBits = (1 + 0xD7AF - 0xAC00) * 11;
    FixedBitSet features = new FixedBitSet(numBits);
    int idx = 0;
    
    // (AC00-D7AF)
    File input = new File(inputDir, "syllable.dic");
    BufferedReader reader = new BufferedReader(IOUtils.getDecodingReader(input, IOUtils.CHARSET_UTF_8));
    String line = null;
    int last = 0xABFF;
    while ((line = reader.readLine()) != null) {
      if (!line.startsWith("!") && !line.startsWith("\uFEFF")) {
        // validate (using the comments!)
        final int ch;
        String currentChar = line.substring(43);
        if (currentChar.length() == 1) {
          ch = currentChar.charAt(0);
        } else {
          ch = Integer.parseInt(currentChar, 16);
        }
        assert ch == last + 1;
        last = ch;
        // set feature bits
        if (line.charAt(0) == '1') features.set(idx); idx++;
        if (line.charAt(1) == '1') features.set(idx); idx++;
        if (line.charAt(3) == '1') features.set(idx); idx++;
        if (line.charAt(19) == '1') features.set(idx); idx++;
        if (line.charAt(20) == '1') features.set(idx); idx++;
        if (line.charAt(21) == '1') features.set(idx); idx++;
        if (line.charAt(22) == '1') features.set(idx); idx++;
        if (line.charAt(23) == '1') features.set(idx); idx++;
        if (line.charAt(27) == '1') features.set(idx); idx++;
        if (line.charAt(38) == '1') features.set(idx); idx++;
        if (line.charAt(39) == '1') features.set(idx); idx++;
      }
    }
    assert idx == numBits;
    long raw[] = features.getBits();
    out.writeVInt(raw.length);
    for (int i = 0; i < raw.length; i++) {
      out.writeLong(raw[i]);
    }
    reader.close();
    stream.close();
  }
  
  
  /** 
   * makes FST (currently byte2 syllables) mapping to "word class"
   * each word has features + compound data, but many of them share the
   * same set of features, and have simple compound splits in the same place.
   */
  static void buildHangulDict(File inputDir, File outputDir) throws Exception {
    TreeMap<String,Integer> sorted = new TreeMap<String,Integer>();
    Map<Output,Integer> classes = new LinkedHashMap<>();
    File input = new File(inputDir, "dictionary.dic");
    BufferedReader reader = new BufferedReader(IOUtils.getDecodingReader(input, IOUtils.CHARSET_UTF_8));
    String line = null;
    while ((line = reader.readLine()) != null) {
      if (!line.startsWith("!") && !line.startsWith("\uFEFF")) {
        processLine(line, sorted, classes);
      }
    }
    reader.close();
    input = new File(inputDir, "extension.dic");
    reader = new BufferedReader(IOUtils.getDecodingReader(input, IOUtils.CHARSET_UTF_8));
    while ((line = reader.readLine()) != null) {
      if (!line.startsWith("!") && !line.startsWith("\uFEFF")) {
        processLine(line, sorted, classes);
      }
    }
    reader.close();
    input = new File(inputDir, "compounds.dic");
    reader = new BufferedReader(IOUtils.getDecodingReader(input, IOUtils.CHARSET_UTF_8));
    while ((line = reader.readLine()) != null) {
      if (!line.startsWith("!") && !line.startsWith("\uFEFF")) {
        processCompound(line, sorted, classes);
      }
    }
    reader.close();
    System.out.println("#words: " + sorted.size());
    System.out.println("#classes: " + classes.size());
    Outputs<Byte> fstOutput = ByteOutputs.getSingleton();
    // why does packed=false give a smaller fst?!?!
    Builder<Byte> builder = new Builder<Byte>(FST.INPUT_TYPE.BYTE2, 0, 0, true, true, Integer.MAX_VALUE, fstOutput, null, false, PackedInts.DEFAULT, true, 15);
    IntsRef scratch = new IntsRef();
    for (Map.Entry<String,Integer> e : sorted.entrySet()) {
      String token = e.getKey();
      scratch.grow(token.length());
      scratch.length = token.length();
      for (int i = 0; i < token.length(); i++) {
        scratch.ints[i] = (int) token.charAt(i);
      }
      int v = e.getValue();
      assert v >= 0 && v < 128;
      builder.add(scratch, (byte)v);
    }
    FST<Byte> fst = builder.finish();
    System.out.println("FST size: " + fst.sizeInBytes());
    OutputStream stream = new BufferedOutputStream(new FileOutputStream(new File(outputDir, DictionaryResources.FILE_WORDS_DAT)));
    DataOutput out = new OutputStreamDataOutput(stream);
    CodecUtil.writeHeader(out, DictionaryResources.FILE_WORDS_DAT, DictionaryResources.DATA_VERSION);
    out.writeByte((byte)classes.size());
    for (Output o : classes.keySet()) {
      o.write(out);
    }
    fst.save(out);
    stream.close();
  }
  
  static void processLine(String line, TreeMap<String,Integer> sorted, Map<Output,Integer> classes) {
    String[] infos = line.split("[,]+");
    assert infos.length == 2;
    assert infos[1].length() == 10;
    Output output = new Output();
    output.flags = (char) parseFlags(infos[1]);
    output.splits = Collections.emptyList();
    Integer ord = classes.get(output);
    if (ord == null) {
      ord = classes.size();
      classes.put(output, ord);
    }
    sorted.put(infos[0], ord);
  }
  
  static void processCompound(String line, TreeMap<String,Integer> sorted, Map<Output,Integer> classes) {
    String[] infos = line.split("[:]+");
    assert infos.length == 3;
    assert infos[2].length() == 4;
    Output output = new Output();
    
    if (!infos[1].replace(",", "").equals(infos[0])) {
      output.flags = (char) parseFlags("300"+infos[2]+"00X");
      output.decomp = infos[1];
    } else {
      output.flags = (char) parseFlags("200"+infos[2]+"00X");
      output.splits = parseSplits(infos[1]);
    }
    
    Integer ord = classes.get(output);
    if (ord == null) {
      ord = classes.size();
      classes.put(output, ord);
    }
    sorted.put(infos[0], ord);
  }
  
  static List<Integer> parseSplits(String line) {
    List<Integer> splits = new ArrayList<>();
    int current = 0;
    while (true) {
      current = line.indexOf(',', current);
      assert current != 0;
      assert current != line.length();
      if (current < 0) {
        break;
      }
      splits.add(current - splits.size());
      current++;
    }
    
    // validate splits data
    assert !splits.isEmpty();
    String comp = line.replaceAll(",", "");
    StringBuilder sb = new StringBuilder();
    int last = 0;
    for (int i : splits) {
      assert i < comp.length();
      assert i > last;
      sb.append(comp.substring(last, i));
      last = i;
    }
    sb.append(comp.substring(last));
    assert sb.toString().equals(comp);
    
    return splits;
  }
    
  
  static class Output {
    char flags;
    List<Integer> splits;
    String decomp;

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((decomp == null) ? 0 : decomp.hashCode());
      result = prime * result + flags;
      result = prime * result + ((splits == null) ? 0 : splits.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      Output other = (Output) obj;
      if (decomp == null) {
        if (other.decomp != null) return false;
      } else if (!decomp.equals(other.decomp)) return false;
      if (flags != other.flags) return false;
      if (splits == null) {
        if (other.splits != null) return false;
      } else if (!splits.equals(other.splits)) return false;
      return true;
    }

    static final int MAX_SPLITS = HangulDictionary.RECORD_SIZE - 3;
    
    public void write(DataOutput output) throws IOException {
      output.writeShort((short)flags);
      if (decomp != null) {
        assert decomp.length() <= MAX_SPLITS/2;
        output.writeByte((byte) decomp.length());
        for (int i = 0; i < decomp.length(); i++) {
          output.writeShort((short) decomp.charAt(i));
        }
        for (int i = decomp.length(); i < MAX_SPLITS/2; i++) {
          output.writeShort((short)0);
        }
      } else {
        assert splits.size() <= MAX_SPLITS;
        output.writeByte((byte) splits.size());
        for (int i : splits) {
          output.writeByte((byte)i);
        }
        for (int i = splits.size(); i < MAX_SPLITS; i++) {
          output.writeByte((byte)0);
        }
      }
    }
  }
  
  private static int parseFlags(String buffer) {
    if (buffer.length() != 10) {
      throw new IllegalArgumentException("Invalid flags: " + buffer);
    }
    int flags = 0;
    // IDX_NOUN: 1 if noun, 2 if compound, 3 if "strange compound"
    if (buffer.charAt(0) == '3') {
      flags |= WordEntry.COMPOUND | WordEntry.COMPOUND_IRREGULAR | WordEntry.NOUN;
    } else if (buffer.charAt(0) == '2') {
      flags |= WordEntry.COMPOUND | WordEntry.NOUN;
    } else if (buffer.charAt(0) == '1') {
      flags |= WordEntry.NOUN;
    } else if (buffer.charAt(0) != '0') {
      throw new IllegalArgumentException("Invalid flags: " + buffer);
    }
    // IDX_VERB
    if (parseBoolean(buffer, 1)) {
      flags |= WordEntry.VERB;
    }
    // IDX_BUSA
    if (parseBoolean(buffer, 2)) {
      flags |= WordEntry.BUSA;
    }
    // IDX_DOV
    if (parseBoolean(buffer, 3)) {
      flags |= WordEntry.DOV;
    }
    // IDX_BEV
    if (parseBoolean(buffer, 4)) {
      flags |= WordEntry.BEV;
    }
    // IDX_NE
    if (parseBoolean(buffer, 5)) {
      flags |= WordEntry.NE;
    }
    // IDX_REGURA
    switch(buffer.charAt(9)) {
      case 'B': return flags | WordEntry.VERB_TYPE_BIUP;
      case 'H': return flags | WordEntry.VERB_TYPE_HIOOT;
      case 'U': return flags | WordEntry.VERB_TYPE_LIUL;
      case 'L': return flags | WordEntry.VERB_TYPE_LOO;
      case 'S': return flags | WordEntry.VERB_TYPE_SIUT;
      case 'D': return flags | WordEntry.VERB_TYPE_DI;
      case 'R': return flags | WordEntry.VERB_TYPE_RU;
      case 'X': return flags | WordEntry.VERB_TYPE_REGULAR;
      default: throw new IllegalArgumentException("Invalid flags: " + buffer);
    }
  }
  
  private static boolean parseBoolean(String buffer, int position) {
    if (buffer.charAt(position) == '1') {
      return true;
    } else if (buffer.charAt(position) == '0') {
      return false;
    } else {
      throw new IllegalArgumentException("Invalid flags: " + buffer);
    }
  }
}
