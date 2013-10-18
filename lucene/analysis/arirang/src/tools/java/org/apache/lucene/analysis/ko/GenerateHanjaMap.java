package org.apache.lucene.analysis.ko;

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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.ZipFile;

/**
 * Generates a mapping from single hanja to a set of possible hangul pronunciations.
 * <p>
 * This is used by KoreanFilter.analysisChinese() to perform substitutions and look
 * for dictionary entries.
 */
public class GenerateHanjaMap {
  
  // change this to where you want the stuff to go
  static final File output = new File("/home/rmuir/workspace/lucene-clean-trunk/lucene/analysis/arirang/src/resources/org/apache/lucene/analysis/ko/dic/mapHanja.dic");
  private static final String NL = System.getProperty("line.separator");

  public static void main(String args[]) throws Exception {
    // inefficient but we dont care
    Map<Character,Set<Character>> mappings = new TreeMap<>();
    addIMEMappings(mappings);
    addUnihanMappings(mappings);
    addOOMappings(mappings);
    // print statistics
    System.out.println("# hanja keys: " + mappings.size());
    int kvpairs = 0;
    for (Set<Character> hangul : mappings.values()) {
      kvpairs += hangul.size();
    }
    System.out.println("# hanja/hangul mappings: " + kvpairs);
    
    // write license
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output), "UTF-8"));
    BufferedReader licenseFile = new BufferedReader(new InputStreamReader(GenerateHanjaMap.class.getResourceAsStream("hanjamap.license.txt"), "UTF-8"));
    String line = null;
    while ((line = licenseFile.readLine()) != null) {
      writer.write(line);
      writer.write(NL);
    }
    licenseFile.close();
    
    // write out the mappings
    for (Character k : mappings.keySet()) {
      writer.write(k);
      writer.write(',');
      for (Character v : mappings.get(k)) {
        writer.write(v);
      }
      writer.write(NL);
    }
    writer.close();
  }
  
  static String IME_URL = "http://google-input-tools.googlecode.com/git/src/chrome/os/nacl-hangul/misc/hanja.txt";
  static void addIMEMappings(Map<Character,Set<Character>> mappings) throws Exception {
    BufferedReader r = new BufferedReader(new InputStreamReader(new URL(IME_URL).openStream(), "UTF-8"));
    String line = null;
    while ((line = r.readLine()) != null) {
      if (!line.startsWith("#") && line.length() > 0) {
        String tokens[] = line.split(":");
        if (tokens[1].length() == 1) {
          char k = tokens[1].charAt(0);
          if (tokens[0].length() != 1) {
            throw new RuntimeException();
          }
          char v = tokens[0].charAt(0);
          add(mappings, k, v);
        }
      }
    }
    r.close();
  }
  
  static String OO_URL = "http://svn.apache.org/repos/asf/openoffice/trunk/main/i18npool/source/textconversion/data/hhc_char.dic";
  static void addOOMappings(Map<Character,Set<Character>> mappings) throws Exception {
    BufferedReader r = new BufferedReader(new InputStreamReader(new URL(OO_URL).openStream(), "UTF-8"));
    String line = null;
    while ((line = r.readLine()) != null) {
      String fields[] = line.split(":");
      if (fields.length != 2) {
        throw new RuntimeException();
      }
      if (fields[0].length() != 1) {
        throw new RuntimeException();
      }
      char v = fields[0].charAt(0);
      for (int i = 0; i < fields[1].length(); i++) {
        add(mappings, fields[1].charAt(i), v);
      }
    }
    r.close();
  }
  
  static String UNIHAN_URL = "http://www.unicode.org/Public/6.3.0/ucd/Unihan.zip";
  static void addUnihanMappings(Map<Character,Set<Character>> mappings) throws Exception {
    URL url = new URL(UNIHAN_URL);
    ReadableByteChannel in = Channels.newChannel(url.openStream());
    File tmp = File.createTempFile("unihan", "zip");
    FileOutputStream out = new FileOutputStream(tmp);
    out.getChannel().transferFrom(in, 0, Long.MAX_VALUE);
    out.close();
    in.close();
    ZipFile zip = new ZipFile(tmp);
    BufferedReader r = new BufferedReader(new InputStreamReader(zip.getInputStream(zip.getEntry("Unihan_Readings.txt")), "UTF-8"));
    String line = null;
    while ((line = r.readLine()) != null) {
      if (!line.startsWith("#") && line.length() > 0) {
        String fields[] = line.split("\t");
        if (fields[1].equals("kHangul")) {
          int codepoint = Integer.parseInt(fields[0].substring(2), 16);
          if (codepoint > 0xFFFF) {
            throw new RuntimeException();
          }
          String readings[] = fields[2].split("\\s+");
          for (String reading : readings) {
            if (reading.length() != 1) {
              throw new RuntimeException();
            }
            add(mappings, (char)codepoint, reading.charAt(0));
          }
        }
      }
    }
    r.close();
    zip.close();
    tmp.delete();
  }
  
  static void add(Map<Character,Set<Character>> mappings, char k, char v) {
    Set<Character> current = mappings.get(k);
    if (current == null) {
      current = new TreeSet<Character>();
      mappings.put(k, current);
    }
    current.add(v);
  }
}
