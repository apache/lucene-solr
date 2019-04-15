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
package org.apache.lucene.analysis.ko.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.analysis.ko.dict.CharacterDefinition;

public class UnknownDictionaryBuilder {
  private static final String NGRAM_DICTIONARY_ENTRY = "NGRAM,1798,3559,3677,SY,*,*,*,*,*,*,*";
  
  private String encoding = "utf-8";
  
  public UnknownDictionaryBuilder(String encoding) {
    this.encoding = encoding;
  }
  
  public UnknownDictionaryWriter build(String dirname) throws IOException {
    UnknownDictionaryWriter unkDictionary = readDictionaryFile(dirname + File.separator + "unk.def");  //Should be only one file
    readCharacterDefinition(dirname + File.separator + "char.def", unkDictionary);
    return unkDictionary;
  }
  
  public UnknownDictionaryWriter readDictionaryFile(String filename)
      throws IOException {
    return readDictionaryFile(filename, encoding);
  }
  
  public UnknownDictionaryWriter readDictionaryFile(String filename, String encoding)
      throws IOException {
    UnknownDictionaryWriter dictionary = new UnknownDictionaryWriter(5 * 1024 * 1024);
    
    FileInputStream inputStream = new FileInputStream(filename);
    Charset cs = Charset.forName(encoding);
    CharsetDecoder decoder = cs.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);
    InputStreamReader streamReader = new InputStreamReader(inputStream, decoder);
    LineNumberReader lineReader = new LineNumberReader(streamReader);
    
    dictionary.put(CSVUtil.parse(NGRAM_DICTIONARY_ENTRY));
    
    List<String[]> lines = new ArrayList<>();
    String line = null;
    while ((line = lineReader.readLine()) != null) {
      // note: unk.def only has 10 fields, it simplifies the writer to just append empty reading and pronunciation,
      // even though the unknown dictionary returns hardcoded null here.
      final String[] parsed = CSVUtil.parse(line + ",*,*"); // Probably we don't need to validate entry
      lines.add(parsed);
    }
    
    Collections.sort(lines, new Comparator<String[]>() {
      public int compare(String[] left, String[] right) {
        int leftId = CharacterDefinition.lookupCharacterClass(left[0]);
        int rightId = CharacterDefinition.lookupCharacterClass(right[0]);
        return leftId - rightId;
      }
    });
    
    for (String[] entry : lines) {
      dictionary.put(entry);
    }
    
    return dictionary;
  }
  
  public void readCharacterDefinition(String filename, UnknownDictionaryWriter dictionary) throws IOException {
    FileInputStream inputStream = new FileInputStream(filename);
    InputStreamReader streamReader = new InputStreamReader(inputStream, encoding);
    LineNumberReader lineReader = new LineNumberReader(streamReader);
    
    String line = null;
    
    while ((line = lineReader.readLine()) != null) {
      line = line.replaceAll("^\\s", "");
      line = line.replaceAll("\\s*#.*", "");
      line = line.replaceAll("\\s+", " ");
      
      // Skip empty line or comment line
      if(line.length() == 0) {
        continue;
      }
      
      if(line.startsWith("0x")) {  // Category mapping
        String[] values = line.split(" ", 2);  // Split only first space
        
        if(!values[0].contains("..")) {
          int cp = Integer.decode(values[0]).intValue();
          dictionary.putCharacterCategory(cp, values[1]);
        } else {
          String[] codePoints = values[0].split("\\.\\.");
          int cpFrom = Integer.decode(codePoints[0]).intValue();
          int cpTo = Integer.decode(codePoints[1]).intValue();
          
          for(int i = cpFrom; i <= cpTo; i++){
            dictionary.putCharacterCategory(i, values[1]);
          }
        }
      } else {  // Invoke definition
        String[] values = line.split(" "); // Consecutive space is merged above
        String characterClassName = values[0];
        int invoke = Integer.parseInt(values[1]);
        int group = Integer.parseInt(values[2]);
        int length = Integer.parseInt(values[3]);
        dictionary.putInvokeDefinition(characterClassName, invoke, group, length);
      }
    }
  }
}
