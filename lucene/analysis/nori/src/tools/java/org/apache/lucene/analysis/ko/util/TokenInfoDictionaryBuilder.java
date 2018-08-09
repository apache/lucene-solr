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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;

import com.ibm.icu.text.Normalizer2;
import org.apache.lucene.util.fst.PositiveIntOutputs;

public class TokenInfoDictionaryBuilder {
  
  /** Internal word id - incrementally assigned as entries are read and added. This will be byte offset of dictionary file */
  private int offset = 0;
  
  private String encoding = "utf-8";
  
  private boolean normalizeEntries = false;
  private Normalizer2 normalizer;

  public TokenInfoDictionaryBuilder(String encoding, boolean normalizeEntries) {
    this.encoding = encoding;
    this.normalizeEntries = normalizeEntries;
    this.normalizer = normalizeEntries ? Normalizer2.getInstance(null, "nfkc", Normalizer2.Mode.COMPOSE) : null;
  }
  
  public TokenInfoDictionaryWriter build(String dirname) throws IOException {
    FilenameFilter filter = (dir, name) -> name.endsWith(".csv");
    ArrayList<File> csvFiles = new ArrayList<>();
    for (File file : new File(dirname).listFiles(filter)) {
      csvFiles.add(file);
    }
    Collections.sort(csvFiles);
    return buildDictionary(csvFiles);
  }

  public TokenInfoDictionaryWriter buildDictionary(List<File> csvFiles) throws IOException {
    TokenInfoDictionaryWriter dictionary = new TokenInfoDictionaryWriter(10 * 1024 * 1024);
    
    // all lines in the file
    System.out.println("  parse...");
    List<String[]> lines = new ArrayList<>(400000);
    for (File file : csvFiles){
      FileInputStream inputStream = new FileInputStream(file);
      Charset cs = Charset.forName(encoding);
      CharsetDecoder decoder = cs.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT);
      InputStreamReader streamReader = new InputStreamReader(inputStream, decoder);
      BufferedReader reader = new BufferedReader(streamReader);
      
      String line = null;
      while ((line = reader.readLine()) != null) {
        String[] entry = CSVUtil.parse(line);

        if(entry.length < 12) {
          System.out.println("Entry in CSV is not valid: " + line);
          continue;
        }

        // NFKC normalize dictionary entry
        if (normalizeEntries) {
          String[] normalizedEntry = new String[entry.length];
          for (int i = 0; i < entry.length; i++) {
            normalizedEntry[i] = normalizer.normalize(entry[i]);
          }
          lines.add(normalizedEntry);
        } else {
          lines.add(entry);
        }
      }
    }
    
    System.out.println("  sort...");

    // sort by term: we sorted the files already and use a stable sort.
    Collections.sort(lines, Comparator.comparing(left -> left[0]));
    
    System.out.println("  encode...");

    PositiveIntOutputs fstOutput = PositiveIntOutputs.getSingleton();
    Builder<Long> fstBuilder = new Builder<>(FST.INPUT_TYPE.BYTE2, 0, 0, true, true, Integer.MAX_VALUE, fstOutput, true, 15);
    IntsRefBuilder scratch = new IntsRefBuilder();
    long ord = -1; // first ord will be 0
    String lastValue = null;

    // build tokeninfo dictionary
    for (String[] entry : lines) {
      int next = dictionary.put(entry);

      if(next == offset){
        System.out.println("Failed to process line: " + Arrays.toString(entry));
        continue;
      }

      String token = entry[0];
      if (!token.equals(lastValue)) {
        // new word to add to fst
        ord++;
        lastValue = token;
        scratch.grow(token.length());
        scratch.setLength(token.length());
        for (int i = 0; i < token.length(); i++) {
          scratch.setIntAt(i, (int) token.charAt(i));
        }
        fstBuilder.add(scratch.get(), ord);
      }
      dictionary.addMapping((int)ord, offset);
      offset = next;
    }

    final FST<Long> fst = fstBuilder.finish();
    
    System.out.print("  " + fstBuilder.getNodeCount() + " nodes, " + fstBuilder.getArcCount() + " arcs, " + fst.ramBytesUsed() + " bytes...  ");
    dictionary.setFST(fst);
    System.out.println(" done");
    
    return dictionary;
  }
}
