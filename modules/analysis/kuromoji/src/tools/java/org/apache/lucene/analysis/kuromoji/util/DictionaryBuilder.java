package org.apache.lucene.analysis.kuromoji.util;

/**
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

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.lucene.analysis.kuromoji.dict.ConnectionCosts;
import org.apache.lucene.analysis.kuromoji.dict.TokenInfoDictionary;
import org.apache.lucene.analysis.kuromoji.dict.UnknownDictionary;
import org.apache.lucene.analysis.kuromoji.trie.DoubleArrayTrie;

// nocommit: same dictionary is yielding different output depending on os/jvm?
public class DictionaryBuilder {
  
  public enum DictionaryFormat { IPADIC, UNIDIC };
  
  public DictionaryBuilder() {
    
  }
  
  public void build(DictionaryFormat format,
      String inputDirname,
      String outputDirname,
      String encoding,
      boolean normalizeEntry) throws IOException {
    System.out.println("building tokeninfo dict...");
    TokenInfoDictionaryBuilder tokenInfoBuilder = new TokenInfoDictionaryBuilder(format, encoding, normalizeEntry);
    TokenInfoDictionary tokenInfoDictionary = tokenInfoBuilder.build(inputDirname);
    
    System.out.print("  building double array trie...");
    DoubleArrayTrie trie = DoubleArrayTrieBuilder.build(tokenInfoBuilder.entrySet());
    trie.write(outputDirname+File.separatorChar+DoubleArrayTrie.class.getPackage().getName().replace('.',File.separatorChar));
    System.out.println("  done");
    
    System.out.print("  processing target map...");
    for (Entry<Integer, String> entry : tokenInfoBuilder.entrySet()) {
      int tokenInfoId = entry.getKey();
      String surfaceform = entry.getValue();
      int doubleArrayId = trie.lookup(surfaceform);
      assert doubleArrayId > 0;
      tokenInfoDictionary.addMapping(doubleArrayId, tokenInfoId);
    }		
    tokenInfoDictionary.write(outputDirname+File.separatorChar+TokenInfoDictionary.class.getPackage().getName().replace('.',File.separatorChar));
    trie = null;
    tokenInfoBuilder = null;
    
    System.out.println("  done");
    System.out.println("done");
    
    System.out.print("building unknown word dict...");
    UnknownDictionaryBuilder unkBuilder = new UnknownDictionaryBuilder(encoding);
    UnknownDictionary unkDictionary = unkBuilder.build(inputDirname);
    unkDictionary.write(outputDirname+File.separatorChar+UnknownDictionary.class.getPackage().getName().replace('.',File.separatorChar));
    System.out.println("done");
    
    System.out.print("building connection costs...");
    ConnectionCosts connectionCosts
      = ConnectionCostsBuilder.build(inputDirname + File.separator + "matrix.def");
    connectionCosts.write(outputDirname+File.separatorChar+ConnectionCosts.class.getPackage().getName().replace('.',File.separatorChar));
    System.out.println("done");
  }
  
  public static void main(String[] args) throws IOException, ClassNotFoundException {
    DictionaryFormat format;
    if (args[0].equalsIgnoreCase("ipadic")) {
      format = DictionaryFormat.IPADIC;
    } else if (args[0].equalsIgnoreCase("unidic")) {
      format = DictionaryFormat.UNIDIC;
    } else {
      System.err.println("Illegal format " + args[0] + " using unidic instead");
      format = DictionaryFormat.IPADIC;
    }
    
    String inputDirname = args[1];
    String outputDirname = args[2];
    String inputEncoding = args[3];
    boolean normalizeEntries = Boolean.parseBoolean(args[4]);
    
    DictionaryBuilder builder = new DictionaryBuilder();
    System.out.println("dictionary builder");
    System.out.println("");
    System.out.println("dictionary format: " + format);
    System.out.println("input directory: " + inputDirname);
    System.out.println("output directory: " + outputDirname);
    System.out.println("input encoding: " + inputEncoding);
    System.out.println("normalize entries: " + normalizeEntries);
    System.out.println("");
    builder.build(format, inputDirname, outputDirname, inputEncoding, normalizeEntries);
  }
  
}
