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
import java.io.IOException;

public class DictionaryBuilder {

  private DictionaryBuilder() {
  }
  
  public static void build(String inputDirname, String outputDirname, String encoding, boolean normalizeEntry) throws IOException {
    System.out.println("building tokeninfo dict...");
    TokenInfoDictionaryBuilder tokenInfoBuilder = new TokenInfoDictionaryBuilder(encoding, normalizeEntry);
    TokenInfoDictionaryWriter tokenInfoDictionary = tokenInfoBuilder.build(inputDirname);
    tokenInfoDictionary.write(outputDirname);
    tokenInfoDictionary = null;
    tokenInfoBuilder = null;
    System.out.println("done");
    
    System.out.print("building unknown word dict...");
    UnknownDictionaryBuilder unkBuilder = new UnknownDictionaryBuilder(encoding);
    UnknownDictionaryWriter unkDictionary = unkBuilder.build(inputDirname);
    unkDictionary.write(outputDirname);
    unkDictionary = null;
    unkBuilder = null;
    System.out.println("done");
    
    System.out.print("building connection costs...");
    ConnectionCostsWriter connectionCosts
      = ConnectionCostsBuilder.build(inputDirname + File.separator + "matrix.def");
    connectionCosts.write(outputDirname);
    System.out.println("done");
  }
  
  public static void main(String[] args) throws IOException {
    String inputDirname = args[0];
    String outputDirname = args[1];
    String inputEncoding = args[2];
    boolean normalizeEntries = Boolean.parseBoolean(args[3]);
    
    System.out.println("dictionary builder");
    System.out.println("");
    System.out.println("input directory: " + inputDirname);
    System.out.println("output directory: " + outputDirname);
    System.out.println("input encoding: " + inputEncoding);
    System.out.println("normalize entries: " + normalizeEntries);
    System.out.println("");
    DictionaryBuilder.build(inputDirname, outputDirname, inputEncoding, normalizeEntries);
  }
  
}
