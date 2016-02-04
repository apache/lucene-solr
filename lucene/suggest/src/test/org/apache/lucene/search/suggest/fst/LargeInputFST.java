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
package org.apache.lucene.search.suggest.fst;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.OfflineSorter;

/**
 * Try to build a suggester from a large data set. The input is a simple text
 * file, newline-delimited.
 */
public class LargeInputFST {
  public static void main(String[] args) throws IOException {
    Path input = Paths.get("/home/dweiss/tmp/shuffled.dict");

    int buckets = 20;
    int shareMaxTail = 10;

    ExternalRefSorter sorter = new ExternalRefSorter(new OfflineSorter());
    FSTCompletionBuilder builder = new FSTCompletionBuilder(buckets, sorter, shareMaxTail);

    BufferedReader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8);
    
    BytesRefBuilder scratch = new BytesRefBuilder();
    String line;
    int count = 0;
    while ((line = reader.readLine()) != null) {
      scratch.copyChars(line);
      builder.add(scratch.get(), count % buckets);
      if ((count++ % 100000) == 0) {
        System.err.println("Line: " + count);
      }
    }

    System.out.println("Building FSTCompletion.");
    FSTCompletion completion = builder.build();

    Path fstFile = Paths.get("completion.fst");
    System.out.println("Done. Writing automaton: " + fstFile.toAbsolutePath());
    completion.getFST().save(fstFile);
    sorter.close();
  }
}
