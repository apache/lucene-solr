package org.apache.lucene.search.suggest.fst;

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

import java.io.*;

import org.apache.lucene.search.suggest.Sort;
import org.apache.lucene.util.BytesRef;

/**
 * Try to build a suggester from a large data set. The input is a simple text
 * file, newline-delimited.
 */
public class LargeInputFST {
  public static void main(String[] args) throws IOException {
    File input = new File("/home/dweiss/tmp/shuffled.dict");

    int buckets = 20;
    int shareMaxTail = 10;

    ExternalRefSorter sorter = new ExternalRefSorter(new Sort());
    FSTCompletionBuilder builder = new FSTCompletionBuilder(buckets, sorter, shareMaxTail);

    BufferedReader reader = new BufferedReader(
        new InputStreamReader(
            new FileInputStream(input), "UTF-8"));
    
    BytesRef scratch = new BytesRef();
    String line;
    int count = 0;
    while ((line = reader.readLine()) != null) {
      scratch.copyChars(line);
      builder.add(scratch, count % buckets);
      if ((count++ % 100000) == 0) {
        System.err.println("Line: " + count);
      }
    }

    System.out.println("Building FSTCompletion.");
    FSTCompletion completion = builder.build();

    File fstFile = new File("completion.fst");
    System.out.println("Done. Writing automaton: " + fstFile.getAbsolutePath());
    completion.getFST().save(fstFile);
    sorter.close();
  }
}
