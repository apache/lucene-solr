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
package org.apache.lucene.analysis.icu;

import com.ibm.icu.text.RuleBasedBreakIterator;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Command-line utility to converts RuleBasedBreakIterator (.rbbi) files into binary compiled form
 * (.brk).
 */
public class RBBIRuleCompiler {

  static String getRules(Path ruleFile) throws IOException {
    StringBuilder rules = new StringBuilder();
    InputStream in = Files.newInputStream(ruleFile);
    BufferedReader cin = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    String line = null;
    while ((line = cin.readLine()) != null) {
      if (!line.startsWith("#")) {
        rules.append(line);
      }
      rules.append('\n');
    }
    cin.close();
    in.close();
    return rules.toString();
  }

  static void compile(Path srcDir, Path destDir) throws Exception {
    List<Path> files;
    try (var stream = Files.list(srcDir)) {
      files =
          stream
              .filter(name -> name.getFileName().toString().endsWith("rbbi"))
              .collect(Collectors.toList());
    }

    if (files.isEmpty()) throw new IOException("No input files matching *.rbbi at: " + srcDir);
    for (Path file : files) {
      Path outputFile = destDir.resolve(file.getFileName().toString().replaceAll("rbbi$", "brk"));
      String rules = getRules(file);
      System.err.print(
          "Compiling " + file.getFileName() + " to " + outputFile.getFileName() + ": ");
      /*
       * if there is a syntax error, compileRules() may succeed. the way to
       * check is to try to instantiate from the string. additionally if the
       * rules are invalid, you can get a useful syntax error.
       */
      try {
        new RuleBasedBreakIterator(rules);
      } catch (IllegalArgumentException e) {
        /*
         * do this intentionally, so you don't get a massive stack trace
         * instead, get a useful syntax error!
         */
        System.err.println(e.getMessage());
        System.exit(1);
      }
      try (OutputStream os = Files.newOutputStream(outputFile)) {
        RuleBasedBreakIterator.compileRules(rules, os);
      }
      System.err.println(Files.size(outputFile) + " bytes.");
    }
  }

  public static void main(String args[]) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: RBBIRuleComputer <sourcedir> <destdir>");
      System.exit(1);
    }
    compile(Paths.get(args[0]), Paths.get(args[1]));
    System.exit(0);
  }
}
