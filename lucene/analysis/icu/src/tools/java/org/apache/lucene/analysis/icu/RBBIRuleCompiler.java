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


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import com.ibm.icu.text.RuleBasedBreakIterator;

/**
 * Command-line utility to converts RuleBasedBreakIterator (.rbbi) files into
 * binary compiled form (.brk).
 */
public class RBBIRuleCompiler {
  
  static String getRules(File ruleFile) throws IOException {
    StringBuilder rules = new StringBuilder();
    InputStream in = new FileInputStream(ruleFile);
    BufferedReader cin = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    String line = null;
    while ((line = cin.readLine()) != null) {
      if (!line.startsWith("#"))
        rules.append(line);
      rules.append('\n');
    }
    cin.close();
    in.close();
    return rules.toString();
  }
  
  static void compile(File srcDir, File destDir) throws Exception {
    File files[] = srcDir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.endsWith("rbbi");
      }});
    if (files == null) throw new IOException("Path does not exist: " + srcDir);
    for (int i = 0; i < files.length; i++) {
      File file = files[i];
      File outputFile = new File(destDir, 
          file.getName().replaceAll("rbbi$", "brk"));
      String rules = getRules(file);
      System.err.print("Compiling " + file.getName() + " to "
          + outputFile.getName() + ": ");
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
      FileOutputStream os = new FileOutputStream(outputFile);
      RuleBasedBreakIterator.compileRules(rules, os);
      os.close();
      System.err.println(outputFile.length() + " bytes.");
    }
  }
  
  public static void main(String args[]) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: RBBIRuleComputer <sourcedir> <destdir>");
      System.exit(1);
    }
    compile(new File(args[0]), new File(args[1]));
    System.exit(0);
  }
}
