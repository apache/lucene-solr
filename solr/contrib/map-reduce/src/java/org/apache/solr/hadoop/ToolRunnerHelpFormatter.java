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
package org.apache.solr.hadoop;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.helper.ASCIITextWidthCounter;
import net.sourceforge.argparse4j.helper.TextHelper;

import org.apache.hadoop.util.ToolRunner;

/**
 * Nicely formats the output of
 * {@link ToolRunner#printGenericCommandUsage(PrintStream)} with the same look and feel that argparse4j uses for help text.
 */
class ToolRunnerHelpFormatter {
  
  public static String getGenericCommandUsage() {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    String msg;
    try {
      ToolRunner.printGenericCommandUsage(new PrintStream(bout, true, "UTF-8"));
      msg = new String(bout.toByteArray(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e); // unreachable
    }
    
    BufferedReader reader = new BufferedReader(new StringReader(msg));    
    StringBuilder result = new StringBuilder();
    while (true) {
      String line;
      try {
        line = reader.readLine();
      } catch (IOException e) {
        throw new RuntimeException(e); // unreachable
      }
      
      if (line == null) {
        return result.toString(); // EOS
      }
      
      if (!line.startsWith("-")) {
        result.append(line + "\n");
      } else {
        line = line.trim();
        int i = line.indexOf("  ");
        if (i < 0) {
          i = line.indexOf('\t');
        }
        if (i < 0) {
          result.append(line + "\n");          
        } else {
          String title = line.substring(0, i).trim();
          if (title.length() >= 3 && Character.isLetterOrDigit(title.charAt(1)) && Character.isLetterOrDigit(title.charAt(2))) {
            title = "-" + title; // prefer "--libjars" long arg style over "-libjars" style but retain "-D foo" short arg style        
          }
          String help = line.substring(i, line.length()).trim();
          StringWriter strWriter = new StringWriter(); 
          PrintWriter writer = new PrintWriter(strWriter, true);
          TextHelper.printHelp(writer, title, help, new ASCIITextWidthCounter(), ArgumentParsers.getFormatWidth());
          result.append(strWriter.toString());          
        }        
      }
    }
  }
}

