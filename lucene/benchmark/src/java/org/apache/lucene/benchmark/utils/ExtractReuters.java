package org.apache.lucene.benchmark.utils;
/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.util.IOUtils;


/**
 * Split the Reuters SGML documents into Simple Text files containing: Title, Date, Dateline, Body
 */
public class ExtractReuters {
  private File reutersDir;
  private File outputDir;
  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  public ExtractReuters(File reutersDir, File outputDir) {
    this.reutersDir = reutersDir;
    this.outputDir = outputDir;
    System.out.println("Deleting all files in " + outputDir);
    for (File f : outputDir.listFiles()) {
      f.delete();
    }
  }

  public void extract() {
    File[] sgmFiles = reutersDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        return file.getName().endsWith(".sgm");
      }
    });
    if (sgmFiles != null && sgmFiles.length > 0) {
      for (File sgmFile : sgmFiles) {
        extractFile(sgmFile);
      }
    } else {
      System.err.println("No .sgm files in " + reutersDir);
    }
  }

  Pattern EXTRACTION_PATTERN = Pattern
      .compile("<TITLE>(.*?)</TITLE>|<DATE>(.*?)</DATE>|<BODY>(.*?)</BODY>");

  private static String[] META_CHARS = { "&", "<", ">", "\"", "'" };

  private static String[] META_CHARS_SERIALIZATIONS = { "&amp;", "&lt;",
      "&gt;", "&quot;", "&apos;" };

  /**
   * Override if you wish to change what is extracted
   */
  protected void extractFile(File sgmFile) {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(sgmFile), IOUtils.CHARSET_UTF_8));

      StringBuilder buffer = new StringBuilder(1024);
      StringBuilder outBuffer = new StringBuilder(1024);

      String line = null;
      int docNumber = 0;
      while ((line = reader.readLine()) != null) {
        // when we see a closing reuters tag, flush the file

        if (line.indexOf("</REUTERS") == -1) {
          // Replace the SGM escape sequences

          buffer.append(line).append(' ');// accumulate the strings for now,
                                          // then apply regular expression to
                                          // get the pieces,
        } else {
          // Extract the relevant pieces and write to a file in the output dir
          Matcher matcher = EXTRACTION_PATTERN.matcher(buffer);
          while (matcher.find()) {
            for (int i = 1; i <= matcher.groupCount(); i++) {
              if (matcher.group(i) != null) {
                outBuffer.append(matcher.group(i));
              }
            }
            outBuffer.append(LINE_SEPARATOR).append(LINE_SEPARATOR);
          }
          String out = outBuffer.toString();
          for (int i = 0; i < META_CHARS_SERIALIZATIONS.length; i++) {
            out = out.replaceAll(META_CHARS_SERIALIZATIONS[i], META_CHARS[i]);
          }
          File outFile = new File(outputDir, sgmFile.getName() + "-"
              + (docNumber++) + ".txt");
          // System.out.println("Writing " + outFile);
          OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(outFile), IOUtils.CHARSET_UTF_8);
          writer.write(out);
          writer.close();
          outBuffer.setLength(0);
          buffer.setLength(0);
        }
      }
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      usage("Wrong number of arguments ("+args.length+")");
      return;
    }
    File reutersDir = new File(args[0]);
    if (!reutersDir.exists()) {
      usage("Cannot find Path to Reuters SGM files ("+reutersDir+")");
      return;
    }
    
    // First, extract to a tmp directory and only if everything succeeds, rename
    // to output directory.
    File outputDir = new File(args[1]);
    outputDir = new File(outputDir.getAbsolutePath() + "-tmp");
    outputDir.mkdirs();
    ExtractReuters extractor = new ExtractReuters(reutersDir, outputDir);
    extractor.extract();
    // Now rename to requested output dir
    outputDir.renameTo(new File(args[1]));
  }

  private static void usage(String msg) {
    System.err.println("Usage: "+msg+" :: java -cp <...> org.apache.lucene.benchmark.utils.ExtractReuters <Path to Reuters SGM files> <Output Path>");
  }
  
}
