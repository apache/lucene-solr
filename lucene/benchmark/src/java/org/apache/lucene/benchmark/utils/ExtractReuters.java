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
package org.apache.lucene.benchmark.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.util.IOUtils;

/**
 * Split the Reuters SGML documents into Simple Text files containing: Title, Date, Dateline, Body
 */
public class ExtractReuters {
  private Path reutersDir;
  private Path outputDir;

  public ExtractReuters(Path reutersDir, Path outputDir) throws IOException {
    this.reutersDir = reutersDir;
    this.outputDir = outputDir;
    System.out.println("Deleting all files in " + outputDir);
    IOUtils.rm(outputDir);
  }

  public void extract() throws IOException {
    long count = 0;
    Files.createDirectories(outputDir);
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(reutersDir, "*.sgm")) {
      for (Path sgmFile : stream) {
        extractFile(sgmFile);
        count++;
      }
    }
    if (count == 0) {
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
  protected void extractFile(Path sgmFile) {
    try (BufferedReader reader = Files.newBufferedReader(sgmFile, StandardCharsets.ISO_8859_1)) {
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
            outBuffer.append(System.lineSeparator()).append(System.lineSeparator());
          }
          String out = outBuffer.toString();
          for (int i = 0; i < META_CHARS_SERIALIZATIONS.length; i++) {
            out = out.replaceAll(META_CHARS_SERIALIZATIONS[i], META_CHARS[i]);
          }
          Path outFile = outputDir.resolve(sgmFile.getFileName() + "-" + (docNumber++) + ".txt");
          // System.out.println("Writing " + outFile);
          try (BufferedWriter writer = Files.newBufferedWriter(outFile, StandardCharsets.UTF_8)) {
            writer.write(out);
          }
          outBuffer.setLength(0);
          buffer.setLength(0);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      usage("Wrong number of arguments ("+args.length+")");
      return;
    }
    Path reutersDir = Paths.get(args[0]);
    if (!Files.exists(reutersDir)) {
      usage("Cannot find Path to Reuters SGM files ("+reutersDir+")");
      return;
    }
    
    // First, extract to a tmp directory and only if everything succeeds, rename
    // to output directory.
    Path outputDir = Paths.get(args[1] + "-tmp");
    Files.createDirectories(outputDir);
    ExtractReuters extractor = new ExtractReuters(reutersDir, outputDir);
    extractor.extract();
    // Now rename to requested output dir
    Files.move(outputDir, Paths.get(args[1]), StandardCopyOption.ATOMIC_MOVE);
  }

  private static void usage(String msg) {
    System.err.println("Usage: "+msg+" :: java -cp <...> org.apache.lucene.benchmark.utils.ExtractReuters <Path to Reuters SGM files> <Output Path>");
  }
  
}
