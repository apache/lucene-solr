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


import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.lucene.benchmark.byTask.feeds.ContentSource;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.EnwikiContentSource;
import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.util.IOUtils;

/**
 * Extract the downloaded Wikipedia dump into separate files for indexing.
 */
public class ExtractWikipedia {

  private Path outputDir;

  static public int count = 0;

  static final int BASE = 10;
  protected DocMaker docMaker;

  public ExtractWikipedia(DocMaker docMaker, Path outputDir) throws IOException {
    this.outputDir = outputDir;
    this.docMaker = docMaker;
    System.out.println("Deleting all files in " + outputDir);
    IOUtils.rm(outputDir);
  }

  public Path directory(int count, Path directory) {
    if (directory == null) {
      directory = outputDir;
    }
    int base = BASE;
    while (base <= count) {
      base *= BASE;
    }
    if (count < BASE) {
      return directory;
    }
    directory = directory.resolve(Integer.toString(base / BASE));
    directory = directory.resolve(Integer.toString(count / (base / BASE)));
    return directory(count % (base / BASE), directory);
  }

  public void create(String id, String title, String time, String body) throws IOException {

    Path d = directory(count++, null);
    Files.createDirectories(d);
    Path f = d.resolve(id + ".txt");

    StringBuilder contents = new StringBuilder();

    contents.append(time);
    contents.append("\n\n");
    contents.append(title);
    contents.append("\n\n");
    contents.append(body);
    contents.append("\n");

    try (Writer writer = Files.newBufferedWriter(f, StandardCharsets.UTF_8)) {
      writer.write(contents.toString());
    }
  }

  public void extract() throws Exception {
    Document doc = null;
    System.out.println("Starting Extraction");
    long start = System.currentTimeMillis();
    try {
      while ((doc = docMaker.makeDocument()) != null) {
        create(doc.get(DocMaker.ID_FIELD), doc.get(DocMaker.TITLE_FIELD), doc
            .get(DocMaker.DATE_FIELD), doc.get(DocMaker.BODY_FIELD));
      }
    } catch (NoMoreDataException e) {
      //continue
    }
    long finish = System.currentTimeMillis();
    System.out.println("Extraction took " + (finish - start) + " ms");
  }

  public static void main(String[] args) throws Exception {

    Path wikipedia = null;
    Path outputDir = Paths.get("enwiki");
    boolean keepImageOnlyDocs = true;
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.equals("--input") || arg.equals("-i")) {
        wikipedia = Paths.get(args[i + 1]);
        i++;
      } else if (arg.equals("--output") || arg.equals("-o")) {
        outputDir = Paths.get(args[i + 1]);
        i++;
      } else if (arg.equals("--discardImageOnlyDocs") || arg.equals("-d")) {
        keepImageOnlyDocs = false;
      }
    }
    
    Properties properties = new Properties();
    properties.setProperty("docs.file", wikipedia.toAbsolutePath().toString());
    properties.setProperty("content.source.forever", "false");
    properties.setProperty("keep.image.only.docs", String.valueOf(keepImageOnlyDocs));
    Config config = new Config(properties);

    ContentSource source = new EnwikiContentSource();
    source.setConfig(config);
    
    DocMaker docMaker = new DocMaker();
    docMaker.setConfig(config, source);
    docMaker.resetInputs();
    if (Files.exists(wikipedia)) {
      System.out.println("Extracting Wikipedia to: " + outputDir + " using EnwikiContentSource");
      Files.createDirectories(outputDir);
      ExtractWikipedia extractor = new ExtractWikipedia(docMaker, outputDir);
      extractor.extract();
    } else {
      printUsage();
    }
  }

  private static void printUsage() {
    System.err.println("Usage: java -cp <...> org.apache.lucene.benchmark.utils.ExtractWikipedia --input|-i <Path to Wikipedia XML file> " +
            "[--output|-o <Output Path>] [--discardImageOnlyDocs|-d]");
    System.err.println("--discardImageOnlyDocs tells the extractor to skip Wiki docs that contain only images");
  }

}