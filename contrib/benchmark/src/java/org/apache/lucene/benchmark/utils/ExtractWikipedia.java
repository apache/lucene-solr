package org.apache.lucene.benchmark.utils;

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

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Extract the downloaded Wikipedia dump into separate files for indexing.
 */
public class ExtractWikipedia {

  private File wikipedia;
  private File outputDir;

  public ExtractWikipedia(File wikipedia, File outputDir) {
    this.wikipedia = wikipedia;
    this.outputDir = outputDir;
    System.out.println("Deleting all files in " + outputDir);
    File [] files = outputDir.listFiles();
    for (int i = 0; i < files.length; i++) {
      files[i].delete();
    }
  }

  static public int count = 0;
  static String[] months = {"JAN", "FEB", "MAR", "APR",
                            "MAY", "JUN", "JUL", "AUG",
                            "SEP", "OCT", "NOV", "DEC"};

  public class Parser extends DefaultHandler {

    public Parser() {
    }

    StringBuffer contents = new StringBuffer();

    public void characters(char[] ch, int start, int length) {
      contents.append(ch, start, length);
    }

    String title;
    String id;
    String body;
    String time;

    static final int BASE = 10;

    public void startElement(String namespace,
                             String simple,
                             String qualified,
                             Attributes attributes) {
      if (qualified.equals("page")) {
        title = null;
        id = null;
        body = null;
        time = null;
      } else if (qualified.equals("text")) {
        contents.setLength(0);
      } else if (qualified.equals("timestamp")) {
        contents.setLength(0);
      } else if (qualified.equals("title")) {
        contents.setLength(0);
      } else if (qualified.equals("id")) {
        contents.setLength(0);
      }
    }

    public File directory (int count, File directory) {
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
      directory = new File (directory, (Integer.toString(base / BASE)));
      directory = new File (directory, (Integer.toString(count / (base / BASE))));
      return directory(count % (base / BASE), directory);
    }

    public void create(String id, String title, String time, String body) {

      File d = directory(count++, null);
      d.mkdirs();
      File f = new File(d, id + ".txt");

      StringBuffer contents = new StringBuffer();

      contents.append(time);
      contents.append("\n\n");
      contents.append(title);
      contents.append("\n\n");
      contents.append(body);
      contents.append("\n");

      try {
        FileWriter writer = new FileWriter(f);
        writer.write(contents.toString());
        writer.close();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }

    }

    String time(String original) {
      StringBuffer buffer = new StringBuffer();

      buffer.append(original.substring(8, 10));
      buffer.append('-');
      buffer.append(months[Integer.valueOf(original.substring(5, 7)).intValue() - 1]);
      buffer.append('-');
      buffer.append(original.substring(0, 4));
      buffer.append(' ');
      buffer.append(original.substring(11, 19));
      buffer.append(".000");

      return buffer.toString();
    }

    public void endElement(String namespace, String simple, String qualified) {
      if (qualified.equals("title")) {
        title = contents.toString();
      } else if (qualified.equals("text")) {
        body = contents.toString();
        if (body.startsWith("#REDIRECT") ||
             body.startsWith("#redirect")) {
          body = null;
        }
      } else if (qualified.equals("timestamp")) {
        time = time(contents.toString());
      } else if (qualified.equals("id") && id == null) {
        id = contents.toString();
      } else if (qualified.equals("page")) {
        if (body != null) {
          create(id, title, time, body);
        }
      }
    }
  }

  public void extract() {

    try {
      Parser parser = new Parser();
      if (false) {
        SAXParser sp = SAXParserFactory.newInstance().newSAXParser();
        sp.parse(new FileInputStream(wikipedia), parser);
      } else {
        XMLReader reader =
          XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
        reader.setContentHandler(parser);
        reader.setErrorHandler(parser);
        reader.parse(new InputSource(new FileInputStream(wikipedia)));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      printUsage();
    }

    File wikipedia = new File(args[0]);

    if (wikipedia.exists()) {
      File outputDir = new File(args[1]);
      outputDir.mkdirs();
      ExtractWikipedia extractor = new ExtractWikipedia(wikipedia, outputDir);
      extractor.extract();
    } else {
      printUsage();
    }
  }

  private static void printUsage() {
    System.err.println("Usage: java -cp <...> org.apache.lucene.benchmark.utils.ExtractWikipedia <Path to Wikipedia XML file> <Output Path>");
  }

}