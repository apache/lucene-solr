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
package org.apache.solr.internal.csv;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Random;

import junit.framework.TestCase;

/**
 * CSVPrinterTest
 */
public class CSVPrinterTest extends TestCase {
  
  String lineSeparator = "\n";

  public void testPrinter1() throws IOException {
    StringWriter sw = new StringWriter();
    CSVPrinter printer = new CSVPrinter(sw, CSVStrategy.DEFAULT_STRATEGY);
    String[] line1 = {"a", "b"};
    printer.println(line1);
    assertEquals("a,b" + lineSeparator, sw.toString());
  }

  public void testPrinter2() throws IOException {
    StringWriter sw = new StringWriter();
    CSVPrinter printer = new CSVPrinter(sw, CSVStrategy.DEFAULT_STRATEGY);
    String[] line1 = {"a,b", "b"};
    printer.println(line1);
    assertEquals("\"a,b\",b" + lineSeparator, sw.toString());
  }

  public void testPrinter3() throws IOException {
    StringWriter sw = new StringWriter();
    CSVPrinter printer = new CSVPrinter(sw, CSVStrategy.DEFAULT_STRATEGY);
    String[] line1 = {"a, b", "b "};
    printer.println(line1);
    assertEquals("\"a, b\",\"b \"" + lineSeparator, sw.toString());
  }

  public void testExcelPrinter1() throws IOException {
    StringWriter sw = new StringWriter();
    CSVPrinter printer = new CSVPrinter(sw, CSVStrategy.EXCEL_STRATEGY);
    String[] line1 = {"a", "b"};
    printer.println(line1);
    assertEquals("a,b" + lineSeparator, sw.toString());
  }

  public void testExcelPrinter2() throws IOException {
    StringWriter sw = new StringWriter();
    CSVPrinter printer = new CSVPrinter(sw, CSVStrategy.EXCEL_STRATEGY);
    String[] line1 = {"a,b", "b"};
    printer.println(line1);
    assertEquals("\"a,b\",b" + lineSeparator, sw.toString());
  }


  
  public void testRandom() throws Exception {
    int iter=10000;
    strategy = CSVStrategy.DEFAULT_STRATEGY;
    doRandom(iter);
    strategy = CSVStrategy.EXCEL_STRATEGY;
    doRandom(iter);

    // Strategy for MySQL
    strategy = new CSVStrategy
        ('\t', CSVStrategy.ENCAPSULATOR_DISABLED, CSVStrategy.COMMENTS_DISABLED,'\\',false, false, false, false, "\n");
    doRandom(iter);
  }

  Random r = new Random();
  CSVStrategy strategy;

  public void doRandom(int iter) throws Exception {
    for (int i=0; i<iter; i++) {
      doOneRandom();
    }
  }

  public void doOneRandom() throws Exception {
    int nLines = r.nextInt(4)+1;
    int nCol = r.nextInt(3)+1;
    // nLines=1;nCol=2;
    String[][] lines = new String[nLines][];
    for (int i=0; i<nLines; i++) {
      String[] line = new String[nCol];
      lines[i] = line;
      for (int j=0; j<nCol; j++) {
        line[j] = randStr();
      }
    }

    StringWriter sw = new StringWriter();
    CSVPrinter printer = new CSVPrinter(sw, strategy);

    for (int i=0; i<nLines; i++) {
      // for (int j=0; j<lines[i].length; j++) System.out.println("### VALUE=:" + printable(lines[i][j]));      
      printer.println(lines[i]);
    }

    printer.flush();
    String result = sw.toString();
    // System.out.println("### :" + printable(result));

    StringReader reader = new StringReader(result);

    CSVParser parser = new CSVParser(reader, strategy);
    String[][] parseResult = parser.getAllValues();

    if (!equals(lines, parseResult)) {
      System.out.println("Printer output :" + printable(result));
      assertTrue(false);
    }
  }

  public static boolean equals(String[][] a, String[][] b) {
    if (a.length != b.length) {
      return false;
    }
    for (int i=0; i<a.length; i++) {
      String[] linea = a[i];
      String[] lineb = b[i];
      if (linea.length != lineb.length) {
        return false;
      }
      for (int j=0; j<linea.length; j++) {
        String aval = linea[j];
        String bval = lineb[j];
        if (!aval.equals(bval)) {
          System.out.println("expected  :" + printable(aval));
          System.out.println("got       :" + printable(bval));
          return false;
        }
      }
    }
    return true;
  }

  public static String printable(String s) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<s.length(); i++) {
      char ch = s.charAt(i);
      if (ch<=' ' || ch>=128) {
        sb.append("(").append((int)ch).append(")");
      } else {
        sb.append(ch);
      }
    }
    return sb.toString();
  }

  public String randStr() {
    int sz = r.nextInt(20);
    // sz = r.nextInt(3);
    char[] buf = new char[sz];
    for (int i=0; i<sz; i++) {
      // stick in special chars with greater frequency
      char ch;
      int what = r.nextInt(20);
      switch (what) {
        case 0: ch = '\r'; break;
        case 1: ch = '\n'; break;
        case 2: ch = '\t'; break;
        case 3: ch = '\f'; break;
        case 4: ch = ' ';  break;
        case 5: ch = ',';  break;
        case 6: ch = '"';  break;
        case 7: ch = '\''; break;
        case 8: ch = '\\'; break;
        default: ch = (char)r.nextInt(300); break;
        // default: ch = 'a'; break;
      }
      buf[i] = ch;
    }
    return new String(buf);
  }

}
