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
package org.apache.lucene.analysis.standard;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generates a file containing JFlex macros to accept valid ASCII TLDs 
 * (top level domains), for inclusion in JFlex grammars that can accept 
 * domain names.
 * <p> 
 * The IANA Root Zone Database is queried via HTTP from URL cmdline arg #0, the
 * response is parsed, and the results are written out to a file containing 
 * a JFlex macro that will accept all valid ASCII-only TLDs, including punycode 
 * forms of internationalized TLDs (output file cmdline arg #1).
 */
public class GenerateJflexTLDMacros {

  public static void main(String... args) throws Exception {
    if (args.length != 2 || args[0].equals("--help") || args[0].equals("-help")) {
      System.err.println("Cmd line params:");
      System.err.println("\tjava " + GenerateJflexTLDMacros.class.getName() 
                         + "<ZoneFileURL> <JFlexOutputFile>");
      System.exit(1);
    }
    new GenerateJflexTLDMacros(args[0], args[1]).execute();
  }
  
  private static final String NL = System.getProperty("line.separator");
  
  private static final String APACHE_LICENSE 
    = "/*" + NL
    + " * Licensed to the Apache Software Foundation (ASF) under one or more" + NL
    + " * contributor license agreements.  See the NOTICE file distributed with" + NL
    + " * this work for additional information regarding copyright ownership." + NL
    + " * The ASF licenses this file to You under the Apache License, Version 2.0" + NL
    + " * (the \"License\"); you may not use this file except in compliance with" + NL
    + " * the License.  You may obtain a copy of the License at" + NL
    + " *" + NL
    + " *     http://www.apache.org/licenses/LICENSE-2.0" + NL
    + " *" + NL
    + " * Unless required by applicable law or agreed to in writing, software" + NL
    + " * distributed under the License is distributed on an \"AS IS\" BASIS," + NL
    + " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied." + NL
    + " * See the License for the specific language governing permissions and" + NL
    + " * limitations under the License." + NL
    + " */" + NL;
    
  private static final Pattern TLD_PATTERN_1 
    = Pattern.compile("([-A-Za-z0-9]+)\\.\\s+NS\\s+.*");
  private static final Pattern TLD_PATTERN_2
    = Pattern.compile("([-A-Za-z0-9]+)\\.\\s+\\d+\\s+IN\\s+NS\\s+.*");
  private final URL tldFileURL;
  private long tldFileLastModified = -1L;
  private final File outputFile;

  public GenerateJflexTLDMacros(String tldFileURL, String outputFile)
    throws Exception {
    this.tldFileURL = new URL(tldFileURL);
    this.outputFile = new File(outputFile);
  }

  /**
   * Downloads the IANA Root Zone Database, extracts the ASCII TLDs, then
   * writes a JFlex macro accepting any of them case-insensitively out to
   * the specified output file.
   * 
   * @throws IOException if there is a problem either downloading the database
   *  or writing out the output file.
   */
  public void execute() throws IOException {
    final SortedSet<String> TLDs = getIANARootZoneDatabase();
    writeOutput(TLDs);
    System.err.println("Wrote " + TLDs.size() + " top level domains to '" 
                       + outputFile + "'.");
  }
  
  /**
   * Downloads the IANA Root Zone Database.
   * @return downcased sorted set of ASCII TLDs
   * @throws java.io.IOException if there is a problem downloading the database 
   */
  private SortedSet<String> getIANARootZoneDatabase() throws IOException {
    final SortedSet<String> TLDs = new TreeSet<>();
    final URLConnection connection = tldFileURL.openConnection();
    connection.setUseCaches(false);
    connection.addRequestProperty("Cache-Control", "no-cache");
    connection.connect();
    tldFileLastModified = connection.getLastModified();
    BufferedReader reader = new BufferedReader
      (new InputStreamReader(connection.getInputStream(), StandardCharsets.US_ASCII));
    try {
      String line;
      while (null != (line = reader.readLine())) {
        Matcher matcher = TLD_PATTERN_1.matcher(line);
        if (matcher.matches()) {
          TLDs.add(matcher.group(1).toLowerCase(Locale.ROOT));
        } else {
          matcher = TLD_PATTERN_2.matcher(line);
          if (matcher.matches()) {
            TLDs.add(matcher.group(1).toLowerCase(Locale.ROOT));
          }
        }
      }
    } finally {
      reader.close();
    }
    return TLDs;
  }

  /**
   * Writes a file containing a JFlex macro that will accept any of the given
   * TLDs case-insensitively.
   * 
   * @param ASCIITLDs The downcased sorted set of top level domains to accept
   * @throws IOException if there is an error writing the output file
   */
  private void writeOutput(SortedSet<String> ASCIITLDs) throws IOException {
    final DateFormat dateFormat = DateFormat.getDateTimeInstance
      (DateFormat.FULL, DateFormat.FULL, Locale.ROOT);
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    final Writer writer = new OutputStreamWriter
      (new FileOutputStream(outputFile), StandardCharsets.UTF_8);
    try {
      writer.write(APACHE_LICENSE);
      writer.write("// Generated from IANA Root Zone Database <");
      writer.write(tldFileURL.toString());
      writer.write(">");
      writer.write(NL);
      if (tldFileLastModified > 0L) {
        writer.write("// file version from ");
        writer.write(dateFormat.format(tldFileLastModified));
        writer.write(NL);
      }
      writer.write("// generated on ");
      writer.write(dateFormat.format(new Date()));
      writer.write(NL);
      writer.write("// by ");
      writer.write(this.getClass().getName());
      writer.write(NL);
      writer.write(NL);
      writer.write("ASCIITLD = \".\" (");
      writer.write(NL);
      boolean isFirst = true;
      for (String ASCIITLD : ASCIITLDs) {
        writer.write("\t");
        if (isFirst) {
          isFirst = false;
          writer.write("  "); 
        } else {
          writer.write("| "); 
        }
        writer.write(getCaseInsensitiveRegex(ASCIITLD));
        writer.write(NL);
      }
      writer.write("\t) \".\"?   // Accept trailing root (empty) domain");
      writer.write(NL);
      writer.write(NL);
    } finally {
      writer.close();
    }
  }

  /**
   * Returns a regex that will accept the given ASCII TLD case-insensitively.
   * 
   * @param ASCIITLD The ASCII TLD to generate a regex for
   * @return a regex that will accept the given ASCII TLD case-insensitively
   */
  private String getCaseInsensitiveRegex(String ASCIITLD) {
    StringBuilder builder = new StringBuilder();
    for (int pos = 0 ; pos < ASCIITLD.length() ; ++pos) {
      char ch = ASCIITLD.charAt(pos);
      if (Character.isDigit(ch) || ch == '-') {
        builder.append(ch);
      } else {
        builder.append("[").append(ch).append(Character.toUpperCase(ch)).append("]");
      }
    }
    return builder.toString();
  }
}
