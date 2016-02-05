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


import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.lang.UProperty;
import com.ibm.icu.text.UnicodeSet;
import com.ibm.icu.text.UnicodeSetIterator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Downloads/generates lucene/analysis/icu/src/data/utr30/*.txt
 *
 * ASSUMPTION: This class will be run with current directory set to
 * lucene/analysis/icu/src/data/utr30/
 *
 * <ol>
 *   <li>
 *     Downloads nfc.txt, nfkc.txt and nfkc_cf.txt from icu-project.org,
 *     overwriting the versions in lucene/analysis/icu/src/data/utr30/.
 *   </li>
 *   <li>
 *     Converts round-trip mappings in nfc.txt (containing '=')
 *     that map to at least one [:Diacritic:] character
 *     into one-way mappings ('&gt;' instead of '=').
 *   </li>
 * </ol>
 */
public class GenerateUTR30DataFiles {
  private static final String ICU_SVN_TAG_URL
      = "http://source.icu-project.org/repos/icu/icu/tags";
  private static final String ICU_RELEASE_TAG = "release-54-1";
  private static final String ICU_DATA_NORM2_PATH = "source/data/unidata/norm2";
  private static final String NFC_TXT = "nfc.txt";
  private static final String NFKC_TXT = "nfkc.txt";
  private static final String NFKC_CF_TXT = "nfkc_cf.txt";
  private static byte[] DOWNLOAD_BUFFER = new byte[8192];
  private static final Pattern ROUND_TRIP_MAPPING_LINE_PATTERN
      = Pattern.compile("^\\s*([^=]+?)\\s*=\\s*(.*)$");
  private static final Pattern VERBATIM_RULE_LINE_PATTERN
      = Pattern.compile("^#\\s*Rule:\\s*verbatim\\s*$", Pattern.CASE_INSENSITIVE);
  private static final Pattern RULE_LINE_PATTERN
      = Pattern.compile("^#\\s*Rule:\\s*(.*)>(.*)", Pattern.CASE_INSENSITIVE);
  private static final Pattern BLANK_OR_COMMENT_LINE_PATTERN
      = Pattern.compile("^\\s*(?:#.*)?$");
  private static final Pattern NUMERIC_VALUE_PATTERN
      = Pattern.compile("Numeric[-\\s_]*Value", Pattern.CASE_INSENSITIVE);

  public static void main(String args[]) {
    try {
      getNFKCDataFilesFromIcuProject();
      expandRulesInUTR30DataFiles();
    } catch (Throwable t) {
      t.printStackTrace(System.err);
      System.exit(1);
    }
  }

  private static void expandRulesInUTR30DataFiles() throws IOException {
    FileFilter filter = new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        String name = pathname.getName();
        return pathname.isFile() && name.matches(".*\\.(?s:txt)")
            && ! name.equals(NFC_TXT) && ! name.equals(NFKC_TXT)
            && ! name.equals(NFKC_CF_TXT);
      }
    };
    for (File file : new File(".").listFiles(filter)) {
      expandDataFileRules(file);
    }
  }

  private static void expandDataFileRules(File file) throws IOException {
    final FileInputStream stream = new FileInputStream(file);
    final InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8);
    final BufferedReader bufferedReader = new BufferedReader(reader);
    StringBuilder builder = new StringBuilder();
    String line;
    boolean verbatim = false;
    boolean modified = false;
    int lineNum = 0;
    try {
      while (null != (line = bufferedReader.readLine())) {
        ++lineNum;
        if (VERBATIM_RULE_LINE_PATTERN.matcher(line).matches()) {
          verbatim = true;
          builder.append(line).append("\n");
        } else {
          Matcher ruleMatcher = RULE_LINE_PATTERN.matcher(line);
          if (ruleMatcher.matches()) {
            verbatim = false;
            builder.append(line).append("\n");
            try {
              String leftHandSide = ruleMatcher.group(1).trim();
              String rightHandSide = ruleMatcher.group(2).trim();
              expandSingleRule(builder, leftHandSide, rightHandSide);
            } catch (IllegalArgumentException e) {
              System.err.println
                  ("ERROR in " + file.getName() + " line #" + lineNum + ":");
              e.printStackTrace(System.err);
              System.exit(1);
            }
            modified = true;
          } else {
            if (BLANK_OR_COMMENT_LINE_PATTERN.matcher(line).matches()) {
              builder.append(line).append("\n");
            } else {
              if (verbatim) {
                builder.append(line).append("\n");
              } else {
                modified = true;
              }
            }
          }
        }
      }
    } finally {
      bufferedReader.close();
    }
    if (modified) {
      System.err.println("Expanding rules in and overwriting " + file.getName());
      final FileOutputStream out = new FileOutputStream(file, false);
      Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
      try {
        writer.write(builder.toString());
      } finally {
        writer.close();
      }
    }
  }

  private static void getNFKCDataFilesFromIcuProject() throws IOException {
    URL icuTagsURL = new URL(ICU_SVN_TAG_URL + "/");
    URL icuReleaseTagURL = new URL(icuTagsURL, ICU_RELEASE_TAG + "/");
    URL norm2url = new URL(icuReleaseTagURL, ICU_DATA_NORM2_PATH + "/");

    System.err.print("Downloading " + NFKC_TXT + " ... ");
    download(new URL(norm2url, NFKC_TXT), NFKC_TXT);
    System.err.println("done.");
    System.err.print("Downloading " + NFKC_CF_TXT + " ... ");
    download(new URL(norm2url, NFKC_CF_TXT), NFKC_CF_TXT);
    System.err.println("done.");

    System.err.print("Downloading " + NFKC_CF_TXT + " and making diacritic rules one-way ... ");
    URLConnection connection = openConnection(new URL(norm2url, NFC_TXT));
    BufferedReader reader = new BufferedReader
        (new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
    Writer writer = new OutputStreamWriter(new FileOutputStream(NFC_TXT), StandardCharsets.UTF_8);
    try {
      String line;

      while (null != (line = reader.readLine())) {
        Matcher matcher = ROUND_TRIP_MAPPING_LINE_PATTERN.matcher(line);
        if (matcher.matches()) {
          final String leftHandSide = matcher.group(1);
          final String rightHandSide = matcher.group(2).trim();
          List<String> diacritics = new ArrayList<>();
          for (String outputCodePoint : rightHandSide.split("\\s+")) {
            int ch = Integer.parseInt(outputCodePoint, 16);
            if (UCharacter.hasBinaryProperty(ch, UProperty.DIACRITIC)
                // gennorm2 fails if U+0653-U+0656 are included in round-trip mappings
                || (ch >= 0x653 && ch <= 0x656)) {
              diacritics.add(outputCodePoint);
            }
          }
          if ( ! diacritics.isEmpty()) {
            StringBuilder replacementLine = new StringBuilder();
            replacementLine.append(leftHandSide).append(">").append(rightHandSide);
            replacementLine.append("  # one-way: diacritic");
            if (diacritics.size() > 1) {
              replacementLine.append("s");
            }
            for (String diacritic : diacritics) {
              replacementLine.append(" ").append(diacritic);
            }
            line = replacementLine.toString();
          }
        }
        writer.write(line);
        writer.write("\n");
      }
    } finally {
      reader.close();
      writer.close();
    }
    System.err.println("done.");
  }

  private static void download(URL url, String outputFile)
      throws IOException {
    final URLConnection connection = openConnection(url);
    final InputStream inputStream = connection.getInputStream();
    final OutputStream outputStream = new FileOutputStream(outputFile);
    int numBytes;
    try {
      while (-1 != (numBytes = inputStream.read(DOWNLOAD_BUFFER))) {
        outputStream.write(DOWNLOAD_BUFFER, 0, numBytes);
      }
    } finally {
      inputStream.close();
      outputStream.close();
    }
  }

  private static URLConnection openConnection(URL url) throws IOException {
    final URLConnection connection = url.openConnection();
    connection.setUseCaches(false);
    connection.addRequestProperty("Cache-Control", "no-cache");
    connection.connect();
    return connection;
  }

  private static void expandSingleRule
      (StringBuilder builder, String leftHandSide, String rightHandSide)
      throws IllegalArgumentException {
    UnicodeSet set = new UnicodeSet(leftHandSide, UnicodeSet.IGNORE_SPACE);
    boolean numericValue = NUMERIC_VALUE_PATTERN.matcher(rightHandSide).matches();
    for (UnicodeSetIterator it = new UnicodeSetIterator(set) ; it.nextRange() ; ) {
      if (it.codepoint != UnicodeSetIterator.IS_STRING) {
        if (numericValue) {
          for (int cp = it.codepoint ; cp <= it.codepointEnd ; ++cp) {
            builder.append(String.format(Locale.ROOT, "%04X", cp)).append('>');
            builder.append(String.format(Locale.ROOT, "%04X", 0x30 + UCharacter.getNumericValue(cp)));
            builder.append("   # ").append(UCharacter.getName(cp));
            builder.append("\n");
          }
        } else {
          builder.append(String.format(Locale.ROOT, "%04X", it.codepoint));
          if (it.codepointEnd > it.codepoint) {
            builder.append("..").append(String.format(Locale.ROOT, "%04X", it.codepointEnd));
          }
          builder.append('>').append(rightHandSide).append("\n");
        }
      } else {
        System.err.println("ERROR: String '" + it.getString() + "' found in UnicodeSet");
        System.exit(1);
      }
    }
  }
}
