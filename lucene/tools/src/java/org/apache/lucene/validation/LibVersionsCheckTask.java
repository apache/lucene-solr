package org.apache.lucene.validation;

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

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.Resource;
import org.apache.tools.ant.types.ResourceCollection;
import org.apache.tools.ant.types.resources.FileResource;
import org.apache.tools.ant.types.resources.Resources;
import org.apache.tools.ant.util.FileNameMapper;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An Ant task to verify that the '/org/name' keys in ivy-versions.properties
 * are sorted lexically and are neither duplicates nor orphans, and that all
 * dependencies in all ivy.xml files use rev="${/org/name}" format.
 */
public class LibVersionsCheckTask extends Task {

  private static final String IVY_XML_FILENAME = "ivy.xml";
  private static final Pattern COORDINATE_KEY_PATTERN = Pattern.compile("(/[^/ \t\f]+/[^=:/ \t\f]+).*");
  private static final Pattern BLANK_OR_COMMENT_LINE_PATTERN = Pattern.compile("[ \t\f]*(?:[#!].*)?");
  private static final Pattern TRAILING_BACKSLASH_PATTERN = Pattern.compile("[^\\\\]*(\\\\+)$");
  private static final Pattern LEADING_WHITESPACE_PATTERN = Pattern.compile("[ \t\f]+(.*)");
  private static final Pattern WHITESPACE_GOODSTUFF_WHITESPACE_BACKSLASH_PATTERN
      = Pattern.compile("[ \t\f]*(.*?)(?:(?<!\\\\)[ \t\f]*)?\\\\");
  private static final Pattern TRAILING_WHITESPACE_BACKSLASH_PATTERN
      = Pattern.compile("(.*?)(?:(?<!\\\\)[ \t\f]*)?\\\\");

  /**
   * All ivy.xml files to check.
   */
  private Resources ivyXmlResources = new Resources();

  /**
   * Centralized Ivy versions properties file
   */
  private File centralizedVersionsFile;

  /**
   * License file mapper.
   */
  private FileNameMapper licenseMapper;

  /**
   * A logging level associated with verbose logging.
   */
  private int verboseLevel = Project.MSG_VERBOSE;

  /**
   * Failure flag.
   */
  private boolean failures;
  
  /**
   * All /org/name version keys found in ivy-versions.properties, and whether they
   * are referenced in any ivy.xml file.
   */
  private Map<String,Boolean> referencedCoordinateKeys = new LinkedHashMap<String,Boolean>();

  /**
   * Adds a set of ivy.xml resources to check.
   */
  public void add(ResourceCollection rc) {
    ivyXmlResources.add(rc);
  }

  public void setVerbose(boolean verbose) {
    verboseLevel = (verbose ? Project.MSG_INFO : Project.MSG_VERBOSE);
  }

  public void setCentralizedVersionsFile(File file) {
    centralizedVersionsFile = file;
  }

  /**
   * Execute the task.
   */
  @Override
  public void execute() throws BuildException {
    log("Starting scan.", verboseLevel);
    long start = System.currentTimeMillis();

    int errors = verifySortedCentralizedVersionsFile() ? 0 : 1;
    int checked = 0;

    @SuppressWarnings("unchecked")
    Iterator<Resource> iter = (Iterator<Resource>)ivyXmlResources.iterator();
    while (iter.hasNext()) {
      final Resource resource = iter.next();
      if ( ! resource.isExists()) {
        throw new BuildException("Resource does not exist: " + resource.getName());
      }
      if ( ! (resource instanceof FileResource)) {
        throw new BuildException("Only filesystem resources are supported: " 
            + resource.getName() + ", was: " + resource.getClass().getName());
      }

      File ivyXmlFile = ((FileResource)resource).getFile();
      try {
        if ( ! checkIvyXmlFile(ivyXmlFile) ) {
          failures = true;
          errors++;
        }
      } catch (Exception e) {
        throw new BuildException("Exception reading file " + ivyXmlFile.getPath(), e);
      }
      checked++;
    }
    log("Checking for orphans in " + centralizedVersionsFile.getName(), verboseLevel);
    for (Map.Entry<String,Boolean> entry : referencedCoordinateKeys.entrySet()) {
      String coordinateKey = entry.getKey();
      boolean isReferenced = entry.getValue();
      if ( ! isReferenced) {
        log("ORPHAN coordinate key '" + coordinateKey + "' in " + centralizedVersionsFile.getName()
            + " is not found in any " + IVY_XML_FILENAME + " file.",
            Project.MSG_ERR);
        failures = true;
        errors++;
      }
    }

    log(String.format(Locale.ROOT, "Checked that %s has lexically sorted "
        + "'/org/name' keys and no duplicates or orphans, and scanned %d %s "
        + "file(s) for rev=\"${/org/name}\" format (in %.2fs.), %d error(s).",
        centralizedVersionsFile.getName(), checked, IVY_XML_FILENAME, 
        (System.currentTimeMillis() - start) / 1000.0, errors),
        errors > 0 ? Project.MSG_ERR : Project.MSG_INFO);

    if (failures) {
      throw new BuildException("Lib versions check failed. Check the logs.");
    }
  }

  /**
   * Returns true if the "/org/name" coordinate keys in ivy-versions.properties
   * are lexically sorted and are not duplicates.
   */
  private boolean verifySortedCentralizedVersionsFile() {
    log("Checking for lexically sorted non-duplicated '/org/name' keys in: " + centralizedVersionsFile, verboseLevel);
    final InputStream stream;
    try {
      stream = new FileInputStream(centralizedVersionsFile);
    } catch (FileNotFoundException e) {
      throw new BuildException("Centralized versions file does not exist: "
          + centralizedVersionsFile.getPath());
    }
    // Properties files are encoded as Latin-1
    final Reader reader = new InputStreamReader(stream, Charset.forName("ISO-8859-1"));
    final BufferedReader bufferedReader = new BufferedReader(reader);
    
    String line = null;
    String currentKey = null;
    String previousKey = null;
    try {
      while (null != (line = readLogicalPropertiesLine(bufferedReader))) {
        final Matcher keyMatcher = COORDINATE_KEY_PATTERN.matcher(line);
        if ( ! keyMatcher.matches()) {
          continue; // Ignore keys that don't look like "/org/name"
        }
        currentKey = keyMatcher.group(1);
        if (null != previousKey) {
          int comparison = currentKey.compareTo(previousKey);
          if (0 == comparison) {
            log("DUPLICATE coordinate key '" + currentKey + "' in " + centralizedVersionsFile.getName(),
                Project.MSG_ERR);
            failures = true;
          } else if (comparison < 0) {
            log("OUT-OF-ORDER coordinate key '" + currentKey + "' in " + centralizedVersionsFile.getName(),
                Project.MSG_ERR);
            failures = true;
          }
        }
        referencedCoordinateKeys.put(currentKey, false);
        previousKey = currentKey;
      }
    } catch (IOException e) {
      throw new BuildException("Exception reading centralized versions file: " 
          + centralizedVersionsFile.getPath(), e);
    } finally {
      try { reader.close(); } catch (IOException e) { }
    }
    return ! failures;
  }

  /**
   * Builds up logical {@link java.util.Properties} lines, composed of one non-blank,
   * non-comment initial line, either:
   * 
   * 1. without a non-escaped trailing slash; or
   * 2. with a non-escaped trailing slash, followed by
   *    zero or more lines with a non-escaped trailing slash, followed by
   *    one or more lines without a non-escaped trailing slash
   *
   * All leading non-escaped whitespace and trailing non-escaped whitespace +
   * non-escaped slash are trimmed from each line before concatenating.
   * 
   * After composing the logical line, escaped characters are un-escaped.
   * 
   * null is returned if there are no lines left to read. 
   */
  private String readLogicalPropertiesLine(BufferedReader reader) throws IOException {
    final StringBuilder logicalLine = new StringBuilder();
    String line;
    do {
      line = reader.readLine();
      if (null == line) { 
        return null;
      }
    } while (BLANK_OR_COMMENT_LINE_PATTERN.matcher(line).matches());

    Matcher backslashMatcher = TRAILING_BACKSLASH_PATTERN.matcher(line); 
    // Check for a non-escaped backslash
    if (backslashMatcher.find() && 1 == (backslashMatcher.group(1).length() % 2)) {
      final Matcher firstLineMatcher = TRAILING_WHITESPACE_BACKSLASH_PATTERN.matcher(line);
      if (firstLineMatcher.matches()) {
        logicalLine.append(firstLineMatcher.group(1)); // trim trailing backslash and any preceding whitespace
      }
      line = reader.readLine();
      while (null != line
             && (backslashMatcher = TRAILING_BACKSLASH_PATTERN.matcher(line)).find()
             && 1 == (backslashMatcher.group(1).length() % 2)) {
        // Trim leading whitespace, the trailing backslash and any preceding whitespace
        final Matcher goodStuffMatcher = WHITESPACE_GOODSTUFF_WHITESPACE_BACKSLASH_PATTERN.matcher(line);
        if (goodStuffMatcher.matches()) {
          logicalLine.append(goodStuffMatcher.group(1));
        }
        line = reader.readLine();
      }
      if (null != line) {
        // line can't have a non-escaped trailing backslash
        final Matcher leadingWhitespaceMatcher = LEADING_WHITESPACE_PATTERN.matcher(line);
        if (leadingWhitespaceMatcher.matches()) {
          line = leadingWhitespaceMatcher.group(1); // trim leading whitespace
        }
        logicalLine.append(line);
      }
    } else {
      logicalLine.append(line);
    }
    // trim non-escaped leading whitespace
    final Matcher leadingWhitespaceMatcher = LEADING_WHITESPACE_PATTERN.matcher(logicalLine);
    final CharSequence leadingWhitespaceStripped = leadingWhitespaceMatcher.matches()
                                                 ? leadingWhitespaceMatcher.group(1)
                                                 : logicalLine;

    // unescape all chars in the logical line
    StringBuilder output = new StringBuilder();
    final int numChars = leadingWhitespaceStripped.length();
    for (int pos = 0 ; pos < numChars - 1 ; ++pos) {
      char ch = leadingWhitespaceStripped.charAt(pos);
      if (ch == '\\') {
        ch = leadingWhitespaceStripped.charAt(++pos); 
      }
      output.append(ch);
    }
    if (numChars > 0) {
      output.append(leadingWhitespaceStripped.charAt(numChars - 1));
    }

    return output.toString();
  }

  /**
   * Check a single ivy.xml file for dependencies' versions in rev="${/org/name}"
   * format.  Returns false if problems are found, true otherwise.
   */
  private boolean checkIvyXmlFile(File ivyXmlFile)
      throws ParserConfigurationException, SAXException, IOException {
    log("Scanning: " + ivyXmlFile.getPath(), verboseLevel);
    XMLReader xmlReader = XMLReaderFactory.createXMLReader();
    DependencyRevChecker revChecker = new DependencyRevChecker(ivyXmlFile); 
    xmlReader.setContentHandler(revChecker);
    xmlReader.setErrorHandler(revChecker);
    xmlReader.parse(new InputSource(ivyXmlFile.getAbsolutePath()));
    return ! revChecker.fail;
  }

  private class DependencyRevChecker extends DefaultHandler {
    private final File ivyXmlFile;
    private final Stack<String> tags = new Stack<String>();
    
    public boolean fail = false;

    public DependencyRevChecker(File ivyXmlFile) {
      this.ivyXmlFile = ivyXmlFile;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
      if (localName.equals("dependency") && insideDependenciesTag()) {
        String org = attributes.getValue("org");
        boolean foundAllAttributes = true;
        if (null == org) {
          log("MISSING 'org' attribute on <dependency> in " + ivyXmlFile.getPath(), Project.MSG_ERR);
          fail = true;
          foundAllAttributes = false;
        }
        String name = attributes.getValue("name");
        if (null == name) {
          log("MISSING 'name' attribute on <dependency> in " + ivyXmlFile.getPath(), Project.MSG_ERR);
          fail = true;
          foundAllAttributes = false;
        }
        String rev = attributes.getValue("rev");
        if (null == rev) {
          log("MISSING 'rev' attribute on <dependency> in " + ivyXmlFile.getPath(), Project.MSG_ERR);
          fail = true;
          foundAllAttributes = false;
        }
        if (foundAllAttributes) {
          String coordinateKey = "/" + org + '/' + name;
          String expectedRev = "${" + coordinateKey + '}';
          if ( ! rev.equals(expectedRev)) {
            log("BAD <dependency> 'rev' attribute value '" + rev + "' - expected '" + expectedRev + "'"
                + " in " + ivyXmlFile.getPath(), Project.MSG_ERR);
            fail = true;
          }
          if ( ! referencedCoordinateKeys.containsKey(coordinateKey)) {
            log("MISSING key '" + coordinateKey + "' in " + centralizedVersionsFile.getPath(), Project.MSG_ERR);
            fail = true;
          }
          referencedCoordinateKeys.put(coordinateKey, true);
        }
      }
      tags.push(localName);
    }

    @Override
    public void endElement (String uri, String localName, String qName) throws SAXException {
      tags.pop();
    }

    private boolean insideDependenciesTag() {
      return tags.size() == 2 && tags.get(0).equals("ivy-module") && tags.get(1).equals("dependencies");
    }
  }
}
