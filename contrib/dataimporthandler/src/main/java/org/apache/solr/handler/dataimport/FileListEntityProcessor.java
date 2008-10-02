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
package org.apache.solr.handler.dataimport;

import java.io.File;
import java.io.FilenameFilter;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * An EntityProcessor instance which can stream file names found in a given base
 * directory matching patterns and returning rows containing file information.
 * </p>
 * <p/>
 * <p>
 * It supports querying a give base directory by matching:
 * <ul>
 * <li>regular expressions to file names</li>
 * <li>excluding certain files based on regular expression</li>
 * <li>last modification date (newer or older than a given date or time)</li>
 * <li>size (bigger or smaller than size given in bytes)</li>
 * <li>recursively iterating through sub-directories</li>
 * </ul>
 * Its output can be used along with FileDataSource to read from files in file
 * systems.
 * </p>
 * <p/>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p/>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class FileListEntityProcessor extends EntityProcessorBase {
  private String fileName, baseDir, excludes;

  private Date newerThan, olderThan;

  private long biggerThan = -1, smallerThan = -1;

  private boolean recursive = false;

  private Pattern fileNamePattern, excludesPattern;

  public void init(Context context) {
    super.init(context);
    fileName = context.getEntityAttribute(FILE_NAME);
    if (fileName != null) {
      fileName = resolver.replaceTokens(fileName);
      fileNamePattern = Pattern.compile(fileName);
    }
    baseDir = context.getEntityAttribute(BASE_DIR);
    if (baseDir == null)
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "'baseDir' is a required attribute");
    baseDir = resolver.replaceTokens(baseDir);
    File dir = new File(baseDir);
    if (!dir.isDirectory())
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "'baseDir' value: " + baseDir + " is not a directory");

    String r = context.getEntityAttribute(RECURSIVE);
    if (r != null)
      recursive = Boolean.parseBoolean(r);
    excludes = context.getEntityAttribute(EXCLUDES);
    if (excludes != null)
      excludes = resolver.replaceTokens(excludes);
    if (excludes != null)
      excludesPattern = Pattern.compile(excludes);

  }

  private Date getDate(String dateStr) {
    if (dateStr == null)
      return null;

    Matcher m = PLACE_HOLDER_PATTERN.matcher(dateStr);
    if (m.find()) {
      return (Date) resolver.resolve(dateStr);
    }
    m = EvaluatorBag.IN_SINGLE_QUOTES.matcher(dateStr);
    if (m.find()) {
      String expr = null;
      expr = m.group(1).replaceAll("NOW", "");
      try {
        return EvaluatorBag.dateMathParser.parseMath(expr);
      } catch (ParseException exp) {
        throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                "Invalid expression for date", exp);
      }
    }
    try {
      return DataImporter.DATE_TIME_FORMAT.parse(dateStr);
    } catch (ParseException exp) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "Invalid expression for date", exp);
    }
  }

  public Map<String, Object> nextRow() {
    if (rowIterator != null)
      return getAndApplyTrans();
    List<Map<String, Object>> fileDetails = new ArrayList<Map<String, Object>>();
    File dir = new File(baseDir);

    String dateStr = context.getEntityAttribute(NEWER_THAN);
    newerThan = getDate(dateStr);
    dateStr = context.getEntityAttribute(OLDER_THAN);
    olderThan = getDate(dateStr);

    getFolderFiles(dir, fileDetails);
    rowIterator = fileDetails.iterator();
    return getAndApplyTrans();
  }

  private Map<String, Object> getAndApplyTrans() {
    if (rowcache != null)
      return getFromRowCache();
    while (true) {
      Map<String, Object> r = getNext();
      if (r == null)
        return null;
      r = applyTransformer(r);
      if (r != null)
        return r;
    }
  }

  private void getFolderFiles(File dir,
                              final List<Map<String, Object>> fileDetails) {
    dir.list(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        if (fileNamePattern == null) {
          addDetails(fileDetails, dir, name);
          return false;
        }
        if (fileNamePattern.matcher(name).find()) {
          if (excludesPattern != null && excludesPattern.matcher(name).find())
            return false;
          addDetails(fileDetails, dir, name);
        }

        return false;
      }
    });
  }

  private void addDetails(List<Map<String, Object>> files, File dir, String name) {
    Map<String, Object> details = new HashMap<String, Object>();
    File aFile = new File(dir, name);
    if (aFile.isDirectory()) {
      if (!recursive)
        return;
      getFolderFiles(aFile, files);
      return;
    }
    long sz = aFile.length();
    Date lastModified = new Date(aFile.lastModified());
    if (biggerThan != -1 && sz <= biggerThan)
      return;
    if (smallerThan != -1 && sz >= smallerThan)
      return;
    if (olderThan != null && lastModified.after(olderThan))
      return;
    if (newerThan != null && lastModified.before(newerThan))
      return;
    details.put(DIR, dir.getAbsolutePath());
    details.put(FILE, name);
    details.put(ABSOLUTE_FILE, aFile.getAbsolutePath());
    details.put(SIZE, sz);
    details.put(LAST_MODIFIED, lastModified);
    files.add(details);
  }

  public static final Pattern PLACE_HOLDER_PATTERN = Pattern
          .compile("\\$\\{.*?\\}");

  public static final String DIR = "fileDir";

  public static final String FILE = "file";

  public static final String ABSOLUTE_FILE = "fileAbsolutePath";

  public static final String SIZE = "fileSize";

  public static final String LAST_MODIFIED = "fileLastModified";

  public static final String FILE_NAME = "fileName";

  public static final String BASE_DIR = "baseDir";

  public static final String EXCLUDES = "excludes";

  public static final String NEWER_THAN = "newerThan";

  public static final String OLDER_THAN = "olderThan";

  public static final String BIGGER_THAN = "biggerThan";

  public static final String SMALLER_THAN = "smallerThan";

  public static final String RECURSIVE = "recursive";

}
