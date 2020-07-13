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
package org.apache.solr.handler.dataimport;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.solr.util.DateMathParser;

/**
 * <p>
 * An {@link EntityProcessor} instance which can stream file names found in a given base
 * directory matching patterns and returning rows containing file information.
 * </p>
 * <p>
 * It supports querying a give base directory by matching:
 * <ul>
 * <li>regular expressions to file names</li>
 * <li>excluding certain files based on regular expression</li>
 * <li>last modification date (newer or older than a given date or time)</li>
 * <li>size (bigger or smaller than size given in bytes)</li>
 * <li>recursively iterating through sub-directories</li>
 * </ul>
 * Its output can be used along with {@link FileDataSource} to read from files in file
 * systems.
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 1.3
 * @see Pattern
 */
public class FileListEntityProcessor extends EntityProcessorBase {
  /**
   * A regex pattern to identify files given in data-config.xml after resolving any variables 
   */
  protected String fileName;

  /**
   * The baseDir given in data-config.xml after resolving any variables
   */
  protected String baseDir;

  /**
   * A Regex pattern of excluded file names as given in data-config.xml after resolving any variables
   */
  protected String excludes;

  /**
   * The newerThan given in data-config as a {@link java.util.Date}
   * <p>
   * <b>Note: </b> This variable is resolved just-in-time in the {@link #nextRow()} method.
   * </p>
   */
  protected Date newerThan;

  /**
   * The newerThan given in data-config as a {@link java.util.Date}
   */
  protected Date olderThan;

  /**
   * The biggerThan given in data-config as a long value
   * <p>
   * <b>Note: </b> This variable is resolved just-in-time in the {@link #nextRow()} method.
   * </p>
   */
  protected long biggerThan = -1;

  /**
   * The smallerThan given in data-config as a long value
   * <p>
   * <b>Note: </b> This variable is resolved just-in-time in the {@link #nextRow()} method.
   * </p>
   */
  protected long smallerThan = -1;

  /**
   * The recursive given in data-config. Default value is false.
   */
  protected boolean recursive = false;

  private Pattern fileNamePattern, excludesPattern;

  @Override
  public void init(Context context) {
    super.init(context);
    fileName = context.getEntityAttribute(FILE_NAME);
    if (fileName != null) {
      fileName = context.replaceTokens(fileName);
      fileNamePattern = Pattern.compile(fileName);
    }
    baseDir = context.getEntityAttribute(BASE_DIR);
    if (baseDir == null)
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "'baseDir' is a required attribute");
    baseDir = context.replaceTokens(baseDir);
    File dir = new File(baseDir);
    if (!dir.isDirectory())
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "'baseDir' value: " + baseDir + " is not a directory");

    String r = context.getEntityAttribute(RECURSIVE);
    if (r != null)
      recursive = Boolean.parseBoolean(r);
    excludes = context.getEntityAttribute(EXCLUDES);
    if (excludes != null) {
      excludes = context.replaceTokens(excludes);
      excludesPattern = Pattern.compile(excludes);
    }
  }

  /**
   * Get the Date object corresponding to the given string.
   *
   * @param dateStr the date string. It can be a DateMath string or it may have a evaluator function
   * @return a Date instance corresponding to the input string
   */
  private Date getDate(String dateStr) {
    if (dateStr == null)
      return null;

    Matcher m = PLACE_HOLDER_PATTERN.matcher(dateStr);
    if (m.find()) {
      Object o = context.resolve(m.group(1));
      if (o instanceof Date)  return (Date)o;
      dateStr = (String) o;
    } else  {
      dateStr = context.replaceTokens(dateStr);
    }
    m = Evaluator.IN_SINGLE_QUOTES.matcher(dateStr);
    if (m.find()) {
      String expr = m.group(1);
      //TODO refactor DateMathParser.parseMath a bit to have a static method for this logic.
      if (expr.startsWith("NOW")) {
        expr = expr.substring("NOW".length());
      }
      try {
        // DWS TODO: is this TimeZone the right default for us?  Deserves explanation if so.
        return new DateMathParser(TimeZone.getDefault()).parseMath(expr);
      } catch (ParseException exp) {
        throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                "Invalid expression for date", exp);
      }
    }
    try {
      return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT).parse(dateStr);
    } catch (ParseException exp) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "Invalid expression for date", exp);
    }
  }

  /**
   * Get the Long value for the given string after resolving any evaluator or variable.
   *
   * @param sizeStr the size as a string
   * @return the Long value corresponding to the given string
   */
  private Long getSize(String sizeStr)  {
    if (sizeStr == null)
      return null;

    Matcher m = PLACE_HOLDER_PATTERN.matcher(sizeStr);
    if (m.find()) {
      Object o = context.resolve(m.group(1));
      if (o instanceof Number) {
        Number number = (Number) o;
        return number.longValue();
      }
      sizeStr = (String) o;
    } else  {
      sizeStr = context.replaceTokens(sizeStr);
    }

    return Long.parseLong(sizeStr);
  }

  @Override
  public Map<String,Object> nextRow() {
    if (rowIterator != null)
      return getNext();

    final Path dir = Paths.get(baseDir);

    String dateStr = context.getEntityAttribute(NEWER_THAN);
    newerThan = getDate(dateStr);
    dateStr = context.getEntityAttribute(OLDER_THAN);
    olderThan = getDate(dateStr);
    String biggerThanStr = context.getEntityAttribute(BIGGER_THAN);
    if (biggerThanStr != null)
      biggerThan = getSize(biggerThanStr);
    String smallerThanStr = context.getEntityAttribute(SMALLER_THAN);
    if (smallerThanStr != null)
      smallerThan = getSize(smallerThanStr);

    final Stream<Map<String,Object>> fileDetails = getFilesStream(dir);
    rowIterator = fileDetails.iterator();

    return getNext();
  }

  private Stream<Path> getFolderFiles(final Path dir) {
    try {
      return Files.list(dir).flatMap(path -> {
        if (Files.isDirectory(path)) {
          if (recursive)
            return getFolderFiles(path);
          return Stream.empty();
        }
        return Stream.of(path);
      }).filter(this::matchesFilename);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean matchesFilename(final Path path) {
    final String name = path.getFileName().toString();
    return fileNamePattern == null || fileNamePattern.matcher(name).find()
        && (excludesPattern == null || excludesPattern.matcher(name).find());
  }

  private Stream<Map<String,Object>> getFilesStream(final Path dir) {
    return getFolderFiles(dir).flatMap(path -> {
      try {
        final long size = Files.size(path);
        if (!matchesSize(size))
          return Stream.empty();

        final Date lastModified = new Date(Files.getLastModifiedTime(path).toMillis());
        if (!matchesDate(lastModified))
          return Stream.empty();

        final Map<String,Object> details = new HashMap<>();

        details.put(DIR, path.getParent().toAbsolutePath().toString());
        details.put(FILE, path.getFileName().toString());
        details.put(ABSOLUTE_FILE, path.toAbsolutePath().toString());
        details.put(SIZE, size);
        details.put(LAST_MODIFIED, lastModified);

        return Stream.of(details);

      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    });
  }

  private boolean matchesSize(final long size) {
    return (biggerThan == -1 || size > biggerThan) && (smallerThan == -1 || size < smallerThan);
  }

  private boolean matchesDate(final Date lastModified) {
    return (olderThan == null || lastModified.before(olderThan))
        && (newerThan == null || lastModified.after(newerThan));
  }

  public static final Pattern PLACE_HOLDER_PATTERN = Pattern
          .compile("\\$\\{(.*?)\\}");

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
