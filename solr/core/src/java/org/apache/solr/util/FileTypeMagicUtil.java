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

package org.apache.solr.util;

import com.j256.simplemagic.ContentInfo;
import com.j256.simplemagic.ContentInfoUtil;
import com.j256.simplemagic.ContentType;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.apache.solr.common.SolrException;

/** Utility class to guess the mime type of file based on its magic number. */
public class FileTypeMagicUtil implements ContentInfoUtil.ErrorCallBack {
  private final ContentInfoUtil util;
  private static final Set<String> SKIP_FOLDERS = new HashSet<>(Arrays.asList(".", ".."));

  public static FileTypeMagicUtil INSTANCE = new FileTypeMagicUtil();

  FileTypeMagicUtil() {
    try {
      util = new ContentInfoUtil("/magic/executables", this);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error parsing magic file", e);
    }
  }

  /**
   * Asserts that an entire configset folder is legal to upload.
   *
   * @param confPath the path to the folder
   * @throws SolrException if an illegal file is found in the folder structure
   */
  public static void assertConfigSetFolderLegal(Path confPath) throws IOException {
    Files.walkFileTree(confPath, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
        // Read first 100 bytes of the file to determine the mime type
        if (FileTypeMagicUtil.isFileForbiddenInConfigset(file)) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              String.format(Locale.ROOT, "Not uploading file %s to configset, as it matched the MAGIC signature of a forbidden mime type %s",
                  file, FileTypeMagicUtil.INSTANCE.guessMimeType(file)));
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
        if (SKIP_FOLDERS.contains(dir.getFileName().toString())) return FileVisitResult.SKIP_SUBTREE;

        return FileVisitResult.CONTINUE;
      }
    });
  }

  /**
   * Guess the mime type of file based on its magic number.
   *
   * @param file file to check
   * @return string with content-type or "application/octet-stream" if unknown
   */
  public String guessMimeType(Path file) {
    try {
      return guessTypeFallbackToOctetStream(util.findMatch(file.toFile()));
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * Guess the mime type of file based on its magic number.
   *
   * @param stream input stream of the file
   * @return string with content-type or "application/octet-stream" if unknown
   */
  String guessMimeType(InputStream stream) {
    try {
      return guessTypeFallbackToOctetStream(util.findMatch(stream));
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * Guess the mime type of file bytes based on its magic number.
   *
   * @param bytes the first bytes at start of the file
   * @return string with content-type or "application/octet-stream" if unknown
   */
  public String guessMimeType(byte[] bytes) {
    return guessTypeFallbackToOctetStream(util.findMatch(bytes));
  }

  @Override
  public void error(String line, String details, Exception e) {
    throw new SolrException(
        SolrException.ErrorCode.SERVER_ERROR,
        String.format(Locale.ROOT, "%s: %s", line, details),
        e);
  }

  /**
   * Determine forbidden file type based on magic bytes matching of the file itself. Forbidden types
   * are:
   *
   * <ul>
   *   <li><code>application/x-java-applet</code>: java class file
   *   <li><code>application/zip</code>: jar or zip archives
   *   <li><code>application/x-tar</code>: tar archives
   *   <li><code>text/x-shellscript</code>: shell or bash script
   * </ul>
   *
   * @param file file to check
   * @return true if file is among the forbidden mime-types
   */
  public static boolean isFileForbiddenInConfigset(Path file) {
    return forbiddenTypes.contains(FileTypeMagicUtil.INSTANCE.guessMimeType(file));
  }

  /**
   * Determine forbidden file type based on magic bytes matching of the file itself. Forbidden types
   * are:
   *
   * <ul>
   *   <li><code>application/x-java-applet</code>: java class file
   *   <li><code>application/zip</code>: jar or zip archives
   *   <li><code>application/x-tar</code>: tar archives
   *   <li><code>text/x-shellscript</code>: shell or bash script
   * </ul>
   *
   * @param fileStream stream from the file content
   * @return true if file is among the forbidden mime-types
   */
  static boolean isFileForbiddenInConfigset(InputStream fileStream) {
    return forbiddenTypes.contains(FileTypeMagicUtil.INSTANCE.guessMimeType(fileStream));
  }

  /**
   * Determine forbidden file type based on magic bytes matching of the first bytes of the file.
   *
   * @param bytes byte array of the file content
   * @return true if file is among the forbidden mime-types
   */
  public static boolean isFileForbiddenInConfigset(byte[] bytes) {
    if (bytes == null || bytes.length == 0)
      return false; // A ZK znode may be a folder with no content
    return forbiddenTypes.contains(FileTypeMagicUtil.INSTANCE.guessMimeType(bytes));
  }

  private static final Set<String> forbiddenTypes =
      new HashSet<>(
          Arrays.asList(
              System.getProperty(
                      "solr.configset.upload.mimetypes.forbidden",
                      "application/x-java-applet,application/zip,application/x-tar,text/x-shellscript")
                  .split(",")));

  private String guessTypeFallbackToOctetStream(ContentInfo contentInfo) {
    if (contentInfo == null) {
      return ContentType.OTHER.getMimeType();
    } else {
      return contentInfo.getContentType().getMimeType();
    }
  }
}
