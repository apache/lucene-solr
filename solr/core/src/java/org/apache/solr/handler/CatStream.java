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

package org.apache.solr.handler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.core.SolrCore;
import org.eclipse.jetty.io.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatStream extends TupleStream implements Expressible {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String commaDelimitedFilepaths;
  private final int maxLines; // -1 for no max

  private StreamContext context;
  private Path chroot;
  private Iterator<CrawlFile> allFilesToCrawl;

  private int linesReturned = 0;
  private CrawlFile currentFilePath;
  private LineIterator currentFileLines;

  public CatStream(StreamExpression expression, StreamFactory factory) throws IOException {
    this(factory.getValueOperand(expression, 0), factory.getIntOperand(expression, "maxLines", -1));
  }

  public CatStream(String commaDelimitedFilepaths, int maxLines) {
    if (commaDelimitedFilepaths == null) {
      throw new IllegalArgumentException("No filepaths provided to stream");
    }
    final String filepathsWithoutSurroundingQuotes = stripSurroundingQuotesIfTheyExist(commaDelimitedFilepaths);
    if (StringUtils.isEmpty(filepathsWithoutSurroundingQuotes)) {
      throw new IllegalArgumentException("No filepaths provided to stream");
    }

    this.commaDelimitedFilepaths = filepathsWithoutSurroundingQuotes;
    this.maxLines = maxLines;
  }

  private String stripSurroundingQuotesIfTheyExist(String value) {
    if (value.length() < 2) return value;
    if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))) {
      return value.substring(1, value.length() - 1);
    }

    return value;
  }

  @Override
  public void setStreamContext(StreamContext context) {
    this.context = context;
    Object solrCoreObj = context.get("solr-core");
    if (solrCoreObj == null || !(solrCoreObj instanceof SolrCore) ) {
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "StreamContext must have SolrCore in solr-core key");
    }
    final SolrCore core = (SolrCore) context.get("solr-core");

    this.chroot = core.getCoreContainer().getUserFilesPath();
    if (! Files.exists(chroot)) {
      throw new IllegalStateException(chroot + " directory used to load files must exist but could not be found!");
    }
  }

  @Override
  public List<TupleStream> children() {
    return new ArrayList<>();
  }

  @Override
  public void open() throws IOException {
    final List<CrawlFile> initialCrawlSeeds = validateAndSetFilepathsInSandbox();

    final List<CrawlFile> filesToCrawl = new ArrayList<>();
    for (CrawlFile crawlSeed: initialCrawlSeeds) {
      findReadableFiles(crawlSeed, filesToCrawl);
    }

    log.debug("Found files [{}] to stream from roots: [{}]", filesToCrawl, initialCrawlSeeds);
    this.allFilesToCrawl = filesToCrawl.iterator();
  }

  @Override
  public void close() throws IOException {}

  @Override
  public Tuple read() throws IOException {
    if (maxLines >= 0 && linesReturned >= maxLines) {
      closeCurrentFileIfSet();
      return Tuple.EOF();
    } else if (currentFileHasMoreLinesToRead()) {
      return fetchNextLineFromCurrentFile();
    } else if (advanceToNextFileWithData()) {
      return fetchNextLineFromCurrentFile();
    } else { // No more data
      closeCurrentFileIfSet();
      return Tuple.EOF();
    }
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    expression.addParameter("\"" + commaDelimitedFilepaths + "\"");
    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new StreamExplanation(getStreamNodeId().toString())
        .withFunctionName(factory.getFunctionName(this.getClass()))
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(Explanation.ExpressionType.STREAM_SOURCE)
        .withExpression(toExpression(factory).toString());
  }

  private List<CrawlFile> validateAndSetFilepathsInSandbox() {
    final List<CrawlFile> crawlSeeds = new ArrayList<>();
    for (String crawlRootStr : commaDelimitedFilepaths.split(",")) {
      Path crawlRootPath = chroot.resolve(crawlRootStr).normalize();
      if (! crawlRootPath.startsWith(chroot)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "file/directory to stream must be under " + chroot);
      }

      if (! Files.exists(crawlRootPath)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "file/directory to stream doesn't exist: " + crawlRootStr);
      }

      crawlSeeds.add(new CrawlFile(crawlRootStr, crawlRootPath));
    }

    return crawlSeeds;
  }

  private boolean advanceToNextFileWithData() throws IOException {
    while (allFilesToCrawl.hasNext()) {
      closeCurrentFileIfSet();
      currentFilePath = allFilesToCrawl.next();
      File currentFile = currentFilePath.absolutePath.toFile();
      if(currentFile.getName().endsWith(".gz")) {
        currentFileLines = new LineIterator(new InputStreamReader(new GZIPInputStream(new FileInputStream(currentFile)), "UTF-8"));
      } else {
        currentFileLines = FileUtils.lineIterator(currentFile, "UTF-8");
      }
      if (currentFileLines.hasNext()) return true;
    }

    return false;
  }

  @SuppressWarnings({"unchecked"})
  private Tuple fetchNextLineFromCurrentFile() {
    linesReturned++;

    return new Tuple(
        "file", currentFilePath.displayPath,
        "line", currentFileLines.next()
    );
  }

  private boolean currentFileHasMoreLinesToRead() {
    return currentFileLines != null && currentFileLines.hasNext();
  }

  private void closeCurrentFileIfSet() throws IOException {
    if (currentFilePath != null) {
      currentFileLines.close();
      currentFilePath = null;
      currentFileLines = null;
    }
  }

  private void findReadableFiles(CrawlFile seed, List<CrawlFile> foundFiles) {

    final Path entry = seed.absolutePath;

    // Skip over paths that don't exist or that are symbolic links
    if ((!Files.exists(entry)) || (!Files.isReadable(entry)) || Files.isSymbolicLink(entry)) {
      return;
    }

    // We already know that the path in question exists, is readable, and is in our sandbox
    if (Files.isRegularFile(entry)) {
      foundFiles.add(seed);
    } else if (Files.isDirectory(entry)) {
      try (Stream<Path> directoryContents = Files.list(entry)) {
        directoryContents.sorted().forEach(iPath -> {
          // debatable: should the separator be OS/file-system specific, or perhaps always "/" ?
          final String displayPathSeparator = iPath.getFileSystem().getSeparator();
          final String itemDisplayPath = seed.displayPath + displayPathSeparator + iPath.getFileName();
          findReadableFiles(new CrawlFile(itemDisplayPath, iPath), foundFiles);
        });
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }
  }

  // A pair of paths for a particular file to stream:
  // - absolute path for reading,
  // - display path to avoid leaking Solr node fs details in tuples (relative to chroot)
  public class CrawlFile {
    private final String displayPath;
    private final Path absolutePath;

    public CrawlFile(String displayPath, Path absolutePath) {
      this.displayPath = displayPath;
      this.absolutePath = absolutePath;
    }
  }
}

