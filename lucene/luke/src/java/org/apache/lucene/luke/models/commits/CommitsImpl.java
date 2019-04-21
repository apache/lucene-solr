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

package org.apache.lucene.luke.models.commits;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.LukeModel;
import org.apache.lucene.luke.util.LoggerFactory;
import org.apache.lucene.store.Directory;

/** Default implementation of {@link Commits} */
public final class CommitsImpl extends LukeModel implements Commits {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String indexPath;

  private final Map<Long, IndexCommit> commitMap;

  /**
   * Constructs a CommitsImpl that holds given {@link Directory}.
   *
   * @param dir - the index directory
   * @param indexPath - the path to index directory
   */
  public CommitsImpl(Directory dir, String indexPath) {
    super(dir);
    this.indexPath = indexPath;
    this.commitMap = initCommitMap();
  }

  /**
   * Constructs a CommitsImpl that holds the {@link Directory} wrapped in the given {@link DirectoryReader}.
   *
   * @param reader - the index reader
   * @param indexPath - the path to index directory
   */
  public CommitsImpl(DirectoryReader reader, String indexPath) {
    super(reader.directory());
    this.indexPath = indexPath;
    this.commitMap = initCommitMap();
  }

  private Map<Long, IndexCommit> initCommitMap() {
    try {
      List<IndexCommit> indexCommits = DirectoryReader.listCommits(dir);
      Map<Long, IndexCommit> map = new TreeMap<>();
      for (IndexCommit ic : indexCommits) {
        map.put(ic.getGeneration(), ic);
      }
      return map;
    } catch (IOException e) {
      throw new LukeException("Failed to get commits list.", e);
    }
  }

  @Override
  public List<Commit> listCommits() throws LukeException {
    List<Commit> commits = getCommitMap().values().stream()
        .map(Commit::of)
        .collect(Collectors.toList());
    Collections.reverse(commits);
    return commits;
  }

  @Override
  public Optional<Commit> getCommit(long commitGen) throws LukeException {
    IndexCommit ic = getCommitMap().get(commitGen);

    if (ic == null) {
      String msg = String.format(Locale.ENGLISH, "Commit generation %d not exists.", commitGen);
      log.warn(msg);
      return Optional.empty();
    }

    return Optional.of(Commit.of(ic));
  }

  @Override
  public List<File> getFiles(long commitGen) throws LukeException {
    IndexCommit ic = getCommitMap().get(commitGen);

    if (ic == null) {
      String msg = String.format(Locale.ENGLISH, "Commit generation %d not exists.", commitGen);
      log.warn(msg);
      return Collections.emptyList();
    }

    try {
      return ic.getFileNames().stream()
          .map(name -> File.of(indexPath, name))
          .sorted(Comparator.comparing(File::getFileName))
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new LukeException(String.format(Locale.ENGLISH, "Failed to load files for commit generation %d", commitGen), e);
    }
  }

  @Override
  public List<Segment> getSegments(long commitGen) throws LukeException {
    try {
      SegmentInfos infos = findSegmentInfos(commitGen);
      if (infos == null) {
        return Collections.emptyList();
      }

      return infos.asList().stream()
          .map(Segment::of)
          .sorted(Comparator.comparing(Segment::getName))
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new LukeException(String.format(Locale.ENGLISH, "Failed to load segment infos for commit generation %d", commitGen), e);
    }
  }

  @Override
  public Map<String, String> getSegmentAttributes(long commitGen, String name) throws LukeException {
    try {
      SegmentInfos infos = findSegmentInfos(commitGen);
      if (infos == null) {
        return Collections.emptyMap();
      }

      return infos.asList().stream()
          .filter(seg -> seg.info.name.equals(name))
          .findAny()
          .map(seg -> seg.info.getAttributes())
          .orElse(Collections.emptyMap());
    } catch (IOException e) {
      throw new LukeException(String.format(Locale.ENGLISH, "Failed to load segment infos for commit generation %d", commitGen), e);
    }
  }

  @Override
  public Map<String, String> getSegmentDiagnostics(long commitGen, String name) throws LukeException {
    try {
      SegmentInfos infos = findSegmentInfos(commitGen);
      if (infos == null) {
        return Collections.emptyMap();
      }

      return infos.asList().stream()
          .filter(seg -> seg.info.name.equals(name))
          .findAny()
          .map(seg -> seg.info.getDiagnostics())
          .orElse(Collections.emptyMap());
    } catch (IOException e) {
      throw new LukeException(String.format(Locale.ENGLISH, "Failed to load segment infos for commit generation %d", commitGen), e);
    }
  }

  @Override
  public Optional<Codec> getSegmentCodec(long commitGen, String name) throws LukeException {
    try {
      SegmentInfos infos = findSegmentInfos(commitGen);
      if (infos == null) {
        return Optional.empty();
      }

      return infos.asList().stream()
          .filter(seg -> seg.info.name.equals(name))
          .findAny()
          .map(seg -> seg.info.getCodec());
    } catch (IOException e) {
      throw new LukeException(String.format(Locale.ENGLISH, "Failed to load segment infos for commit generation %d", commitGen), e);
    }
  }

  private Map<Long, IndexCommit> getCommitMap() throws LukeException {
    if (dir == null) {
      return Collections.emptyMap();
    }
    return new TreeMap<>(commitMap);
  }

  private SegmentInfos findSegmentInfos(long commitGen) throws LukeException, IOException {
    IndexCommit ic = getCommitMap().get(commitGen);
    if (ic == null) {
      return null;
    }
    String segmentFile = ic.getSegmentsFileName();
    return SegmentInfos.readCommit(dir, segmentFile);
  }

  static String toDisplaySize(long size) {
    if (size < 1024) {
      return String.valueOf(size) + " B";
    } else if (size < 1048576) {
      return String.valueOf(size / 1024) + " KB";
    } else {
      return String.valueOf(size / 1048576) + " MB";
    }
  }
}
