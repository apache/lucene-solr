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

package org.apache.lucene.luke.models.overview;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.luke.models.LukeModel;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.util.IndexUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class OverviewImpl extends LukeModel implements Overview {

  private final String indexPath;

  private final TermCounts termCounts;

  private final TopTerms topTerms;

  /**
   * Constructs an OverviewImpl that holds the given {@link IndexReader}.
   *
   * @param reader - the index reader
   * @param indexPath - the (root) index directory path
   * @throws LukeException - if an internal error is occurred when accessing index
   */
  public OverviewImpl(@Nonnull IndexReader reader, @Nonnull String indexPath) {
    super(reader);
    this.indexPath = indexPath;
    try {
      this.termCounts = new TermCounts(reader);
    } catch (IOException e) {
      throw new LukeException("An error occurred when collecting term statistics.");
    }
    this.topTerms = new TopTerms(reader);
  }

  @Override
  public String getIndexPath() {
    return indexPath;
  }

  @Override
  public int getNumFields() {
    return IndexUtils.getFieldInfos(reader).size();
  }

  @Override
  public int getNumDocuments() {
    return reader.numDocs();
  }

  @Override
  public long getNumTerms() {
    return termCounts.numTerms();
  }

  @Override
  public boolean hasDeletions() {
    return reader.hasDeletions();
  }

  @Override
  public int getNumDeletedDocs() {
    return reader.numDeletedDocs();
  }

  @Override
  public Optional<Boolean> isOptimized() {
    if (commit != null) {
      return Optional.of(commit.getSegmentCount() == 1);
    }
    return Optional.empty();
  }

  @Override
  public Optional<Long> getIndexVersion() {
    if (reader instanceof DirectoryReader) {
      return Optional.of(((DirectoryReader) reader).getVersion());
    }
    return Optional.empty();
  }

  @Override
  public Optional<String> getIndexFormat() {
    if (dir == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(IndexUtils.getIndexFormat(dir));
    } catch (IOException e) {
      throw new LukeException("Index format not available.", e);
    }
  }

  @Override
  public Optional<String> getDirImpl() {
    if (dir == null) {
      return Optional.empty();
    }
    return Optional.of(dir.getClass().getName());
  }

  @Override
  public Optional<String> getCommitDescription() {
    if (commit == null) {
      return Optional.empty();
    }
    return Optional.of(
        commit.getSegmentsFileName()
            + " (generation=" + commit.getGeneration()
            + ", segs=" + commit.getSegmentCount() + ")");
  }

  @Override
  public Optional<String> getCommitUserData() {
    if (commit == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(IndexUtils.getCommitUserData(commit));
    } catch (IOException e) {
      throw new LukeException("Commit user data not available.", e);
    }
  }

  @Override
  public Map<String, Long> getSortedTermCounts(@Nullable TermCountsOrder order) {
    if (order == null) {
      order = TermCountsOrder.COUNT_DESC;
    }
    return termCounts.sortedTermCounts(order);
  }

  @Override
  public List<TermStats> getTopTerms(@Nonnull String field, int numTerms) {
    if (numTerms < 0) {
      throw new IllegalArgumentException(String.format("'numTerms' must be a positive integer: %d is not accepted.", numTerms));
    }
    try {
      return topTerms.getTopTerms(field, numTerms);
    } catch (Exception e) {
      throw new LukeException(String.format("Top terms for field %s not available.", field), e);
    }
  }

}
