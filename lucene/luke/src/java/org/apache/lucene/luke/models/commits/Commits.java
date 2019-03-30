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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.luke.models.LukeException;

/**
 * A dedicated interface for Luke's Commits tab.
 */
public interface Commits {

  /**
   * Returns commits that exists in this Directory.
   * @throws LukeException - if an internal error occurs when accessing index
   */
  List<Commit> listCommits();

  /**
   * Returns a commit of the specified generation.
   * @param commitGen - generation
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Optional<Commit> getCommit(long commitGen);

  /**
   * Returns index files for the specified generation.
   * @param commitGen - generation
   * @throws LukeException - if an internal error occurs when accessing index
   */
  List<File> getFiles(long commitGen);

  /**
   * Returns segments for the specified generation.
   * @param commitGen - generation
   * @throws LukeException - if an internal error occurs when accessing index
   */
  List<Segment> getSegments(long commitGen);

  /**
   * Returns internal codec attributes map for the specified segment.
   * @param commitGen - generation
   * @param name - segment name
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Map<String, String> getSegmentAttributes(long commitGen, String name);

  /**
   * Returns diagnotics for the specified segment.
   * @param commitGen - generation
   * @param name - segment name
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Map<String, String> getSegmentDiagnostics(long commitGen, String name);

  /**
   * Returns codec for the specified segment.
   * @param commitGen - generation
   * @param name - segment name
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Optional<Codec> getSegmentCodec(long commitGen, String name);
}
