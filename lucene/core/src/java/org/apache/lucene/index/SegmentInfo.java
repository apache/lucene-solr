package org.apache.lucene.index;

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


import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene3x.Lucene3xSegmentInfoFormat;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.TrackingDirectoryWrapper;

/**
 * Information about a segment such as it's name, directory, and files related
 * to the segment.
 *
 * @lucene.experimental
 */
public final class SegmentInfo {
  
  // TODO: remove these from this class, for now this is the representation
  /** Used by some member fields to mean not present (e.g.,
   *  norms, deletions). */
  public static final int NO = -1;          // e.g. no norms; no deletes;

  /** Used by some member fields to mean present (e.g.,
   *  norms, deletions). */
  public static final int YES = 1;          // e.g. have norms; have deletes;

  /** Unique segment name in the directory. */
  public final String name;

  private int docCount;         // number of docs in seg

  /** Where this segment resides. */
  public final Directory dir;

  private boolean isCompoundFile;

  private Codec codec;

  private Map<String,String> diagnostics;
  
  private Map<String,String> attributes;

  // Tracks the Lucene version this segment was created with, since 3.1. Null
  // indicates an older than 3.0 index, and it's used to detect a too old index.
  // The format expected is "x.y" - "2.x" for pre-3.0 indexes (or null), and
  // specific versions afterwards ("3.0", "3.1" etc.).
  // see Constants.LUCENE_MAIN_VERSION.
  private String version;

  void setDiagnostics(Map<String, String> diagnostics) {
    this.diagnostics = diagnostics;
  }

  /** Returns diagnostics saved into the segment when it was
   *  written. */
  public Map<String, String> getDiagnostics() {
    return diagnostics;
  }

  /**
   * Construct a new complete SegmentInfo instance from input.
   * <p>Note: this is public only to allow access from
   * the codecs package.</p>
   */
  public SegmentInfo(Directory dir, String version, String name, int docCount, 
                     boolean isCompoundFile, Codec codec, Map<String,String> diagnostics, Map<String,String> attributes) {
    assert !(dir instanceof TrackingDirectoryWrapper);
    this.dir = dir;
    this.version = version;
    this.name = name;
    this.docCount = docCount;
    this.isCompoundFile = isCompoundFile;
    this.codec = codec;
    this.diagnostics = diagnostics;
    this.attributes = attributes;
  }

  /**
   * @deprecated separate norms are not supported in >= 4.0
   */
  @Deprecated
  boolean hasSeparateNorms() {
    return getAttribute(Lucene3xSegmentInfoFormat.NORMGEN_KEY) != null;
  }

  /**
   * Mark whether this segment is stored as a compound file.
   *
   * @param isCompoundFile true if this is a compound file;
   * else, false
   */
  void setUseCompoundFile(boolean isCompoundFile) {
    this.isCompoundFile = isCompoundFile;
  }
  
  /**
   * Returns true if this segment is stored as a compound
   * file; else, false.
   */
  public boolean getUseCompoundFile() {
    return isCompoundFile;
  }

  /** Can only be called once. */
  public void setCodec(Codec codec) {
    assert this.codec == null;
    if (codec == null) {
      throw new IllegalArgumentException("segmentCodecs must be non-null");
    }
    this.codec = codec;
  }

  /** Return {@link Codec} that wrote this segment. */
  public Codec getCodec() {
    return codec;
  }

  /** Returns number of documents in this segment (deletions
   *  are not taken into account). */
  public int getDocCount() {
    if (this.docCount == -1) {
      throw new IllegalStateException("docCount isn't set yet");
    }
    return docCount;
  }

  // NOTE: leave package private
  void setDocCount(int docCount) {
    if (this.docCount != -1) {
      throw new IllegalStateException("docCount was already set");
    }
    this.docCount = docCount;
  }

  /** Return all files referenced by this SegmentInfo. */
  public Set<String> files() {
    if (setFiles == null) {
      throw new IllegalStateException("files were not computed yet");
    }
    return Collections.unmodifiableSet(setFiles);
  }

  @Override
  public String toString() {
    return toString(dir, 0);
  }

  /** Used for debugging.  Format may suddenly change.
   *
   *  <p>Current format looks like
   *  <code>_a(3.1):c45/4</code>, which means the segment's
   *  name is <code>_a</code>; it was created with Lucene 3.1 (or
   *  '?' if it's unknown); it's using compound file
   *  format (would be <code>C</code> if not compound); it
   *  has 45 documents; it has 4 deletions (this part is
   *  left off when there are no deletions).</p>
   */
  public String toString(Directory dir, int delCount) {

    StringBuilder s = new StringBuilder();
    s.append(name).append('(').append(version == null ? "?" : version).append(')').append(':');
    char cfs = getUseCompoundFile() ? 'c' : 'C';
    s.append(cfs);

    if (this.dir != dir) {
      s.append('x');
    }
    s.append(docCount);

    if (delCount != 0) {
      s.append('/').append(delCount);
    }

    // TODO: we could append toString of attributes() here?

    return s.toString();
  }

  /** We consider another SegmentInfo instance equal if it
   *  has the same dir and same name. */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj instanceof SegmentInfo) {
      final SegmentInfo other = (SegmentInfo) obj;
      return other.dir == dir && other.name.equals(name);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return dir.hashCode() + name.hashCode();
  }

  /**
   * Used by DefaultSegmentInfosReader to upgrade a 3.0 segment to record its
   * version is "3.0". This method can be removed when we're not required to
   * support 3x indexes anymore, e.g. in 5.0.
   * <p>
   * <b>NOTE:</b> this method is used for internal purposes only - you should
   * not modify the version of a SegmentInfo, or it may result in unexpected
   * exceptions thrown when you attempt to open the index.
   *
   * @lucene.internal
   */
  public void setVersion(String version) {
    this.version = version;
  }

  /** Returns the version of the code which wrote the segment. */
  public String getVersion() {
    return version;
  }

  private Set<String> setFiles;

  /** Sets the files written for this segment. */
  public void setFiles(Set<String> files) {
    checkFileNames(files);
    setFiles = files;
  }

  /** Add these files to the set of files written for this
   *  segment. */
  public void addFiles(Collection<String> files) {
    checkFileNames(files);
    setFiles.addAll(files);
  }

  /** Add this file to the set of files written for this
   *  segment. */
  public void addFile(String file) {
    checkFileNames(Collections.singleton(file));
    setFiles.add(file);
  }
  
  private void checkFileNames(Collection<String> files) {
    Matcher m = IndexFileNames.CODEC_FILE_PATTERN.matcher("");
    for (String file : files) {
      m.reset(file);
      if (!m.matches()) {
        throw new IllegalArgumentException("invalid codec filename '" + file + "', must match: " + IndexFileNames.CODEC_FILE_PATTERN.pattern());
      }
    }
  }
    
  /**
   * Get a codec attribute value, or null if it does not exist
   */
  public String getAttribute(String key) {
    if (attributes == null) {
      return null;
    } else {
      return attributes.get(key);
    }
  }
  
  /**
   * Puts a codec attribute value.
   * <p>
   * This is a key-value mapping for the field that the codec can use
   * to store additional metadata, and will be available to the codec
   * when reading the segment via {@link #getAttribute(String)}
   * <p>
   * If a value already exists for the field, it will be replaced with 
   * the new value.
   */
  public String putAttribute(String key, String value) {
    if (attributes == null) {
      attributes = new HashMap<String,String>();
    }
    return attributes.put(key, value);
  }
  
  /**
   * Returns the internal codec attributes map.
   *
   * @return internal codec attributes map. May be null if no mappings exist.
   */
  public Map<String,String> attributes() {
    return attributes;
  }
}
