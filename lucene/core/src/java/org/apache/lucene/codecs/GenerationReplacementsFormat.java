package org.apache.lucene.codecs;

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

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.lucene.index.FieldGenerationReplacements;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

/**
 * Format for field replacements of certain generation
 * 
 * @lucene.experimental
 */
public abstract class GenerationReplacementsFormat {
  
  /** Extension of generation replacements vectors */
  static final String FIELD_GENERATION_REPLACEMENT_EXTENSION = "fgr";
  
  /**
   * Sole constructor. (For invocation by subclass constructors, typically
   * implicit.)
   */
  protected GenerationReplacementsFormat() {}
  
  /**
   * Read field generation replacements. If no replacements exist return
   * {@code null}.
   */
  public FieldGenerationReplacements readGenerationReplacements(String field,
      SegmentInfoPerCommit info, IOContext context) throws IOException {
    FieldGenerationReplacements reps = null;
    
    for (long gen = 1; gen <= info.getUpdateGen(); gen++) {
      final String fileName = IndexFileNames.segmentFileName(
          IndexFileNames.fileNameFromGeneration(info.info.name, "", gen, true),
          field, FIELD_GENERATION_REPLACEMENT_EXTENSION);
      if (info.info.dir.fileExists(fileName)) {
        final FieldGenerationReplacements 
        newGeneration = internalReadGeneration(info.info.dir, fileName, context);
        if (reps == null) {
          reps = newGeneration;
        } else {
          reps.merge(newGeneration);
        }
      }
    }

    return reps;
  }
  
  private FieldGenerationReplacements internalReadGeneration(Directory dir,
      String fileName, IOContext context) throws IOException {
    IndexInput input = dir.openInput(fileName, context);
    
    boolean success = false;
    try {
      final FieldGenerationReplacements persistedGeneration = readPersistedGeneration(input);
      success = true;
      return persistedGeneration;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(input);
      } else {
        input.close();
      }
    }
  }
  
  /**
   * Read persisted field generation replacements from a given input.
   */
  protected abstract FieldGenerationReplacements readPersistedGeneration(
      IndexInput input) throws IOException;
  
  /**
   * Persist field generation replacements. Use
   * {@link SegmentInfoPerCommit#getNextUpdateGen()} to determine the generation
   * of the deletes file you should write to.
   */
  public void writeGenerationReplacement(String field,
      FieldGenerationReplacements reps, Directory dir,
      SegmentInfoPerCommit info, IOContext context,
      Set<String> generationReplacementFilenames) throws IOException {
    if (reps == null) {
      // nothing new to write
      return;
    }
    
    final String nameWithGeneration = IndexFileNames.fileNameFromGeneration(
        info.info.name, "", info.getNextUpdateGen(), true);
    final String fileName = IndexFileNames.segmentFileName(nameWithGeneration,
        field, FIELD_GENERATION_REPLACEMENT_EXTENSION);
    
    final IndexOutput output = dir.createOutput(fileName, context);
    boolean success = false;
    try {
      persistGeneration(reps, output);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(output);
        dir.deleteFile(fileName);
      } else {
        generationReplacementFilenames.add(fileName);
        output.close();
      }
    }
  }
  
  /**
   * Persist field generation replacements to a given output.
   */
  protected abstract void persistGeneration(FieldGenerationReplacements reps,
      IndexOutput output) throws IOException;
  
  /**
   * Records all files in use by this {@link SegmentInfoPerCommit} into the
   * files argument.
   */
  public void files(SegmentInfoPerCommit info, Directory dir,
      Collection<String> files) throws IOException {
    Pattern pattern = Pattern.compile(info.info.name + "[\\S]*."
        + FIELD_GENERATION_REPLACEMENT_EXTENSION);
    final String[] dirFiles = dir.listAll();
    for (int i = 0; i < dirFiles.length; i++) {
      if (pattern.matcher(dirFiles[i]).matches()) {
        files.add(dirFiles[i]);
      }
    }
  }
}
