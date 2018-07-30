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

package org.apache.lucene.luke.models.tools;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.luke.models.LukeModel;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.util.IndexUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;

public final class IndexToolsImpl extends LukeModel implements IndexTools {

  private final boolean useCompound;

  private final boolean keepAllCommits;


  @SuppressWarnings("rawtypes")
  private static final Class[] presetFieldClasses = new Class[]{
      TextField.class, StringField.class,
      IntPoint.class, LongPoint.class, FloatPoint.class, DoublePoint.class,
      SortedDocValuesField.class, SortedSetDocValuesField.class,
      NumericDocValuesField.class, SortedNumericDocValuesField.class,
      StoredField.class
  };

  /**
   * Constructs an IndexToolsImpl that holds given {@link Directory}.
   *
   * @param dir - the index directory
   * @param useCompound - if true, compound file format is used
   * @param keepAllCommits - if true, all commit points are reserved
   */
  IndexToolsImpl(@Nonnull Directory dir, boolean useCompound, boolean keepAllCommits) {
    super(dir);
    this.useCompound = useCompound;
    this.keepAllCommits = keepAllCommits;
  }

  /**
   * Constructs an IndexToolsImpl that holds given {@link IndexReader}.
   *
   * @param reader - the index reader
   * @param useCompound - if true, compound file format is used
   * @param keepAllCommits - if true, all commit points are reserved
   */
  public IndexToolsImpl(@Nonnull IndexReader reader, boolean useCompound, boolean keepAllCommits) {
    super(reader);
    this.useCompound = useCompound;
    this.keepAllCommits = keepAllCommits;
  }

  @Override
  public void optimize(boolean expunge, int maxNumSegments, PrintStream ps) {
    if (reader instanceof DirectoryReader) {
      Directory dir = ((DirectoryReader) reader).directory();
      try (IndexWriter writer = IndexUtils.createWriter(dir, null, useCompound, keepAllCommits, ps)) {
        IndexUtils.optimizeIndex(writer, expunge, maxNumSegments);
      } catch (IOException e) {
        throw new LukeException("Failed to optimize index", e);
      }
    } else {
      throw new LukeException("Current reader is not a DirectoryReader.");
    }
  }

  @Override
  public CheckIndex.Status checkIndex(PrintStream ps) {
    try {
      if (dir != null) {
        return IndexUtils.checkIndex(dir, ps);
      } else if (reader instanceof DirectoryReader) {
        Directory dir = ((DirectoryReader) reader).directory();
        return IndexUtils.checkIndex(dir, ps);
      } else {
        throw new IllegalStateException("Directory is not set.");
      }
    } catch (Exception e) {
      throw new LukeException("Failed to check index.", e);
    }
  }

  @Override
  public void repairIndex(CheckIndex.Status st, PrintStream ps) {
    try {
      if (dir != null) {
        IndexUtils.tryRepairIndex(dir, st, ps);
      } else {
        throw new IllegalStateException("Directory is not set.");
      }
    } catch (Exception e) {
      throw new LukeException("Failed to repair index.", e);
    }
  }

  @Override
  public void addDocument(Document doc, @Nullable Analyzer analyzer) {
    if (reader instanceof DirectoryReader) {
      Directory dir = ((DirectoryReader) reader).directory();
      try (IndexWriter writer = IndexUtils.createWriter(dir, analyzer, useCompound, keepAllCommits)) {
        writer.addDocument(doc);
        writer.commit();
      } catch (IOException e) {
        throw new LukeException("Failed to add document", e);
      }
    } else {
      throw new LukeException("Current reader is not an instance of DirectoryReader.");
    }
  }

  @Override
  public void deleteDocuments(@Nonnull Query query) {
    if (reader instanceof DirectoryReader) {
      Directory dir = ((DirectoryReader) reader).directory();
      try (IndexWriter writer = IndexUtils.createWriter(dir, null, useCompound, keepAllCommits)) {
        writer.deleteDocuments(query);
        writer.commit();
      } catch (IOException e) {
        throw new LukeException("Failed to add document", e);
      }
    } else {
      throw new LukeException("Current reader is not an instance of DirectoryReader.");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Class<? extends Field>> getPresetFields() {
    return Arrays.asList(presetFieldClasses);
  }
}
