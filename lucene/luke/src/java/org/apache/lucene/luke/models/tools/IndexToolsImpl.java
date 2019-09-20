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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.LukeModel;
import org.apache.lucene.luke.models.util.IndexUtils;
import org.apache.lucene.luke.models.util.twentynewsgroups.Message;
import org.apache.lucene.luke.models.util.twentynewsgroups.MessageFilesParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/** Default implementation of {@link IndexTools} */
public final class IndexToolsImpl extends LukeModel implements IndexTools {

  private final boolean useCompound;

  private final boolean keepAllCommits;

  /**
   * Constructs an IndexToolsImpl that holds given {@link Directory}.
   *
   * @param dir - the index directory
   * @param useCompound - if true, compound file format is used
   * @param keepAllCommits - if true, all commit points are reserved
   */
  public IndexToolsImpl(Directory dir, boolean useCompound, boolean keepAllCommits) {
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
  public IndexToolsImpl(IndexReader reader, boolean useCompound, boolean keepAllCommits) {
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
  public void addDocument(Document doc, Analyzer analyzer) {
    Objects.requireNonNull(analyzer);

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
  public void deleteDocuments(Query query) {
    Objects.requireNonNull(query);

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
  public void createNewIndex() {
    createNewIndex(null);
  }

  @Override
  public void createNewIndex(String dataDir) {
    IndexWriter writer = null;
    try {
      if (dir == null || dir.listAll().length > 0) {
        // Directory is null or not empty
        throw new IllegalStateException();
      }

      writer = IndexUtils.createWriter(dir, Message.createLuceneAnalyzer(), useCompound, keepAllCommits);

      if (Objects.nonNull(dataDir)) {
        Path path = Paths.get(dataDir);
        MessageFilesParser parser = new MessageFilesParser(path);
        List<Message> messages = parser.parseAll();
        for (Message message : messages) {
          writer.addDocument(message.toLuceneDoc());
        }
      }

      writer.commit();
    } catch (IOException e) {
      throw new LukeException("Cannot create new index.", e);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {}
      }
    }
  }

  public String exportTerms(String destDir, String field, String delimiter) {
    String filename = "terms_" + field + "_" + System.currentTimeMillis() + ".out";
    Path path = Paths.get(destDir, filename);
    try {
      Terms terms = MultiTerms.getTerms(reader, field);
      if (terms == null) {
        throw new LukeException(String.format(Locale.US, "Field %s does not contain any terms to be exported", field));
      }
      try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("UTF-8"))) {
        TermsEnum termsEnum = terms.iterator();
        BytesRef term;
        while (!Thread.currentThread().isInterrupted() && (term = termsEnum.next()) != null) {
          writer.write(String.format(Locale.US, "%s%s%d\n", term.utf8ToString(), delimiter, +termsEnum.docFreq()));
        }
        return path.toString();
      }
    } catch (IOException e) {
      throw new LukeException("Terms file export for field [" + field + "] to file [" + filename + "] has failed.", e);
    }
  }
}
