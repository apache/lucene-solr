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
package org.apache.lucene.replicator;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.replicator.IndexAndTaxonomyRevision.SnapshotDirectoryTaxonomyWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

public class IndexAndTaxonomyRevisionTest extends ReplicatorTestCase {
  
  private Document newDocument(TaxonomyWriter taxoWriter) throws IOException {
    FacetsConfig config = new FacetsConfig();
    Document doc = new Document();
    doc.add(new FacetField("A", "1"));
    return config.build(taxoWriter, doc);
  }
  
  @Test
  public void testNoCommit() throws Exception {
    Directory indexDir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(null);
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
    IndexWriter indexWriter = new IndexWriter(indexDir, conf);
    
    Directory taxoDir = newDirectory();
    SnapshotDirectoryTaxonomyWriter taxoWriter = new SnapshotDirectoryTaxonomyWriter(taxoDir);
    try {
      assertNotNull(new IndexAndTaxonomyRevision(indexWriter, taxoWriter));
      fail("should have failed when there are no commits to snapshot");
    } catch (IllegalStateException e) {
      // expected
    } finally {
      indexWriter.close();
      IOUtils.close(taxoWriter, taxoDir, indexDir);
    }
  }
  
  @Test
  public void testRevisionRelease() throws Exception {
    Directory indexDir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(null);
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
    IndexWriter indexWriter = new IndexWriter(indexDir, conf);
    
    Directory taxoDir = newDirectory();
    SnapshotDirectoryTaxonomyWriter taxoWriter = new SnapshotDirectoryTaxonomyWriter(taxoDir);
    // we look to see that certain files are deleted:
    if (indexDir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)indexDir).setEnableVirusScanner(false);
    }
    try {
      indexWriter.addDocument(newDocument(taxoWriter));
      indexWriter.commit();
      taxoWriter.commit();
      Revision rev1 = new IndexAndTaxonomyRevision(indexWriter, taxoWriter);
      // releasing that revision should not delete the files
      rev1.release();
      assertTrue(slowFileExists(indexDir, IndexFileNames.SEGMENTS + "_1"));
      assertTrue(slowFileExists(taxoDir, IndexFileNames.SEGMENTS + "_1"));
      
      rev1 = new IndexAndTaxonomyRevision(indexWriter, taxoWriter); // create revision again, so the files are snapshotted
      indexWriter.addDocument(newDocument(taxoWriter));
      indexWriter.commit();
      taxoWriter.commit();
      assertNotNull(new IndexAndTaxonomyRevision(indexWriter, taxoWriter));
      rev1.release(); // this release should trigger the delete of segments_1
      assertFalse(slowFileExists(indexDir, IndexFileNames.SEGMENTS + "_1"));
      indexWriter.close();
    } finally {
      IOUtils.close(indexWriter, taxoWriter, taxoDir, indexDir);
      if (indexDir instanceof MockDirectoryWrapper) {
        // set back to on for other tests
        ((MockDirectoryWrapper)indexDir).setEnableVirusScanner(true);
      }
    }
  }
  
  @Test
  public void testSegmentsFileLast() throws Exception {
    Directory indexDir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(null);
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
    IndexWriter indexWriter = new IndexWriter(indexDir, conf);
    
    Directory taxoDir = newDirectory();
    SnapshotDirectoryTaxonomyWriter taxoWriter = new SnapshotDirectoryTaxonomyWriter(taxoDir);
    try {
      indexWriter.addDocument(newDocument(taxoWriter));
      indexWriter.commit();
      taxoWriter.commit();
      Revision rev = new IndexAndTaxonomyRevision(indexWriter, taxoWriter);
      Map<String,List<RevisionFile>> sourceFiles = rev.getSourceFiles();
      assertEquals(2, sourceFiles.size());
      for (List<RevisionFile> files : sourceFiles.values()) {
        String lastFile = files.get(files.size() - 1).fileName;
        assertTrue(lastFile.startsWith(IndexFileNames.SEGMENTS));
      }
      indexWriter.close();
    } finally {
      IOUtils.close(indexWriter, taxoWriter, taxoDir, indexDir);
    }
  }
  
  @Test
  public void testOpen() throws Exception {
    Directory indexDir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(null);
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
    IndexWriter indexWriter = new IndexWriter(indexDir, conf);
    
    Directory taxoDir = newDirectory();
    SnapshotDirectoryTaxonomyWriter taxoWriter = new SnapshotDirectoryTaxonomyWriter(taxoDir);
    try {
      indexWriter.addDocument(newDocument(taxoWriter));
      indexWriter.commit();
      taxoWriter.commit();
      Revision rev = new IndexAndTaxonomyRevision(indexWriter, taxoWriter);
      for (Entry<String,List<RevisionFile>> e : rev.getSourceFiles().entrySet()) {
        String source = e.getKey();
        @SuppressWarnings("resource") // silly, both directories are closed in the end
        Directory dir = source.equals(IndexAndTaxonomyRevision.INDEX_SOURCE) ? indexDir : taxoDir;
        for (RevisionFile file : e.getValue()) {
          IndexInput src = dir.openInput(file.fileName, IOContext.READONCE);
          InputStream in = rev.open(source, file.fileName);
          assertEquals(src.length(), in.available());
          byte[] srcBytes = new byte[(int) src.length()];
          byte[] inBytes = new byte[(int) src.length()];
          int offset = 0;
          if (random().nextBoolean()) {
            int skip = random().nextInt(10);
            if (skip >= src.length()) {
              skip = 0;
            }
            in.skip(skip);
            src.seek(skip);
            offset = skip;
          }
          src.readBytes(srcBytes, offset, srcBytes.length - offset);
          in.read(inBytes, offset, inBytes.length - offset);
          assertArrayEquals(srcBytes, inBytes);
          IOUtils.close(src, in);
        }
      }
      indexWriter.close();
    } finally {
      IOUtils.close(indexWriter, taxoWriter, taxoDir, indexDir);
    }
  }
  
}
