package org.apache.lucene.facet.enhancements;

import java.io.IOException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.enhancements.EnhancementsPayloadIterator;
import org.apache.lucene.facet.enhancements.association.AssociationEnhancement;
import org.apache.lucene.facet.enhancements.params.EnhancementsIndexingParams;
import org.apache.lucene.facet.example.association.AssociationIndexer;
import org.apache.lucene.facet.example.association.AssociationUtils;
import org.apache.lucene.facet.search.DrillDown;
import org.apache.lucene.facet.taxonomy.CategoryPath;

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

public class EnhancementsPayloadIteratorTest extends LuceneTestCase {

  private static Directory indexDir;
  private static Directory taxoDir;
  private static EnhancementsIndexingParams indexingParams;
  private static AssociationEnhancement associationEnhancement;

  @BeforeClass
  public static void buildAssociationIndex() throws Exception {
    // create Directories for the search index and for the taxonomy index
    indexDir = newDirectory();
    taxoDir = newDirectory();

    // index the sample documents
    if (VERBOSE) {
      System.out.println("index the sample documents...");
    }
    AssociationIndexer.index(indexDir, taxoDir);

    indexingParams = AssociationUtils.assocIndexingParams;
    associationEnhancement = (AssociationEnhancement) indexingParams
        .getCategoryEnhancements().get(0);
  }

  @Test
  public void testFullIterator() throws IOException {
    IndexReader indexReader = DirectoryReader.open(indexDir);
    Term term = DrillDown.term(indexingParams, new CategoryPath("tags", "lucene"));
    EnhancementsPayloadIterator iterator = new EnhancementsPayloadIterator(
        indexingParams.getCategoryEnhancements(), indexReader, term);
    assertTrue("Unexpected failure of init()", iterator.init());
    assertTrue("Missing instance of tags/lucene in doc 0", iterator.setdoc(0));
    int assoc = (Integer) iterator.getCategoryData(associationEnhancement);
    assertEquals("Unexpected association value for tags/lucene in doc 0", 3, assoc, 1E-5);
    assertTrue("Missing instance of tags/lucene in doc 1", iterator.setdoc(1));
    assoc = (Integer) iterator.getCategoryData(associationEnhancement);
    assertEquals("Unexpected association value for tags/lucene in doc 1", 1, assoc, 1E-5);
    indexReader.close();
  }

  @Test
  public void testEmptyIterator() throws IOException {
    IndexReader indexReader = DirectoryReader.open(indexDir);
    Term term = DrillDown.term(indexingParams, new CategoryPath("root","a", "f2"));
    EnhancementsPayloadIterator iterator = new EnhancementsPayloadIterator(
        indexingParams.getCategoryEnhancements(), indexReader, term);
    assertTrue("Unexpected failure of init()", iterator.init());
    assertFalse("Unexpected payload for root/a/f2 in doc 0", iterator.setdoc(0));
    assertFalse("Unexpected instance of root/a/f2 in doc 1", iterator.setdoc(1));
    indexReader.close();
  }

  @Test
  public void testPartialIterator() throws IOException {
    IndexReader indexReader = DirectoryReader.open(indexDir);
    Term term = DrillDown.term(indexingParams, new CategoryPath("genre","software"));
    EnhancementsPayloadIterator iterator = new EnhancementsPayloadIterator(
        indexingParams.getCategoryEnhancements(), indexReader, term);
    assertTrue("Unexpected failure of init()", iterator.init());
    assertFalse("Unexpected payload for genre/computing in doc 0", iterator.setdoc(0));
    assertTrue("Missing instance of genre/computing in doc 1", iterator.setdoc(1));
    float assoc = Float.intBitsToFloat((Integer) iterator
        .getCategoryData(associationEnhancement));
    assertEquals("Unexpected association value for genre/computing in doc 1", 0.34f, assoc, 0.001);
    indexReader.close();
  }

  @AfterClass
  public static void closeDirectories() throws IOException {
    indexDir.close();
    indexDir = null;
    taxoDir.close();
    taxoDir = null;
  }
}
