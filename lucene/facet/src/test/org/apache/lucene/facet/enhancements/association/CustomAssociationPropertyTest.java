package org.apache.lucene.facet.enhancements.association;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.enhancements.EnhancementsDocumentBuilder;
import org.apache.lucene.facet.enhancements.params.DefaultEnhancementsIndexingParams;
import org.apache.lucene.facet.enhancements.params.EnhancementsIndexingParams;
import org.apache.lucene.facet.index.CategoryContainer;
import org.apache.lucene.facet.index.attributes.CategoryAttributeImpl;
import org.apache.lucene.facet.index.attributes.CategoryProperty;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;

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

public class CustomAssociationPropertyTest extends LuceneTestCase {

  @Test
  public void testCustomProperty() throws Exception {
    class CustomProperty extends AssociationIntProperty {
      public CustomProperty(int value) {
        super(value);
      }
      @Override
      public void merge(CategoryProperty other) {
        throw new UnsupportedOperationException();
      }
    }

    final int NUM_CATEGORIES = 10;
    EnhancementsIndexingParams iParams = new DefaultEnhancementsIndexingParams(
        new AssociationEnhancement());

    Directory iDir = newDirectory();
    Directory tDir = newDirectory();
    
    RandomIndexWriter w = new RandomIndexWriter(random(), iDir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.KEYWORD, false)));
    DirectoryTaxonomyWriter taxoW = new DirectoryTaxonomyWriter(tDir);
    
    CategoryContainer cc = new CategoryContainer();
    EnhancementsDocumentBuilder builder = new EnhancementsDocumentBuilder(taxoW, iParams);
    for (int i = 1; i <= NUM_CATEGORIES; i++) {
      CategoryAttributeImpl ca = new CategoryAttributeImpl(new CategoryPath(Integer.toString(i)));
      ca.addProperty(new CustomProperty(i));
      
      cc.addCategory(ca);
    }
    builder.setCategories(cc);
    w.addDocument(builder.build(new Document()));
    taxoW.close();
    IndexReader reader = w.getReader();
    w.close();
    
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(tDir);
    String field = iParams.getCategoryListParams(new CategoryPath("0")).getTerm().field();
    AssociationsPayloadIterator api = new AssociationsPayloadIterator(reader, field);

    api.setNextDoc(0);

    boolean flag = false;
    for (int i = 1; i <= NUM_CATEGORIES; i++) {
      int ordinal = taxo.getOrdinal(new CategoryPath(Integer.toString(i)));
      flag = true;
      long association = api.getAssociation(ordinal);
      assertTrue("Association expected for ordinal "+ordinal+" but none was found",
          association <= Integer.MAX_VALUE);
      
      assertEquals("Wrong association value for category '"+ i+"'", i, (int)association);
    }
    
    assertTrue("No categories found for doc #0", flag);
    
    reader.close();
    taxo.close();
    iDir.close();
    tDir.close();
  }
}
