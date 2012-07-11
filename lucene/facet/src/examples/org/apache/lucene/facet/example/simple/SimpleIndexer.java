package org.apache.lucene.facet.example.simple;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;

import org.apache.lucene.facet.example.ExampleUtils;
import org.apache.lucene.facet.index.CategoryDocumentBuilder;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
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

/**
 * Sample indexer creates an index, and adds to it sample documents and facets.
 * 
 * @lucene.experimental
 */
public class SimpleIndexer {

  /**
   * Create an index, and adds to it sample documents and facets.
   * @param indexDir Directory in which the index should be created.
   * @param taxoDir Directory in which the taxonomy index should be created.
   * @throws Exception on error (no detailed exception handling here for sample simplicity
   */
  public static void index (Directory indexDir, Directory taxoDir) throws Exception {

    // create and open an index writer
    IndexWriter iw = new IndexWriter(indexDir, new IndexWriterConfig(ExampleUtils.EXAMPLE_VER, SimpleUtils.analyzer));

    // create and open a taxonomy writer
    TaxonomyWriter taxo = new DirectoryTaxonomyWriter(taxoDir, OpenMode.CREATE);

    // loop over  sample documents 
    int nDocsAdded = 0;
    int nFacetsAdded = 0;
    for (int docNum=0; docNum<SimpleUtils.docTexts.length; docNum++) {

      // obtain the sample facets for current document
      List<CategoryPath> facetList = Arrays.asList(SimpleUtils.categories[docNum]);

      // we do not alter indexing parameters!  
      // a category document builder will add the categories to a document once build() is called
      CategoryDocumentBuilder categoryDocBuilder = new CategoryDocumentBuilder(taxo).setCategoryPaths(facetList);

      // create a plain Lucene document and add some regular Lucene fields to it 
      Document doc = new Document();
      doc.add(new TextField(SimpleUtils.TITLE, SimpleUtils.docTitles[docNum], Field.Store.YES));
      doc.add(new TextField(SimpleUtils.TEXT, SimpleUtils.docTexts[docNum], Field.Store.NO));

      // invoke the category document builder for adding categories to the document and,
      // as required, to the taxonomy index 
      categoryDocBuilder.build(doc);

      // finally add the document to the index
      iw.addDocument(doc);

      nDocsAdded ++;
      nFacetsAdded += facetList.size(); 
    }

    // commit changes.
    // we commit changes to the taxonomy index prior to committing them to the search index.
    // this is important, so that all facets referred to by documents in the search index 
    // will indeed exist in the taxonomy index.
    taxo.commit();
    iw.commit();

    // close the taxonomy index and the index - all modifications are 
    // now safely in the provided directories: indexDir and taxoDir.
    taxo.close();
    iw.close();

    ExampleUtils.log("Indexed "+nDocsAdded+" documents with overall "+nFacetsAdded+" facets.");
  }
  
}
