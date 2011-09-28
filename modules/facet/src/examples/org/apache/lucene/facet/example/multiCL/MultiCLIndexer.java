package org.apache.lucene.facet.example.multiCL;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import org.apache.lucene.DocumentBuilder;
import org.apache.lucene.facet.example.ExampleUtils;
import org.apache.lucene.facet.example.simple.SimpleUtils;
import org.apache.lucene.facet.index.CategoryDocumentBuilder;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.index.params.PerDimensionIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.lucene.LuceneTaxonomyWriter;

/**
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
 * Sample indexer creates an index, and adds to it sample documents and facets 
 * with multiple CategoryLists specified for different facets, so there are different
 * category lists for different facets.
 * 
 * @lucene.experimental
 */
public class MultiCLIndexer {

  // Number of documents to index
  public static int NUM_DOCS = 100;
  // Number of facets to add per document
  public static int NUM_FACETS_PER_DOC = 10;
  // Number of tokens in title
  public static int TITLE_LENGTH = 5;
  // Number of tokens in text
  public static int TEXT_LENGTH = 100;
  
  // Lorum ipsum to use as content - this will be tokenized and used for document
  // titles/text.
  static String words = "Sed ut perspiciatis unde omnis iste natus error sit "
      + "voluptatem accusantium doloremque laudantium totam rem aperiam "
      + "eaque ipsa quae ab illo inventore veritatis et quasi architecto "
      + "beatae vitae dicta sunt explicabo Nemo enim ipsam voluptatem "
      + "quia voluptas sit aspernatur aut odit aut fugit sed quia consequuntur "
      + "magni dolores eos qui ratione voluptatem sequi nesciunt Neque porro "
      + "quisquam est qui dolorem ipsum quia dolor sit amet consectetur adipisci velit "
      + "sed quia non numquam eius modi tempora incidunt ut labore et dolore "
      + "magnam aliquam quaerat voluptatem Ut enim ad minima veniam "
      + "quis nostrum exercitationem ullam corporis suscipit laboriosam "
      + "nisi ut aliquid ex ea commodi consequatur? Quis autem vel eum iure"
      + "reprehenderit qui in ea voluptate velit esse quam nihil molestiae "
      + "consequatur vel illum qui dolorem eum fugiat quo voluptas nulla pariatur";
  // PerDimensionIndexingParams for multiple category lists
  public static PerDimensionIndexingParams MULTI_IPARAMS = new PerDimensionIndexingParams();

  // Initialize PerDimensionIndexingParams
  static {
    MULTI_IPARAMS.addCategoryListParams(new CategoryPath("0"),
        new CategoryListParams(new Term("$Digits", "Zero")));
    MULTI_IPARAMS.addCategoryListParams(new CategoryPath("1"),
        new CategoryListParams(new Term("$Digits", "One")));
    MULTI_IPARAMS.addCategoryListParams(new CategoryPath("2"),
        new CategoryListParams(new Term("$Digits", "Two")));
    MULTI_IPARAMS.addCategoryListParams(new CategoryPath("3"),
        new CategoryListParams(new Term("$Digits", "Three")));
    MULTI_IPARAMS.addCategoryListParams(new CategoryPath("4"),
        new CategoryListParams(new Term("$Digits", "Four")));
    MULTI_IPARAMS.addCategoryListParams(new CategoryPath("5"),
        new CategoryListParams(new Term("$Digits", "Five")));
  }
  
  /**
   * Create an index, and adds to it sample documents and facets.
   * @param indexDir Directory in which the index should be created.
   * @param taxoDir Directory in which the taxonomy index should be created.
   * @throws Exception on error (no detailed exception handling here for sample simplicity
   */
  public static void index(Directory indexDir, Directory taxoDir)
      throws Exception {

    Random random = new Random(2003);

    String[] docTitles = new String[NUM_DOCS];
    String[] docTexts = new String[NUM_DOCS];
    CategoryPath[][] cPaths = new CategoryPath[NUM_DOCS][NUM_FACETS_PER_DOC];

    String[] tokens = words.split(" ");
    for (int docNum = 0; docNum < NUM_DOCS; docNum++) {
      String title = "";
      String text = "";
      for (int j = 0; j < TITLE_LENGTH; j++) {
        title = title + tokens[random.nextInt(tokens.length)] + " ";
      }
      docTitles[docNum] = title;

      for (int j = 0; j < TEXT_LENGTH; j++) {
        text = text + tokens[random.nextInt(tokens.length)] + " ";
      }
      docTexts[docNum] = text;

      for (int facetNum = 0; facetNum < NUM_FACETS_PER_DOC; facetNum++) {
        cPaths[docNum][facetNum] = new CategoryPath(Integer
            .toString(random.nextInt(7)), Integer.toString(random.nextInt(10)));
      }
    }
    index(indexDir, taxoDir, MULTI_IPARAMS, docTitles, docTexts, cPaths);
  }
  
  /**
   * More advanced method for specifying custom indexing params, doc texts, 
   * doc titles and category paths.
   */
  public static void index(Directory indexDir, Directory taxoDir,
      FacetIndexingParams iParams, String[] docTitles,
      String[] docTexts, CategoryPath[][] cPaths) throws Exception {
    // create and open an index writer
    IndexWriter iw = new IndexWriter(indexDir, new IndexWriterConfig(
        ExampleUtils.EXAMPLE_VER, SimpleUtils.analyzer).setOpenMode(OpenMode.CREATE));
    // create and open a taxonomy writer
    LuceneTaxonomyWriter taxo = new LuceneTaxonomyWriter(taxoDir, OpenMode.CREATE);
    index(iw, taxo, iParams, docTitles, docTexts, cPaths);
  }
  
  /**
   * More advanced method for specifying custom indexing params, doc texts, 
   * doc titles and category paths.
   * <p>
   * Create an index, and adds to it sample documents and facets.
   * @throws Exception
   *             on error (no detailed exception handling here for sample
   *             simplicity
   */
  public static void index(IndexWriter iw, LuceneTaxonomyWriter taxo,
      FacetIndexingParams iParams, String[] docTitles,
      String[] docTexts, CategoryPath[][] cPaths) throws Exception {

    // loop over sample documents
    int nDocsAdded = 0;
    int nFacetsAdded = 0;
    for (int docNum = 0; docNum < SimpleUtils.docTexts.length; docNum++) {
      List<CategoryPath> facetList = Arrays.asList(cPaths[docNum]);

      // we do not alter indexing parameters!
      // a category document builder will add the categories to a document
      // once build() is called
      DocumentBuilder categoryDocBuilder = new CategoryDocumentBuilder(
          taxo, iParams).setCategoryPaths(facetList);

      // create a plain Lucene document and add some regular Lucene fields
      // to it
      Document doc = new Document();
      doc.add(new Field(SimpleUtils.TITLE, docTitles[docNum], TextField.TYPE_STORED));
      doc.add(new TextField(SimpleUtils.TEXT, docTexts[docNum]));

      // finally add the document to the index
      categoryDocBuilder.build(doc);
      iw.addDocument(doc);
      
      nDocsAdded++;
      nFacetsAdded += facetList.size();
    }

    // commit changes.
    // we commit changes to the taxonomy index prior to committing them to
    // the search index.
    // this is important, so that all facets referred to by documents in the
    // search index
    // will indeed exist in the taxonomy index.
    taxo.commit();
    iw.commit();

    // close the taxonomy index and the index - all modifications are
    // now safely in the provided directories: indexDir and taxoDir.
    taxo.close();
    iw.close();

    ExampleUtils.log("Indexed " + nDocsAdded + " documents with overall "
        + nFacetsAdded + " facets.");
  }

  public static void main(String[] args) throws Exception {
    index(new RAMDirectory(), new RAMDirectory());
  }
  
}
