package org.apache.lucene.facet.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.CategoryListParams.OrdinalPolicy;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.params.PerDimensionIndexingParams;
import org.apache.lucene.facet.params.PerDimensionOrdinalPolicy;
import org.apache.lucene.facet.search.CategoryListIterator;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.DrillDownQuery;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.junit.Test;

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

/** Tests facets index migration from payload to DocValues.*/
public class TestFacetsPayloadMigrationReader extends FacetTestCase {
  
  private static class PayloadFacetFields extends FacetFields {

    private static final class CountingListStream extends TokenStream {
      private final PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);
      private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
      private final Iterator<Entry<String,BytesRef>> categoriesData;
      
      CountingListStream(Map<String,BytesRef> categoriesData) {
        this.categoriesData = categoriesData.entrySet().iterator();
      }
      
      @Override
      public boolean incrementToken() throws IOException {
        if (!categoriesData.hasNext()) {
          return false;
        }
        
        Entry<String,BytesRef> entry = categoriesData.next();
        termAtt.setEmpty().append(FacetsPayloadMigrationReader.PAYLOAD_TERM_TEXT + entry.getKey());
        payloadAtt.setPayload(entry.getValue());
        return true;
      }
      
    }

    private static final FieldType COUNTING_LIST_PAYLOAD_TYPE = new FieldType();
    static {
      COUNTING_LIST_PAYLOAD_TYPE.setIndexed(true);
      COUNTING_LIST_PAYLOAD_TYPE.setTokenized(true);
      COUNTING_LIST_PAYLOAD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
      COUNTING_LIST_PAYLOAD_TYPE.setStored(false);
      COUNTING_LIST_PAYLOAD_TYPE.setOmitNorms(true);
      COUNTING_LIST_PAYLOAD_TYPE.freeze();
    }
    
    public PayloadFacetFields(TaxonomyWriter taxonomyWriter, FacetIndexingParams params) {
      super(taxonomyWriter, params);
    }

    @Override
    protected FieldType drillDownFieldType() {
      // Since the payload is indexed in the same field as the drill-down terms,
      // we must set IndexOptions to DOCS_AND_FREQS_AND_POSITIONS
      final FieldType type = new FieldType(TextField.TYPE_NOT_STORED);
      type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
      type.freeze();
      return type;
    }

    @Override
    protected void addCountingListData(Document doc, Map<String,BytesRef> categoriesData, String field) {
      CountingListStream ts = new CountingListStream(categoriesData);
      doc.add(new Field(field, ts, COUNTING_LIST_PAYLOAD_TYPE));
    }
  }

  private static final String[] DIMENSIONS = new String[] { "dim1", "dim2", "dim3.1", "dim3.2" };
  
  private HashMap<String,Integer> createIndex(Directory indexDir, Directory taxoDir, FacetIndexingParams fip) 
      throws Exception {
    Random random = random();
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    conf.setMaxBufferedDocs(2); // force few segments
    conf.setMergePolicy(NoMergePolicy.COMPOUND_FILES); // avoid merges so that we're left with few segments
    IndexWriter indexWriter = new IndexWriter(indexDir, conf);
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    
    FacetFields facetFields = new PayloadFacetFields(taxoWriter, fip);
    
    HashMap<String,Integer> expectedCounts = new HashMap<String,Integer>(DIMENSIONS.length);
    int numDocs = atLeast(10);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int numCategories = random.nextInt(3) + 1;
      ArrayList<CategoryPath> categories = new ArrayList<CategoryPath>(numCategories);
      HashSet<String> docDimensions = new HashSet<String>();
      while (numCategories-- > 0) {
        String dim = DIMENSIONS[random.nextInt(DIMENSIONS.length)];
        // we should only increment the expected count by 1 per document
        docDimensions.add(dim);
        categories.add(new CategoryPath(dim, Integer.toString(i), Integer.toString(numCategories)));
      }
      facetFields.addFields(doc, categories);
      doc.add(new StringField("docid", Integer.toString(i), Store.YES));
      doc.add(new TextField("foo", "content" + i, Store.YES));
      indexWriter.addDocument(doc);

      // update expected count per dimension
      for (String dim : docDimensions) {
        Integer val = expectedCounts.get(dim);
        if (val == null) {
          expectedCounts.put(dim, Integer.valueOf(1));
        } else {
          expectedCounts.put(dim, Integer.valueOf(val.intValue() + 1));
        }
      }
      
      if (random.nextDouble() < 0.2) { // add some documents that will be deleted
        doc = new Document();
        doc.add(new StringField("del", "key", Store.NO));
        facetFields.addFields(doc, Collections.singletonList(new CategoryPath("dummy")));
        indexWriter.addDocument(doc);
      }
    }
    
    indexWriter.commit();
    taxoWriter.commit();

    // delete the docs that were marked for deletion. note that the 'dummy'
    // category is not removed from the taxonomy, so must account for it when we
    // verify the migrated index.
    indexWriter.deleteDocuments(new Term("del", "key"));
    indexWriter.commit();
    
    IOUtils.close(indexWriter, taxoWriter);
    
    return expectedCounts;
  }
  
  private void migrateIndex(Directory indexDir, FacetIndexingParams fip) throws Exception {
    final Map<String,Term> fieldTerms = FacetsPayloadMigrationReader.buildFieldTermsMap(indexDir, fip);
    DirectoryReader reader = DirectoryReader.open(indexDir);
    List<AtomicReaderContext> leaves = reader.leaves();
    int numReaders = leaves.size();
    AtomicReader wrappedLeaves[] = new AtomicReader[numReaders];
    for (int i = 0; i < numReaders; i++) {
      wrappedLeaves[i] = new FacetsPayloadMigrationReader(leaves.get(i).reader(), fieldTerms);
    }
    
    IndexWriter writer = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    writer.deleteAll();
    try {
      writer.addIndexes(new MultiReader(wrappedLeaves));
      writer.commit();
    } finally {
      reader.close();
      writer.close();
    }
  }
  
  private void verifyMigratedIndex(Directory indexDir, Directory taxoDir, HashMap<String,Integer> expectedCounts, 
      FacetIndexingParams fip) throws Exception {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = newSearcher(indexReader);

    assertFalse("index should not have deletions", indexReader.hasDeletions());
    
    verifyNotFacetsData(indexReader, searcher);
    verifyFacetedSearch(expectedCounts, fip, indexReader, taxoReader, searcher);
    verifyDrillDown(expectedCounts, fip, indexReader, taxoReader, searcher);
    verifyIndexOrdinals(indexReader, taxoReader, fip);
    
    IOUtils.close(indexReader, taxoReader);
  }
  
  private void verifyNotFacetsData(DirectoryReader indexReader, IndexSearcher searcher) throws IOException {
    // verify that non facets data was not damaged
    TotalHitCountCollector total = new TotalHitCountCollector();
    searcher.search(new PrefixQuery(new Term("foo", "content")), total);
    assertEquals("invalid number of results for content query", total.getTotalHits(), indexReader.maxDoc());
    
    int numDocIDs = 0;
    for (AtomicReaderContext context : indexReader.leaves()) {
      Terms docIDs = context.reader().terms("docid");
      assertNotNull(docIDs);
      TermsEnum te = docIDs.iterator(null);
      while (te.next() != null) {
        ++numDocIDs;
      }
    }
    assertEquals("invalid number of docid terms", indexReader.maxDoc(), numDocIDs);
  }
  
  private void verifyFacetedSearch(Map<String,Integer> expectedCounts, FacetIndexingParams fip, 
      DirectoryReader indexReader, TaxonomyReader taxoReader, IndexSearcher searcher) throws IOException {
    // run faceted search and assert expected counts
    ArrayList<FacetRequest> requests = new ArrayList<FacetRequest>(expectedCounts.size());
    for (String dim : expectedCounts.keySet()) {
      requests.add(new CountFacetRequest(new CategoryPath(dim), 5));
    }
    FacetSearchParams fsp = new FacetSearchParams(fip, requests);
    FacetsCollector fc = FacetsCollector.create(fsp, indexReader, taxoReader);
    MatchAllDocsQuery base = new MatchAllDocsQuery();
    searcher.search(base, fc);
    List<FacetResult> facetResults = fc.getFacetResults();
    assertEquals(requests.size(), facetResults.size());
    for (FacetResult res : facetResults) {
      FacetResultNode node = res.getFacetResultNode();
      String dim = node.label.components[0];
      assertEquals("wrong count for " + dim, expectedCounts.get(dim).intValue(), (int) node.value);
    }
  }
  
  private void verifyDrillDown(Map<String,Integer> expectedCounts, FacetIndexingParams fip, DirectoryReader indexReader, 
      TaxonomyReader taxoReader, IndexSearcher searcher) throws IOException {
    // verify drill-down
    for (String dim : expectedCounts.keySet()) {
      CategoryPath drillDownCP = new CategoryPath(dim);
      FacetSearchParams fsp = new FacetSearchParams(fip, new CountFacetRequest(drillDownCP, 10));
      DrillDownQuery drillDown = new DrillDownQuery(fip, new MatchAllDocsQuery());
      drillDown.add(drillDownCP);
      TotalHitCountCollector total = new TotalHitCountCollector();
      FacetsCollector fc = FacetsCollector.create(fsp, indexReader, taxoReader);
      searcher.search(drillDown, MultiCollector.wrap(fc, total));
      assertTrue("no results for drill-down query " + drillDown, total.getTotalHits() > 0);
      List<FacetResult> facetResults = fc.getFacetResults();
      assertEquals(1, facetResults.size());
      FacetResultNode rootNode = facetResults.get(0).getFacetResultNode();
      assertEquals("wrong count for " + dim, expectedCounts.get(dim).intValue(), (int) rootNode.value);
    }
  }
  
  private void verifyIndexOrdinals(DirectoryReader indexReader, TaxonomyReader taxoReader, FacetIndexingParams fip) 
      throws IOException {
    // verify that the ordinals in the index match the ones in the taxonomy, and vice versa
    
    // collect all fields which have DocValues, to assert later that all were
    // visited i.e. that during migration we didn't add FieldInfos with no
    // DocValues
    HashSet<String> docValuesFields = new HashSet<String>();
    for (AtomicReaderContext context : indexReader.leaves()) {
      FieldInfos infos = context.reader().getFieldInfos();
      for (FieldInfo info : infos) {
        if (info.hasDocValues()) {
          docValuesFields.add(info.name);
        }
      }
    }
    
    // check that all visited ordinals are found in the taxonomy and vice versa
    boolean[] foundOrdinals = new boolean[taxoReader.getSize()];
    for (int i = 0; i < foundOrdinals.length; i++) {
      foundOrdinals[i] = false; // init to be on the safe side
    }
    foundOrdinals[0] = true; // ROOT ordinals isn't indexed
    // mark 'dummy' category ordinal as seen
    int dummyOrdinal = taxoReader.getOrdinal(new CategoryPath("dummy"));
    if (dummyOrdinal > 0) {
      foundOrdinals[dummyOrdinal] = true;
    }
    
    int partitionSize = fip.getPartitionSize();
    int numPartitions = (int) Math.ceil(taxoReader.getSize() / (double) partitionSize);
    final IntsRef ordinals = new IntsRef(32);
    for (String dim : DIMENSIONS) {
      CategoryListParams clp = fip.getCategoryListParams(new CategoryPath(dim));
      int partitionOffset = 0;
      for (int partition = 0; partition < numPartitions; partition++, partitionOffset += partitionSize) {
        final CategoryListIterator cli = clp.createCategoryListIterator(partition);
        for (AtomicReaderContext context : indexReader.leaves()) {
          if (cli.setNextReader(context)) { // not all fields may exist in all segments
            // remove that field from the list of DocValues fields
            docValuesFields.remove(clp.field + PartitionsUtils.partitionName(partition));
            int maxDoc = context.reader().maxDoc();
            for (int doc = 0; doc < maxDoc; doc++) {
              cli.getOrdinals(doc, ordinals);
              for (int j = 0; j < ordinals.length; j++) {
                // verify that the ordinal is recognized by the taxonomy
                int ordinal = ordinals.ints[j] + partitionOffset;
                assertTrue("should not have received dummy ordinal (" + dummyOrdinal + ")", dummyOrdinal != ordinal);
                assertNotNull("missing category for ordinal " + ordinal, taxoReader.getPath(ordinal));
                foundOrdinals[ordinal] = true;
              }
            }
          }
        }
      }
    }
    
    assertTrue("some fields which have docValues were not visited: " + docValuesFields, docValuesFields.isEmpty());
    
    for (int i = 0; i < foundOrdinals.length; i++) {
      assertTrue("ordinal " + i + " not visited", foundOrdinals[i]);
    }
  }
  
  private void doTestMigration(final int partitionSize) throws Exception {
    // create a facets index with PayloadFacetFields and check it after migration
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    // set custom CLP fields for two dimensions and use the default ($facets) for the other two
    HashMap<CategoryPath,CategoryListParams> params = new HashMap<CategoryPath,CategoryListParams>();
    params.put(new CategoryPath(DIMENSIONS[0]), new CategoryListParams(DIMENSIONS[0]) {
      @Override
      public OrdinalPolicy getOrdinalPolicy(String dimension) {
        return OrdinalPolicy.ALL_PARENTS;
      }
    });
    params.put(new CategoryPath(DIMENSIONS[1]), new CategoryListParams(DIMENSIONS[1]) {
      @Override
      public OrdinalPolicy getOrdinalPolicy(String dimension) {
        return OrdinalPolicy.ALL_PARENTS;
      }
    });
    
    HashMap<String,OrdinalPolicy> policies = new HashMap<String,CategoryListParams.OrdinalPolicy>();
    policies.put(DIMENSIONS[2], OrdinalPolicy.ALL_PARENTS);
    policies.put(DIMENSIONS[3], OrdinalPolicy.ALL_PARENTS);
    FacetIndexingParams fip = new PerDimensionIndexingParams(params, new PerDimensionOrdinalPolicy(policies)) {
      @Override
      public int getPartitionSize() {
        return partitionSize;
      }
    };
    
    HashMap<String,Integer> expectedCounts = createIndex(indexDir, taxoDir, fip);
    migrateIndex(indexDir, fip);
    verifyMigratedIndex(indexDir, taxoDir, expectedCounts, fip);
    
    IOUtils.close(indexDir, taxoDir);
  }
  
  @Test
  public void testMigration() throws Exception {
    doTestMigration(Integer.MAX_VALUE);
  }
  
  @Test
  public void testMigrationWithPartitions() throws Exception {
    doTestMigration(2);
  }
  
}
