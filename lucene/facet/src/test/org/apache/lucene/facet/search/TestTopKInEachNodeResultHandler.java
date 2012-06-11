package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.index.CategoryDocumentBuilder;
import org.apache.lucene.facet.index.params.DefaultFacetIndexingParams;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.params.FacetRequest.ResultMode;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.util.PartitionsUtils;

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

public class TestTopKInEachNodeResultHandler extends LuceneTestCase {

  //TODO (Facet): Move to extend BaseTestTopK and separate to several smaller test cases (methods) - see TestTopKResultsHandler
  
  @Test
  public void testSimple() throws Exception {

    int[] partitionSizes = new int[] { 
        2,3,4, 5, 6, 7, 10, 1000,
        Integer.MAX_VALUE };

    for (int partitionSize : partitionSizes) {
      Directory iDir = newDirectory();
      Directory tDir = newDirectory();

      if (VERBOSE) {
        System.out.println("Partition Size: " + partitionSize);
      }
      
      final int pSize = partitionSize;
      DefaultFacetIndexingParams iParams = new DefaultFacetIndexingParams() {
        @Override
        protected int fixedPartitionSize() {
          return pSize;
        }
      };

      RandomIndexWriter iw = new RandomIndexWriter(random(), iDir,
          newIndexWriterConfig(TEST_VERSION_CURRENT,
              new MockAnalyzer(random())).setOpenMode(OpenMode.CREATE));
      TaxonomyWriter tw = new DirectoryTaxonomyWriter(tDir);
      prvt_add(iParams, iw, tw, "a", "b");
      prvt_add(iParams, iw, tw, "a", "b", "1");
      prvt_add(iParams, iw, tw, "a", "b", "1");
      prvt_add(iParams, iw, tw, "a", "b", "2");
      prvt_add(iParams, iw, tw, "a", "b", "2");
      prvt_add(iParams, iw, tw, "a", "b", "2");
      prvt_add(iParams, iw, tw, "a", "b", "3");
      prvt_add(iParams, iw, tw, "a", "b", "4");
      prvt_add(iParams, iw, tw, "a", "c");
      prvt_add(iParams, iw, tw, "a", "c");
      prvt_add(iParams, iw, tw, "a", "c");
      prvt_add(iParams, iw, tw, "a", "c");
      prvt_add(iParams, iw, tw, "a", "c");
      prvt_add(iParams, iw, tw, "a", "c", "1");
      prvt_add(iParams, iw, tw, "a", "d");
      prvt_add(iParams, iw, tw, "a", "e");

      IndexReader ir = iw.getReader();
      iw.close();
      tw.commit();
      tw.close();

      IndexSearcher is = newSearcher(ir);
      DirectoryTaxonomyReader tr = new DirectoryTaxonomyReader(tDir);

      // Get all of the documents and run the query, then do different
      // facet counts and compare to control
      Query q = new TermQuery(new Term("content", "alpha"));
      ScoredDocIdCollector scoredDoc = ScoredDocIdCollector.create(ir.maxDoc(), true);

      // Collector collector = new MultiCollector(scoredDoc);
      is.search(q, scoredDoc);

      CountFacetRequest cfra23 = new CountFacetRequest(
          new CategoryPath("a"), 2);
      cfra23.setDepth(3);
      cfra23.setResultMode(ResultMode.PER_NODE_IN_TREE);

      CountFacetRequest cfra22 = new CountFacetRequest(
          new CategoryPath("a"), 2);
      cfra22.setDepth(2);
      cfra22.setResultMode(ResultMode.PER_NODE_IN_TREE);

      CountFacetRequest cfra21 = new CountFacetRequest(
          new CategoryPath("a"), 2);
      cfra21.setDepth(1);
      cfra21.setResultMode(ResultMode.PER_NODE_IN_TREE);

      CountFacetRequest cfrb22 = new CountFacetRequest(
          new CategoryPath("a", "b"), 2);
      cfrb22.setDepth(2);
      cfrb22.setResultMode(ResultMode.PER_NODE_IN_TREE);

      CountFacetRequest cfrb23 = new CountFacetRequest(
          new CategoryPath("a", "b"), 2);
      cfrb23.setDepth(3);
      cfrb23.setResultMode(ResultMode.PER_NODE_IN_TREE);

      CountFacetRequest cfrb21 = new CountFacetRequest(
          new CategoryPath("a", "b"), 2);
      cfrb21.setDepth(1);
      cfrb21.setResultMode(ResultMode.PER_NODE_IN_TREE);

      CountFacetRequest doctor = new CountFacetRequest(
          new CategoryPath("Doctor"), 2);
      doctor.setDepth(1);
      doctor.setResultMode(ResultMode.PER_NODE_IN_TREE);

      CountFacetRequest cfrb20 = new CountFacetRequest(
          new CategoryPath("a", "b"), 2);
      cfrb20.setDepth(0);
      cfrb20.setResultMode(ResultMode.PER_NODE_IN_TREE);

      FacetSearchParams facetSearchParams = new FacetSearchParams(iParams);
      facetSearchParams.addFacetRequest(cfra23);
      facetSearchParams.addFacetRequest(cfra22);
      facetSearchParams.addFacetRequest(cfra21);
      facetSearchParams.addFacetRequest(cfrb23);
      facetSearchParams.addFacetRequest(cfrb22);
      facetSearchParams.addFacetRequest(cfrb21);
      facetSearchParams.addFacetRequest(doctor);
      facetSearchParams.addFacetRequest(cfrb20);
      
      IntArrayAllocator iaa = new IntArrayAllocator(PartitionsUtils.partitionSize(facetSearchParams,tr), 1);
      FloatArrayAllocator faa = new FloatArrayAllocator(PartitionsUtils.partitionSize(facetSearchParams,tr), 1);
      FacetsAccumulator fctExtrctr = new StandardFacetsAccumulator(facetSearchParams, is.getIndexReader(), tr, iaa, faa);
      fctExtrctr.setComplementThreshold(FacetsAccumulator.DISABLE_COMPLEMENT);
      long start = System.currentTimeMillis();

      List<FacetResult> facetResults = fctExtrctr.accumulate(scoredDoc.getScoredDocIDs());

      long end = System.currentTimeMillis();
      if (VERBOSE) {
        System.out.println("Time: " + (end - start));
      }
      
      FacetResult fr = facetResults.get(0); // a, depth=3, K=2
      boolean hasDoctor = "Doctor".equals(fr.getFacetRequest().getCategoryPath().getComponent(0));
      assertEquals(9, fr.getNumValidDescendants());
      FacetResultNode parentRes = fr.getFacetResultNode();
      assertEquals(16.0, parentRes.getValue(), Double.MIN_VALUE);
      assertEquals(2.0, parentRes.getResidue(), Double.MIN_VALUE);
      assertEquals(2, parentRes.getNumSubResults());
      // two nodes sorted by descending values: a/b with 8  and a/c with 6
      // a/b has residue 2 and two children a/b/2 with value 3, and a/b/1 with value 2. 
      // a/c has residue 0, and one child a/c/1 with value 1.
      double [] expectedValues0 = { 8.0, 2.0, 3.0, 0.0, 2.0, 0.0, 6.0, 0.0, 1.0, 0.0 };
      int i = 0;
      for (FacetResultNode node : parentRes.getSubResults()) {
        assertEquals(expectedValues0[i++], node.getValue(), Double.MIN_VALUE);
        assertEquals(expectedValues0[i++], node.getResidue(), Double.MIN_VALUE);
        for (FacetResultNode node2 : node.getSubResults()) {
          assertEquals(expectedValues0[i++], node2.getValue(), Double.MIN_VALUE);
          assertEquals(expectedValues0[i++], node2.getResidue(), Double.MIN_VALUE);
        }
      }

      // now just change the value of the first child of the root to 5, and then rearrange
      // expected are: first a/c of value 6 and residue 0, and one child a/c/1 with value 1
      // then a/b with value 5 and residue 2, and both children: a/b/2 with value 3, and a/b/1 with value 2.
      for (FacetResultNode node : parentRes.getSubResults()) {
        node.setValue(5.0);
        break;
      }
      // now rearrange
      double [] expectedValues00 = { 6.0, 0.0, 1.0, 0.0, 5.0, 2.0, 3.0, 0.0, 2.0, 0.0 };
      fr = cfra23.createFacetResultsHandler(tr).rearrangeFacetResult(fr);
      i = 0;
      for (FacetResultNode node : parentRes.getSubResults()) {
        assertEquals(expectedValues00[i++], node.getValue(), Double.MIN_VALUE);
        assertEquals(expectedValues00[i++], node.getResidue(), Double.MIN_VALUE);
        for (FacetResultNode node2 : node.getSubResults()) {
          assertEquals(expectedValues00[i++], node2.getValue(), Double.MIN_VALUE);
          assertEquals(expectedValues00[i++], node2.getResidue(), Double.MIN_VALUE);
        }
      }

      fr = facetResults.get(1); // a, depth=2, K=2. same result as before
      hasDoctor |= "Doctor".equals(fr.getFacetRequest().getCategoryPath().getComponent(0));
      assertEquals(9, fr.getNumValidDescendants());
      parentRes = fr.getFacetResultNode();
      assertEquals(16.0, parentRes.getValue(), Double.MIN_VALUE);
      assertEquals(2.0, parentRes.getResidue(), Double.MIN_VALUE);
      assertEquals(2, parentRes.getNumSubResults());
      // two nodes sorted by descending values: a/b with 8  and a/c with 6
      // a/b has residue 2 and two children a/b/2 with value 3, and a/b/1 with value 2. 
      // a/c has residue 0, and one child a/c/1 with value 1.
      i = 0;
      for (FacetResultNode node : parentRes.getSubResults()) {
        assertEquals(expectedValues0[i++], node.getValue(), Double.MIN_VALUE);
        assertEquals(expectedValues0[i++], node.getResidue(), Double.MIN_VALUE);
        for (FacetResultNode node2 : node.getSubResults()) {
          assertEquals(expectedValues0[i++], node2.getValue(), Double.MIN_VALUE);
          assertEquals(expectedValues0[i++], node2.getResidue(), Double.MIN_VALUE);
        }
      }

      fr = facetResults.get(2); // a, depth=1, K=2
      hasDoctor |= "Doctor".equals(fr.getFacetRequest().getCategoryPath().getComponent(0));
      assertEquals(4, fr.getNumValidDescendants(), 4);
      parentRes = fr.getFacetResultNode();
      assertEquals(16.0, parentRes.getValue(), Double.MIN_VALUE);
      assertEquals(2.0, parentRes.getResidue(), Double.MIN_VALUE);
      assertEquals(2, parentRes.getNumSubResults());
      // two nodes sorted by descending values: 
      // a/b with value 8 and residue 0 (because no children considered),  
      //  and a/c with value 6 and residue 0 (because no children considered)
      double [] expectedValues2 = { 8.0, 0.0, 6.0, 0.0 };
      i = 0;
      for (FacetResultNode node : parentRes.getSubResults()) {
        assertEquals(expectedValues2[i++], node.getValue(), Double.MIN_VALUE);
        assertEquals(expectedValues2[i++], node.getResidue(), Double.MIN_VALUE);
        assertEquals(node.getNumSubResults(), 0);
      }
      
      fr = facetResults.get(3); // a/b, depth=3, K=2
      hasDoctor |= "Doctor".equals(fr.getFacetRequest().getCategoryPath().getComponent(0));
      assertEquals(4, fr.getNumValidDescendants());
      parentRes = fr.getFacetResultNode();
      assertEquals(8.0, parentRes.getValue(), Double.MIN_VALUE);
      assertEquals(2.0, parentRes.getResidue(), Double.MIN_VALUE);
      assertEquals(2, parentRes.getNumSubResults());
      double [] expectedValues3 = { 3.0, 2.0 };
      i = 0;
      for (FacetResultNode node : parentRes.getSubResults()) {
        assertEquals(expectedValues3[i++], node.getValue(), Double.MIN_VALUE);
        assertEquals(0.0, node.getResidue(), Double.MIN_VALUE);
        assertEquals(0, node.getNumSubResults());
      }

      fr = facetResults.get(4); // a/b, depth=2, K=2
      hasDoctor |= "Doctor".equals(fr.getFacetRequest().getCategoryPath().getComponent(0));
      assertEquals(4, fr.getNumValidDescendants());
      parentRes = fr.getFacetResultNode();
      assertEquals(8.0, parentRes.getValue(), Double.MIN_VALUE);
      assertEquals(2.0, parentRes.getResidue(), Double.MIN_VALUE);
      assertEquals(2, parentRes.getNumSubResults());
      i = 0;
      for (FacetResultNode node : parentRes.getSubResults()) {
        assertEquals(expectedValues3[i++], node.getValue(), Double.MIN_VALUE);
        assertEquals(0.0, node.getResidue(), Double.MIN_VALUE);
        assertEquals(0, node.getNumSubResults());
      }

      fr = facetResults.get(5); // a/b, depth=1, K=2
      hasDoctor |= "Doctor".equals(fr.getFacetRequest().getCategoryPath().getComponent(0));
      assertEquals(4, fr.getNumValidDescendants());
      parentRes = fr.getFacetResultNode();
      assertEquals(8.0, parentRes.getValue(), Double.MIN_VALUE);
      assertEquals(2.0, parentRes.getResidue(), Double.MIN_VALUE);
      assertEquals(2, parentRes.getNumSubResults());
      i = 0;
      for (FacetResultNode node : parentRes.getSubResults()) {
        assertEquals(expectedValues3[i++], node.getValue(), Double.MIN_VALUE);
        assertEquals(0.0, node.getResidue(), Double.MIN_VALUE);
        assertEquals(0, node.getNumSubResults());
      }
      
      fr = facetResults.get(6); // a/b, depth=0, K=2
      hasDoctor |= "Doctor".equals(fr.getFacetRequest().getCategoryPath().getComponent(0));
      assertEquals(0, fr.getNumValidDescendants()); // 0 descendants but rootnode
      parentRes = fr.getFacetResultNode();
      assertEquals(8.0, parentRes.getValue(), Double.MIN_VALUE);
      assertEquals(0.0, parentRes.getResidue(), Double.MIN_VALUE);
      assertEquals(0, parentRes.getNumSubResults());
      hasDoctor |= "Doctor".equals(fr.getFacetRequest().getCategoryPath().getComponent(0));

      // doctor, depth=1, K=2
      assertFalse("Shouldn't have found anything for a FacetRequest " +
          "of a facet that doesn't exist in the index.", hasDoctor);
      assertEquals("Shouldn't have found more than seven request.", 7, facetResults.size());
      ir.close();
      tr.close();
      iDir.close();
      tDir.close();
    }

  }

  private void prvt_add(DefaultFacetIndexingParams iParams, RandomIndexWriter iw,
                    TaxonomyWriter tw, String... strings) throws IOException,
      CorruptIndexException {
    ArrayList<CategoryPath> cps = new ArrayList<CategoryPath>();
    CategoryPath cp = new CategoryPath(strings);
    cps.add(cp);
    Document d = new Document();
    new CategoryDocumentBuilder(tw, iParams).setCategoryPaths(cps).build(d);
    d.add(new TextField("content", "alpha", Field.Store.YES));
    iw.addDocument(d);
  }

}
