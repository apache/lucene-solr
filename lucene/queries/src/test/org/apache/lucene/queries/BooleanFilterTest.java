package org.apache.lucene.queries;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

public class BooleanFilterTest extends LuceneTestCase {
  private Directory directory;
  private AtomicReader reader;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));

    //Add series of docs with filterable fields : acces rights, prices, dates and "in-stock" flags
    addDoc(writer, "admin guest", "010", "20040101", "Y");
    addDoc(writer, "guest", "020", "20040101", "Y");
    addDoc(writer, "guest", "020", "20050101", "Y");
    addDoc(writer, "admin", "020", "20050101", "Maybe");
    addDoc(writer, "admin guest", "030", "20050101", "N");
    reader = new SlowCompositeReaderWrapper(writer.getReader());
    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }

  private void addDoc(RandomIndexWriter writer, String accessRights, String price, String date, String inStock) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("accessRights", accessRights, Field.Store.YES));
    doc.add(newTextField("price", price, Field.Store.YES));
    doc.add(newTextField("date", date, Field.Store.YES));
    doc.add(newTextField("inStock", inStock, Field.Store.YES));
    writer.addDocument(doc);
  }

  private Filter getRangeFilter(String field, String lowerPrice, String upperPrice) {
    Filter f = TermRangeFilter.newStringRange(field, lowerPrice, upperPrice, true, true);
    return f;
  }

  private Filter getTermsFilter(String field, String text) {
    return new TermsFilter(new Term(field, text));
  }
  
  private Filter getWrappedTermQuery(String field, String text) {
    return new QueryWrapperFilter(new TermQuery(new Term(field, text)));
  }
  
  private Filter getEmptyFilter() {
    return new Filter() {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) {
        return new FixedBitSet(context.reader().maxDoc());
      }
    };
  }

  private Filter getNullDISFilter() {
    return new Filter() {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) {
        return null;
      }
    };
  }

  private Filter getNullDISIFilter() {
    return new Filter() {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) {
        return new DocIdSet() {
          @Override
          public DocIdSetIterator iterator() {
            return null;
          }
          
          @Override
          public boolean isCacheable() {
            return true;
          }
        };
      }
    };
  }

  private void tstFilterCard(String mes, int expected, Filter filt)
      throws Exception {
    // BooleanFilter never returns null DIS or null DISI!
    DocIdSetIterator disi = filt.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator();
    int actual = 0;
    while (disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      actual++;
    }
    assertEquals(mes, expected, actual);
  }


  public void testShould() throws Exception {
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(getTermsFilter("price", "030"), Occur.SHOULD);
    tstFilterCard("Should retrieves only 1 doc", 1, booleanFilter);
    
    // same with a real DISI (no OpenBitSetIterator)
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getWrappedTermQuery("price", "030"), Occur.SHOULD);
    tstFilterCard("Should retrieves only 1 doc", 1, booleanFilter);
  }

  public void testShoulds() throws Exception {
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(getRangeFilter("price", "010", "020"), Occur.SHOULD);
    booleanFilter.add(getRangeFilter("price", "020", "030"), Occur.SHOULD);
    tstFilterCard("Shoulds are Ored together", 5, booleanFilter);
  }

  public void testShouldsAndMustNot() throws Exception {
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(getRangeFilter("price", "010", "020"), Occur.SHOULD);
    booleanFilter.add(getRangeFilter("price", "020", "030"), Occur.SHOULD);
    booleanFilter.add(getTermsFilter("inStock", "N"), Occur.MUST_NOT);
    tstFilterCard("Shoulds Ored but AndNot", 4, booleanFilter);

    booleanFilter.add(getTermsFilter("inStock", "Maybe"), Occur.MUST_NOT);
    tstFilterCard("Shoulds Ored but AndNots", 3, booleanFilter);
    
    // same with a real DISI (no OpenBitSetIterator)
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getRangeFilter("price", "010", "020"), Occur.SHOULD);
    booleanFilter.add(getRangeFilter("price", "020", "030"), Occur.SHOULD);
    booleanFilter.add(getWrappedTermQuery("inStock", "N"), Occur.MUST_NOT);
    tstFilterCard("Shoulds Ored but AndNot", 4, booleanFilter);

    booleanFilter.add(getWrappedTermQuery("inStock", "Maybe"), Occur.MUST_NOT);
    tstFilterCard("Shoulds Ored but AndNots", 3, booleanFilter);
  }

  public void testShouldsAndMust() throws Exception {
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(getRangeFilter("price", "010", "020"), Occur.SHOULD);
    booleanFilter.add(getRangeFilter("price", "020", "030"), Occur.SHOULD);
    booleanFilter.add(getTermsFilter("accessRights", "admin"), Occur.MUST);
    tstFilterCard("Shoulds Ored but MUST", 3, booleanFilter);
    
    // same with a real DISI (no OpenBitSetIterator)
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getRangeFilter("price", "010", "020"), Occur.SHOULD);
    booleanFilter.add(getRangeFilter("price", "020", "030"), Occur.SHOULD);
    booleanFilter.add(getWrappedTermQuery("accessRights", "admin"), Occur.MUST);
    tstFilterCard("Shoulds Ored but MUST", 3, booleanFilter);
  }

  public void testShouldsAndMusts() throws Exception {
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(getRangeFilter("price", "010", "020"), Occur.SHOULD);
    booleanFilter.add(getRangeFilter("price", "020", "030"), Occur.SHOULD);
    booleanFilter.add(getTermsFilter("accessRights", "admin"), Occur.MUST);
    booleanFilter.add(getRangeFilter("date", "20040101", "20041231"), Occur.MUST);
    tstFilterCard("Shoulds Ored but MUSTs ANDED", 1, booleanFilter);
  }

  public void testShouldsAndMustsAndMustNot() throws Exception {
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(getRangeFilter("price", "030", "040"), Occur.SHOULD);
    booleanFilter.add(getTermsFilter("accessRights", "admin"), Occur.MUST);
    booleanFilter.add(getRangeFilter("date", "20050101", "20051231"), Occur.MUST);
    booleanFilter.add(getTermsFilter("inStock", "N"), Occur.MUST_NOT);
    tstFilterCard("Shoulds Ored but MUSTs ANDED and MustNot", 0, booleanFilter);
    
    // same with a real DISI (no OpenBitSetIterator)
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getRangeFilter("price", "030", "040"), Occur.SHOULD);
    booleanFilter.add(getWrappedTermQuery("accessRights", "admin"), Occur.MUST);
    booleanFilter.add(getRangeFilter("date", "20050101", "20051231"), Occur.MUST);
    booleanFilter.add(getWrappedTermQuery("inStock", "N"), Occur.MUST_NOT);
    tstFilterCard("Shoulds Ored but MUSTs ANDED and MustNot", 0, booleanFilter);
  }

  public void testJustMust() throws Exception {
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(getTermsFilter("accessRights", "admin"), Occur.MUST);
    tstFilterCard("MUST", 3, booleanFilter);
    
    // same with a real DISI (no OpenBitSetIterator)
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getWrappedTermQuery("accessRights", "admin"), Occur.MUST);
    tstFilterCard("MUST", 3, booleanFilter);
  }

  public void testJustMustNot() throws Exception {
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(getTermsFilter("inStock", "N"), Occur.MUST_NOT);
    tstFilterCard("MUST_NOT", 4, booleanFilter);
    
    // same with a real DISI (no OpenBitSetIterator)
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getWrappedTermQuery("inStock", "N"), Occur.MUST_NOT);
    tstFilterCard("MUST_NOT", 4, booleanFilter);
  }

  public void testMustAndMustNot() throws Exception {
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(getTermsFilter("inStock", "N"), Occur.MUST);
    booleanFilter.add(getTermsFilter("price", "030"), Occur.MUST_NOT);
    tstFilterCard("MUST_NOT wins over MUST for same docs", 0, booleanFilter);
    
    // same with a real DISI (no OpenBitSetIterator)
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getWrappedTermQuery("inStock", "N"), Occur.MUST);
    booleanFilter.add(getWrappedTermQuery("price", "030"), Occur.MUST_NOT);
    tstFilterCard("MUST_NOT wins over MUST for same docs", 0, booleanFilter);
  }

  public void testEmpty() throws Exception {
    BooleanFilter booleanFilter = new BooleanFilter();
    tstFilterCard("empty BooleanFilter returns no results", 0, booleanFilter);
  }

  public void testCombinedNullDocIdSets() throws Exception {
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(getTermsFilter("price", "030"), Occur.MUST);
    booleanFilter.add(getNullDISFilter(), Occur.MUST);
    tstFilterCard("A MUST filter that returns a null DIS should never return documents", 0, booleanFilter);
    
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getTermsFilter("price", "030"), Occur.MUST);
    booleanFilter.add(getNullDISIFilter(), Occur.MUST);
    tstFilterCard("A MUST filter that returns a null DISI should never return documents", 0, booleanFilter);
    
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getTermsFilter("price", "030"), Occur.SHOULD);
    booleanFilter.add(getNullDISFilter(), Occur.SHOULD);
    tstFilterCard("A SHOULD filter that returns a null DIS should be invisible", 1, booleanFilter);
    
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getTermsFilter("price", "030"), Occur.SHOULD);
    booleanFilter.add(getNullDISIFilter(), Occur.SHOULD);
    tstFilterCard("A SHOULD filter that returns a null DISI should be invisible", 1, booleanFilter);
    
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getTermsFilter("price", "030"), Occur.MUST);
    booleanFilter.add(getNullDISFilter(), Occur.MUST_NOT);
    tstFilterCard("A MUST_NOT filter that returns a null DIS should be invisible", 1, booleanFilter);
    
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getTermsFilter("price", "030"), Occur.MUST);
    booleanFilter.add(getNullDISIFilter(), Occur.MUST_NOT);
    tstFilterCard("A MUST_NOT filter that returns a null DISI should be invisible", 1, booleanFilter);
  }

  public void testJustNullDocIdSets() throws Exception {
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(getNullDISFilter(), Occur.MUST);
    tstFilterCard("A MUST filter that returns a null DIS should never return documents", 0, booleanFilter);
    
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getNullDISIFilter(), Occur.MUST);
    tstFilterCard("A MUST filter that returns a null DISI should never return documents", 0, booleanFilter);
    
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getNullDISFilter(), Occur.SHOULD);
    tstFilterCard("A single SHOULD filter that returns a null DIS should never return documents", 0, booleanFilter);
    
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getNullDISIFilter(), Occur.SHOULD);
    tstFilterCard("A single SHOULD filter that returns a null DISI should never return documents", 0, booleanFilter);
    
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getNullDISFilter(), Occur.MUST_NOT);
    tstFilterCard("A single MUST_NOT filter that returns a null DIS should be invisible", 5, booleanFilter);
    
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getNullDISIFilter(), Occur.MUST_NOT);
    tstFilterCard("A single MUST_NOT filter that returns a null DIS should be invisible", 5, booleanFilter);
  }
  
  public void testNonMatchingShouldsAndMusts() throws Exception {
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(getEmptyFilter(), Occur.SHOULD);
    booleanFilter.add(getTermsFilter("accessRights", "admin"), Occur.MUST);
    tstFilterCard(">0 shoulds with no matches should return no docs", 0, booleanFilter);
    
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getNullDISFilter(), Occur.SHOULD);
    booleanFilter.add(getTermsFilter("accessRights", "admin"), Occur.MUST);
    tstFilterCard(">0 shoulds with no matches should return no docs", 0, booleanFilter);
    
    booleanFilter = new BooleanFilter();
    booleanFilter.add(getNullDISIFilter(), Occur.SHOULD);
    booleanFilter.add(getTermsFilter("accessRights", "admin"), Occur.MUST);
    tstFilterCard(">0 shoulds with no matches should return no docs", 0, booleanFilter);
  }

  public void testToStringOfBooleanFilterContainingTermsFilter() {
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(getTermsFilter("inStock", "N"), Occur.MUST);
    booleanFilter.add(getTermsFilter("isFragile", "Y"), Occur.MUST);

    assertEquals("BooleanFilter(+inStock:N +isFragile:Y)", booleanFilter.toString());
  }

  public void testToStringOfWrappedBooleanFilters() {
    BooleanFilter orFilter = new BooleanFilter();

    BooleanFilter stockFilter = new BooleanFilter();
    stockFilter.add(new FilterClause(getTermsFilter("inStock", "Y"), Occur.MUST));
    stockFilter.add(new FilterClause(getTermsFilter("barCode", "12345678"), Occur.MUST));

    orFilter.add(new FilterClause(stockFilter,Occur.SHOULD));

    BooleanFilter productPropertyFilter = new BooleanFilter();
    productPropertyFilter.add(new FilterClause(getTermsFilter("isHeavy", "N"), Occur.MUST));
    productPropertyFilter.add(new FilterClause(getTermsFilter("isDamaged", "Y"), Occur.MUST));

    orFilter.add(new FilterClause(productPropertyFilter,Occur.SHOULD));

    BooleanFilter composedFilter = new BooleanFilter();
    composedFilter.add(new FilterClause(orFilter,Occur.MUST));

    assertEquals("BooleanFilter(+BooleanFilter(BooleanFilter(+inStock:Y +barCode:12345678) BooleanFilter(+isHeavy:N +isDamaged:Y)))",
        composedFilter.toString());
  }
}
