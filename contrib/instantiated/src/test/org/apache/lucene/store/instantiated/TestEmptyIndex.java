/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.store.instantiated;

import junit.framework.TestCase;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocCollector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

public class TestEmptyIndex extends TestCase {

  public void testSearch() throws Exception {

    InstantiatedIndex ii = new InstantiatedIndex();

    IndexReader r = new InstantiatedIndexReader(ii);
    IndexSearcher s = new IndexSearcher(r);

    TopDocCollector c = new TopDocCollector(1);
    s.search(new TermQuery(new Term("foo", "bar")), c);

    assertEquals(0, c.getTotalHits());

    s.close();
    r.close();
    ii.close();

  }

  public void testTermEnum() throws Exception {

    InstantiatedIndex ii = new InstantiatedIndex();
    IndexReader r = new InstantiatedIndexReader(ii);
    termEnumTest(r);
    r.close();
    ii.close();

    // make sure a Directory acts the same

    Directory d = new RAMDirectory();
    new IndexWriter(d, null, true, IndexWriter.MaxFieldLength.UNLIMITED).close();
    r = IndexReader.open(d);
    termEnumTest(r);
    r.close();
    d.close();
  }

  public void termEnumTest(IndexReader r) throws Exception {
    TermEnum terms = r.terms();

    assertNull(terms.term());
    assertFalse(terms.next());
    assertFalse(terms.skipTo(new Term("foo", "bar")));

  }

}
