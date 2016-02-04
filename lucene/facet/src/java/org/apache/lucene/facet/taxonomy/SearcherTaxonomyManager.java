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
package org.apache.lucene.facet.taxonomy;

import java.io.IOException;

import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

/**
 * Manages near-real-time reopen of both an IndexSearcher
 * and a TaxonomyReader.
 *
 * <p><b>NOTE</b>: If you call {@link
 * DirectoryTaxonomyWriter#replaceTaxonomy} then you must
 * open a new {@code SearcherTaxonomyManager} afterwards.
 */
public class SearcherTaxonomyManager extends ReferenceManager<SearcherTaxonomyManager.SearcherAndTaxonomy> {

  /** Holds a matched pair of {@link IndexSearcher} and
   *  {@link TaxonomyReader} */
  public static class SearcherAndTaxonomy {
    /** Point-in-time {@link IndexSearcher}. */
    public final IndexSearcher searcher;

    /** Matching point-in-time {@link DirectoryTaxonomyReader}. */
    public final DirectoryTaxonomyReader taxonomyReader;

    /** Create a SearcherAndTaxonomy */
    public SearcherAndTaxonomy(IndexSearcher searcher, DirectoryTaxonomyReader taxonomyReader) {
      this.searcher = searcher;
      this.taxonomyReader = taxonomyReader;
    }
  }

  private final SearcherFactory searcherFactory;
  private final long taxoEpoch;
  private final DirectoryTaxonomyWriter taxoWriter;

  /** Creates near-real-time searcher and taxonomy reader
   *  from the corresponding writers. */
  public SearcherTaxonomyManager(IndexWriter writer, SearcherFactory searcherFactory, DirectoryTaxonomyWriter taxoWriter) throws IOException {
    this(writer, true, searcherFactory, taxoWriter);
  }

  /** Expert: creates near-real-time searcher and taxonomy reader
   *  from the corresponding writers, controlling whether deletes should be applied. */
  public SearcherTaxonomyManager(IndexWriter writer, boolean applyAllDeletes, SearcherFactory searcherFactory, DirectoryTaxonomyWriter taxoWriter) throws IOException {
    if (searcherFactory == null) {
      searcherFactory = new SearcherFactory();
    }
    this.searcherFactory = searcherFactory;
    this.taxoWriter = taxoWriter;
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    current = new SearcherAndTaxonomy(SearcherManager.getSearcher(searcherFactory, DirectoryReader.open(writer, applyAllDeletes), null), taxoReader);
    this.taxoEpoch = taxoWriter.getTaxonomyEpoch();
  }

  /**
   * Creates search and taxonomy readers over the corresponding directories.
   * 
   * <p>
   * <b>NOTE:</b> you should only use this constructor if you commit and call
   * {@link #maybeRefresh()} in the same thread. Otherwise it could lead to an
   * unsync'd {@link IndexSearcher} and {@link TaxonomyReader} pair.
   */
  public SearcherTaxonomyManager(Directory indexDir, Directory taxoDir, SearcherFactory searcherFactory) throws IOException {
    if (searcherFactory == null) {
      searcherFactory = new SearcherFactory();
    }
    this.searcherFactory = searcherFactory;
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    current = new SearcherAndTaxonomy(SearcherManager.getSearcher(searcherFactory, DirectoryReader.open(indexDir), null), taxoReader);
    this.taxoWriter = null;
    taxoEpoch = -1;
  }

  @Override
  protected void decRef(SearcherAndTaxonomy ref) throws IOException {
    ref.searcher.getIndexReader().decRef();

    // This decRef can fail, and then in theory we should
    // tryIncRef the searcher to put back the ref count
    // ... but 1) the below decRef should only fail because
    // it decRef'd to 0 and closed and hit some IOException
    // during close, in which case 2) very likely the
    // searcher was also just closed by the above decRef and
    // a tryIncRef would fail:
    ref.taxonomyReader.decRef();
  }

  @Override
  protected boolean tryIncRef(SearcherAndTaxonomy ref) throws IOException {
    if (ref.searcher.getIndexReader().tryIncRef()) {
      if (ref.taxonomyReader.tryIncRef()) {
        return true;
      } else {
        ref.searcher.getIndexReader().decRef();
      }
    }
    return false;
  }

  @Override
  protected SearcherAndTaxonomy refreshIfNeeded(SearcherAndTaxonomy ref) throws IOException {
    // Must re-open searcher first, otherwise we may get a
    // new reader that references ords not yet known to the
    // taxonomy reader:
    final IndexReader r = ref.searcher.getIndexReader();
    final IndexReader newReader = DirectoryReader.openIfChanged((DirectoryReader) r);
    if (newReader == null) {
      return null;
    } else {
      DirectoryTaxonomyReader tr = TaxonomyReader.openIfChanged(ref.taxonomyReader);
      if (tr == null) {
        ref.taxonomyReader.incRef();
        tr = ref.taxonomyReader;
      } else if (taxoWriter != null && taxoWriter.getTaxonomyEpoch() != taxoEpoch) {
        IOUtils.close(newReader, tr);
        throw new IllegalStateException("DirectoryTaxonomyWriter.replaceTaxonomy was called, which is not allowed when using SearcherTaxonomyManager");
      }

      return new SearcherAndTaxonomy(SearcherManager.getSearcher(searcherFactory, newReader, r), tr);
    }
  }

  @Override
  protected int getRefCount(SearcherAndTaxonomy reference) {
    return reference.searcher.getIndexReader().getRefCount();
  }
}
