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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.IntsRef;

import java.io.IOException;

import static org.apache.lucene.facet.taxonomy.TaxonomyReader.INVALID_ORDINAL;
import static org.apache.lucene.facet.taxonomy.TaxonomyReader.ROOT_ORDINAL;

/**
 * Utility class to easily retrieve previously indexed facet labels, allowing you to skip also adding stored fields for these values,
 * reducing your index size.
 *
 * @lucene.experimental
 **/
public class TaxonomyFacetLabels {

  /**
   * Index field name provided to the constructor
   */
  private final String indexFieldName;

  /**
   * {@code TaxonomyReader} provided to the constructor
   */
  private final TaxonomyReader taxoReader;


  /**
   * {@code OrdinalsReader} to decode ordinals previously indexed into the {@code BinaryDocValues} facet field
   */
  private final OrdinalsReader ordsReader;

  /**
   * Sole constructor.  Do not close the provided {@link TaxonomyReader} while still using this instance!
   */
  public TaxonomyFacetLabels(TaxonomyReader taxoReader, String indexFieldName) throws IOException {
    this.taxoReader = taxoReader;
    this.indexFieldName = indexFieldName;
    this.ordsReader = new DocValuesOrdinalsReader(indexFieldName);
  }

  /**
   * Create and return an instance of {@link FacetLabelReader} to retrieve facet labels for
   * multiple documents and (optionally) for a specific dimension.  You must create this per-segment,
   * and then step through all hits, in order, for that segment.
   *
   * <p><b>NOTE</b>: This class is not thread-safe, so you must use a new instance of this
   * class for each thread.</p>
   *
   * @param readerContext LeafReaderContext used to access the {@code BinaryDocValues} facet field
   * @return an instance of {@link FacetLabelReader}
   * @throws IOException when a low-level IO issue occurs
   */
  public FacetLabelReader getFacetLabelReader(LeafReaderContext readerContext) throws IOException {
    return new FacetLabelReader(ordsReader, readerContext);
  }

  /**
   * Utility class to retrieve facet labels for multiple documents.
   *
   * @lucene.experimental
   */
  public class FacetLabelReader {
    private final OrdinalsReader.OrdinalsSegmentReader ordinalsSegmentReader;
    private final IntsRef decodedOrds = new IntsRef();
    private int currentDocId = -1;
    private int currentPos = -1;

    // Lazily set when nextFacetLabel(int docId, String facetDimension) is first called
    private int[] parents;

    /**
     * Sole constructor.
     */
    public FacetLabelReader(OrdinalsReader ordsReader, LeafReaderContext readerContext) throws IOException {
      ordinalsSegmentReader = ordsReader.getReader(readerContext);
    }

    /**
     * Retrieves the next {@link FacetLabel} for the specified {@code docId}, or {@code null} if there are no more.
     * This method has state: if the provided {@code docId} is the same as the previous invocation, it returns the
     * next {@link FacetLabel} for that document.  Otherwise, it advances to the new {@code docId} and provides the
     * first {@link FacetLabel} for that document, or {@code null} if that document has no indexed facets.  Each
     * new {@code docId} must be in strictly monotonic (increasing) order.
     *
     * <p><b>NOTE</b>: The returned FacetLabels may not be in the same order in which they were indexed</p>
     *
     * @param docId input docId provided in monotonic (non-decreasing) order
     * @return the first or next {@link FacetLabel}, or {@code null} if there are no more
     * @throws IOException when a low-level IO issue occurs
     * @throws IllegalArgumentException if docId provided is less than docId supplied in an earlier invocation
     */
    public FacetLabel nextFacetLabel(int docId) throws IOException {
      if (currentDocId != docId) {
        if (docId < currentDocId) {
          throw new IllegalArgumentException("docs out of order: previous docId=" + currentDocId
              + " current docId=" + docId);
        }
        ordinalsSegmentReader.get(docId, decodedOrds);
        currentDocId = docId;
        currentPos = decodedOrds.offset;
      }

      int endPos = decodedOrds.offset + decodedOrds.length;
      assert currentPos <= endPos;

      if (currentPos == endPos) {
        // no more FacetLabels
        return null;
      }

      int ord = decodedOrds.ints[currentPos++];
      return taxoReader.getPath(ord);
    }

    private boolean isDescendant(int ord, int ancestorOrd) {
      while (ord != INVALID_ORDINAL && ord != ROOT_ORDINAL) {
        if (parents[ord] == ancestorOrd) {
          return true;
        }
        ord = parents[ord];
      }
      return false;
    }

    /**
     * Retrieves the next {@link FacetLabel} for the specified {@code docId} under the requested {@code facetDimension},
     * or {@code null} if there are no more. This method has state: if the provided {@code docId} is the same as the
     * previous invocation, it returns the next {@link FacetLabel} for that document.  Otherwise, it advances to
     * the new {@code docId} and provides the first {@link FacetLabel} for that document, or {@code null} if that document
     * has no indexed facets.  Each new {@code docId} must be in strictly monotonic (increasing) order.
     *
     * <p><b>NOTE</b>: This method loads the {@code int[] parents} array from the taxonomy index.
     * The returned FacetLabels may not be in the same order in which they were indexed.</p>
     *
     * @param docId input docId provided in non-decreasing order
     * @return the first or next {@link FacetLabel}, or {@code null} if there are no more
     * @throws IOException if {@link TaxonomyReader} has problems getting path for an ordinal
     * @throws IllegalArgumentException if docId provided is less than docId supplied in an earlier invocation
     * @throws IllegalArgumentException if facetDimension is null
     */
    public FacetLabel nextFacetLabel(int docId, String facetDimension) throws IOException {
      if (facetDimension == null) {
        throw new IllegalArgumentException("Input facet dimension cannot be null");
      }
      final int parentOrd = taxoReader.getOrdinal(new FacetLabel(facetDimension));
      if (parentOrd == INVALID_ORDINAL) {
        throw new IllegalArgumentException("Category ordinal not found for facet dimension: " + facetDimension);
      }

      if (currentDocId != docId) {
        if (docId < currentDocId) {
          throw new IllegalArgumentException("docs out of order: previous docId=" + currentDocId
              + " current docId=" + docId);
        }
        ordinalsSegmentReader.get(docId, decodedOrds);
        currentPos = decodedOrds.offset;
        currentDocId = docId;
      }

      if (parents == null) {
        parents = taxoReader.getParallelTaxonomyArrays().parents();
      }

      int endPos = decodedOrds.offset + decodedOrds.length;
      assert currentPos <= endPos;

      for (; currentPos < endPos; ) {
        int ord = decodedOrds.ints[currentPos++];
        if (isDescendant(ord, parentOrd) == true) {
          return taxoReader.getPath(ord);
        }
      }
      return null;
    }
  }
}
