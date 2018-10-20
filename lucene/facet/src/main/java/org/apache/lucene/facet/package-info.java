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
 * Faceted search.
 * <p>
 * This module provides multiple methods for computing facet counts and
 * value aggregations:
 * <ul>
 *   <li> Taxonomy-based methods rely on a separate taxonomy index to
 *        map hierarchical facet paths to global int ordinals for fast
 *        counting at search time; these methods can compute counts
 *        (({@link org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts}, {@link
 *        org.apache.lucene.facet.taxonomy.TaxonomyFacetCounts}) aggregate long or double values {@link
 *        org.apache.lucene.facet.taxonomy.TaxonomyFacetSumIntAssociations}, {@link
 *        org.apache.lucene.facet.taxonomy.TaxonomyFacetSumFloatAssociations}, {@link
 *        org.apache.lucene.facet.taxonomy.TaxonomyFacetSumValueSource}.  Add {@link org.apache.lucene.facet.FacetField} or
 *        {@link org.apache.lucene.facet.taxonomy.AssociationFacetField} to your documents at index time
 *        to use taxonomy-based methods.
 * 
 *   <li> Sorted-set doc values method does not require a separate
 *        taxonomy index, and computes counts based on sorted set doc
 *        values fields ({@link org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts}).  Add
 *        {@link org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField} to your documents at
 *        index time to use sorted set facet counts.
 * 
 *  <li> Range faceting {@link org.apache.lucene.facet.range.LongRangeFacetCounts}, {@link
 *       org.apache.lucene.facet.range.DoubleRangeFacetCounts} compute counts for a dynamic numeric
 *       range from a provided {@link org.apache.lucene.search.LongValuesSource} (previously indexed
 *       numeric field, or a dynamic expression such as distance).
 * </ul>
 * <p>
 * At search time you first run your search, but pass a {@link
 * org.apache.lucene.facet.FacetsCollector} to gather all hits (and optionally, scores for each
 * hit).  Then, instantiate whichever facet methods you'd like to use
 * to compute aggregates.  Finally, all methods implement a common
 * {@link org.apache.lucene.facet.Facets} base API that you use to obtain specific facet
 * counts.
 * </p>
 * <p>
 * The various {@link org.apache.lucene.facet.FacetsCollector#search} utility methods are
 * useful for doing an "ordinary" search (sorting by score, or by a
 * specified Sort) but also collecting into a {@link org.apache.lucene.facet.FacetsCollector} for
 * subsequent faceting.
 * </p>
 */
package org.apache.lucene.facet;
