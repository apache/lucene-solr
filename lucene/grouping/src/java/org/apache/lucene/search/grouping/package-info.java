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
 * Grouping.
 *
 * <p>This module enables search result grouping with Lucene, where hits with the same value in the
 * specified single-valued group field are grouped together. For example, if you group by the <code>
 * author</code> field, then all documents with the same value in the <code>author</code> field fall
 * into a single group.
 *
 * <p>Grouping requires a number of inputs:
 *
 * <ul>
 *   <li><code>groupSelector</code>: this defines how groups are created from values per-document.
 *       The grouping module ships with selectors for grouping by term, and by long and double
 *       ranges.
 *   <li><code>groupSort</code>: how the groups are sorted. For sorting purposes, each group is
 *       "represented" by the highest-sorted document according to the <code>groupSort</code> within
 *       it. For example, if you specify "price" (ascending) then the first group is the one with
 *       the lowest price book within it. Or if you specify relevance group sort, then the first
 *       group is the one containing the highest scoring book.
 *   <li><code>topNGroups</code>: how many top groups to keep. For example, 10 means the top 10
 *       groups are computed.
 *   <li><code>groupOffset</code>: which "slice" of top groups you want to retrieve. For example, 3
 *       means you'll get 7 groups back (assuming <code>topNGroups</code> is 10). This is useful for
 *       paging, where you might show 5 groups per page.
 *   <li><code>withinGroupSort</code>: how the documents within each group are sorted. This can be
 *       different from the group sort.
 *   <li><code>maxDocsPerGroup</code>: how many top documents within each group to keep.
 *   <li><code>withinGroupOffset</code>: which "slice" of top documents you want to retrieve from
 *       each group.
 * </ul>
 *
 * <p>The implementation is two-pass: the first pass ({@link
 * org.apache.lucene.search.grouping.FirstPassGroupingCollector}) gathers the top groups, and the
 * second pass ({@link org.apache.lucene.search.grouping.SecondPassGroupingCollector}) gathers
 * documents within those groups. If the search is costly to run you may want to use the {@link
 * org.apache.lucene.search.CachingCollector} class, which caches hits and can (quickly) replay them
 * for the second pass. This way you only run the query once, but you pay a RAM cost to (briefly)
 * hold all hits. Results are returned as a {@link org.apache.lucene.search.grouping.TopGroups}
 * instance.
 *
 * <p>Groups are defined by {@link org.apache.lucene.search.grouping.GroupSelector} implementations:
 *
 * <ul>
 *   <li>{@link org.apache.lucene.search.grouping.TermGroupSelector} groups based on the value of a
 *       {@link org.apache.lucene.index.SortedDocValues} field
 *   <li>{@link org.apache.lucene.search.grouping.ValueSourceGroupSelector} groups based on the
 *       value of a {@link org.apache.lucene.queries.function.ValueSource}
 *   <li>{@link org.apache.lucene.search.grouping.DoubleRangeGroupSelector} groups based on the
 *       value of a {@link org.apache.lucene.search.DoubleValuesSource}
 *   <li>{@link org.apache.lucene.search.grouping.LongRangeGroupSelector} groups based on the value
 *       of a {@link org.apache.lucene.search.LongValuesSource}
 * </ul>
 *
 * <p>Known limitations:
 *
 * <ul>
 *   <li>Sharding is not directly supported, though is not too difficult, if you can merge the top
 *       groups and top documents per group yourself.
 * </ul>
 *
 * <p>Typical usage for the generic two-pass grouping search looks like this using the grouping
 * convenience utility (optionally using caching for the second pass search):
 *
 * <pre class="prettyprint">
 *   GroupingSearch groupingSearch = new GroupingSearch("author");
 *   groupingSearch.setGroupSort(groupSort);
 *   groupingSearch.setFillSortFields(fillFields);
 *
 *   if (useCache) {
 *     // Sets cache in MB
 *     groupingSearch.setCachingInMB(4.0, true);
 *   }
 *
 *   if (requiredTotalGroupCount) {
 *     groupingSearch.setAllGroups(true);
 *   }
 *
 *   TermQuery query = new TermQuery(new Term("content", searchTerm));
 *   TopGroups&lt;BytesRef&gt; result = groupingSearch.search(indexSearcher, query, groupOffset, groupLimit);
 *
 *   // Render groupsResult...
 *   if (requiredTotalGroupCount) {
 *     int totalGroupCount = result.totalGroupCount;
 *   }
 * </pre>
 *
 * <p>To use the single-pass <code>BlockGroupingCollector</code>, first, at indexing time, you must
 * ensure all docs in each group are added as a block, and you have some way to find the last
 * document of each group. One simple way to do this is to add a marker binary field:
 *
 * <pre class="prettyprint">
 *   // Create Documents from your source:
 *   List&lt;Document&gt; oneGroup = ...;
 *
 *   Field groupEndField = new Field("groupEnd", "x", Field.Store.NO, Field.Index.NOT_ANALYZED);
 *   groupEndField.setIndexOptions(IndexOptions.DOCS_ONLY);
 *   groupEndField.setOmitNorms(true);
 *   oneGroup.get(oneGroup.size()-1).add(groupEndField);
 *
 *   // You can also use writer.updateDocuments(); just be sure you
 *   // replace an entire previous doc block with this new one.  For
 *   // example, each group could have a "groupID" field, with the same
 *   // value for all docs in this group:
 *   writer.addDocuments(oneGroup);
 * </pre>
 *
 * Then, at search time:
 *
 * <pre class="prettyprint">
 *   Query groupEndDocs = new TermQuery(new Term("groupEnd", "x"));
 *   BlockGroupingCollector c = new BlockGroupingCollector(groupSort, groupOffset+topNGroups, needsScores, groupEndDocs);
 *   s.search(new TermQuery(new Term("content", searchTerm)), c);
 *   TopGroups groupsResult = c.getTopGroups(withinGroupSort, groupOffset, docOffset, docOffset+docsPerGroup, fillFields);
 *
 *   // Render groupsResult...
 * </pre>
 *
 * Or alternatively use the <code>GroupingSearch</code> convenience utility:
 *
 * <pre class="prettyprint">
 *   // Per search:
 *   GroupingSearch groupingSearch = new GroupingSearch(groupEndDocs);
 *   groupingSearch.setGroupSort(groupSort);
 *   groupingSearch.setIncludeScores(needsScores);
 *   TermQuery query = new TermQuery(new Term("content", searchTerm));
 *   TopGroups groupsResult = groupingSearch.search(indexSearcher, query, groupOffset, groupLimit);
 *
 *   // Render groupsResult...
 * </pre>
 *
 * Note that the <code>groupValue</code> of each <code>GroupDocs</code> will be <code>null</code>,
 * so if you need to present this value you'll have to separately retrieve it (for example using
 * stored fields, <code>FieldCache</code>, etc.).
 *
 * <p>Another collector is the <code>AllGroupHeadsCollector</code> that can be used to retrieve all
 * most relevant documents per group. Also known as group heads. This can be useful in situations
 * when one wants to compute group based facets / statistics on the complete query result. The
 * collector can be executed during the first or second phase. This collector can also be used with
 * the <code>GroupingSearch</code> convenience utility, but when if one only wants to compute the
 * most relevant documents per group it is better to just use the collector as done here below.
 *
 * <pre class="prettyprint">
 *   TermGroupSelector grouper = new TermGroupSelector(groupField);
 *   AllGroupHeadsCollector c = AllGroupHeadsCollector.newCollector(grouper, sortWithinGroup);
 *   s.search(new TermQuery(new Term("content", searchTerm)), c);
 *   // Return all group heads as int array
 *   int[] groupHeadsArray = c.retrieveGroupHeads()
 *   // Return all group heads as FixedBitSet.
 *   int maxDoc = s.maxDoc();
 *   FixedBitSet groupHeadsBitSet = c.retrieveGroupHeads(maxDoc)
 * </pre>
 */
package org.apache.lucene.search.grouping;
