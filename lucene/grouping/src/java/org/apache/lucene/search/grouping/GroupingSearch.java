package org.apache.lucene.search.grouping;

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

import java.io.IOException;
import java.util.*;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.*;
import org.apache.lucene.search.grouping.function.FunctionAllGroupHeadsCollector;
import org.apache.lucene.search.grouping.function.FunctionAllGroupsCollector;
import org.apache.lucene.search.grouping.function.FunctionFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.function.FunctionSecondPassGroupingCollector;
import org.apache.lucene.search.grouping.term.TermAllGroupHeadsCollector;
import org.apache.lucene.search.grouping.term.TermAllGroupsCollector;
import org.apache.lucene.search.grouping.term.TermFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.term.TermSecondPassGroupingCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.mutable.MutableValue;

/**
 * Convenience class to perform grouping in a non distributed environment.
 *
 * @lucene.experimental
 */
public class GroupingSearch {

  private final String groupField;
  private final ValueSource groupFunction;
  private final Map<?, ?> valueSourceContext;
  private final Filter groupEndDocs;

  private Sort groupSort = Sort.RELEVANCE;
  private Sort sortWithinGroup;

  private int groupDocsOffset;
  private int groupDocsLimit = 1;
  private boolean fillSortFields;
  private boolean includeScores = true;
  private boolean includeMaxScore = true;

  private Double maxCacheRAMMB;
  private Integer maxDocsToCache;
  private boolean cacheScores;
  private boolean allGroups;
  private boolean allGroupHeads;
  private int initialSize = 128;

  private Collection<?> matchingGroups;
  private Bits matchingGroupHeads;

  /**
   * Constructs a <code>GroupingSearch</code> instance that groups documents by index terms using the {@link FieldCache}.
   * The group field can only have one token per document. This means that the field must not be analysed.
   *
   * @param groupField The name of the field to group by.
   */
  public GroupingSearch(String groupField) {
    this(groupField, null, null, null);
  }

  /**
   * Constructs a <code>GroupingSearch</code> instance that groups documents by function using a {@link ValueSource}
   * instance.
   *
   * @param groupFunction      The function to group by specified as {@link ValueSource}
   * @param valueSourceContext The context of the specified groupFunction
   */
  public GroupingSearch(ValueSource groupFunction, Map<?, ?> valueSourceContext) {
    this(null, groupFunction, valueSourceContext, null);
  }

  /**
   * Constructor for grouping documents by doc block.
   * This constructor can only be used when documents belonging in a group are indexed in one block.
   *
   * @param groupEndDocs The filter that marks the last document in all doc blocks
   */
  public GroupingSearch(Filter groupEndDocs) {
    this(null, null, null, groupEndDocs);
  }

  private GroupingSearch(String groupField, ValueSource groupFunction, Map<?, ?> valueSourceContext, Filter groupEndDocs) {
    this.groupField = groupField;
    this.groupFunction = groupFunction;
    this.valueSourceContext = valueSourceContext;
    this.groupEndDocs = groupEndDocs;
  }

  /**
   * Executes a grouped search. Both the first pass and second pass are executed on the specified searcher.
   *
   * @param searcher    The {@link org.apache.lucene.search.IndexSearcher} instance to execute the grouped search on.
   * @param query       The query to execute with the grouping
   * @param groupOffset The group offset
   * @param groupLimit  The number of groups to return from the specified group offset
   * @return the grouped result as a {@link TopGroups} instance
   * @throws IOException If any I/O related errors occur
   */
  public <T> TopGroups<T> search(IndexSearcher searcher, Query query, int groupOffset, int groupLimit) throws IOException {
    return search(searcher, null, query, groupOffset, groupLimit);
  }

  /**
   * Executes a grouped search. Both the first pass and second pass are executed on the specified searcher.
   *
   * @param searcher    The {@link org.apache.lucene.search.IndexSearcher} instance to execute the grouped search on.
   * @param filter      The filter to execute with the grouping
   * @param query       The query to execute with the grouping
   * @param groupOffset The group offset
   * @param groupLimit  The number of groups to return from the specified group offset
   * @return the grouped result as a {@link TopGroups} instance
   * @throws IOException If any I/O related errors occur
   */
  @SuppressWarnings("unchecked")
  public <T> TopGroups<T> search(IndexSearcher searcher, Filter filter, Query query, int groupOffset, int groupLimit) throws IOException {
    if (groupField != null || groupFunction != null) {
      return groupByFieldOrFunction(searcher, filter, query, groupOffset, groupLimit);
    } else if (groupEndDocs != null) {
      return (TopGroups<T>) groupByDocBlock(searcher, filter, query, groupOffset, groupLimit);
    } else {
      throw new IllegalStateException("Either groupField, groupFunction or groupEndDocs must be set."); // This can't happen...
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected TopGroups groupByFieldOrFunction(IndexSearcher searcher, Filter filter, Query query, int groupOffset, int groupLimit) throws IOException {
    int topN = groupOffset + groupLimit;
    final AbstractFirstPassGroupingCollector firstPassCollector;
    final AbstractAllGroupsCollector allGroupsCollector;
    final AbstractAllGroupHeadsCollector allGroupHeadsCollector;
    if (groupFunction != null) {
      firstPassCollector = new FunctionFirstPassGroupingCollector(groupFunction, valueSourceContext, groupSort, topN);
      if (allGroups) {
        allGroupsCollector = new FunctionAllGroupsCollector(groupFunction, valueSourceContext);
      } else {
        allGroupsCollector = null;
      }
      if (allGroupHeads) {
        allGroupHeadsCollector = new FunctionAllGroupHeadsCollector(groupFunction, valueSourceContext, sortWithinGroup);
      } else {
        allGroupHeadsCollector = null;
      }
    } else {
      firstPassCollector = new TermFirstPassGroupingCollector(groupField, groupSort, topN);
      if (allGroups) {
        allGroupsCollector = new TermAllGroupsCollector(groupField, initialSize);
      } else {
        allGroupsCollector = null;
      }
      if (allGroupHeads) {
        allGroupHeadsCollector = TermAllGroupHeadsCollector.create(groupField, sortWithinGroup, initialSize);
      } else {
        allGroupHeadsCollector = null;
      }
    }

    final Collector firstRound;
    if (allGroupHeads || allGroups) {
      List<Collector> collectors = new ArrayList<Collector>();
      collectors.add(firstPassCollector);
      if (allGroups) {
        collectors.add(allGroupsCollector);
      }
      if (allGroupHeads) {
        collectors.add(allGroupHeadsCollector);
      }
      firstRound = MultiCollector.wrap(collectors.toArray(new Collector[collectors.size()]));
    } else {
      firstRound = firstPassCollector;
    }

    CachingCollector cachedCollector = null;
    if (maxCacheRAMMB != null || maxDocsToCache != null) {
      if (maxCacheRAMMB != null) {
        cachedCollector = CachingCollector.create(firstRound, cacheScores, maxCacheRAMMB);
      } else {
        cachedCollector = CachingCollector.create(firstRound, cacheScores, maxDocsToCache);
      }
      searcher.search(query, filter, cachedCollector);
    } else {
      searcher.search(query, filter, firstRound);
    }

    if (allGroups) {
      matchingGroups = allGroupsCollector.getGroups();
    } else {
      matchingGroups = Collections.emptyList();
    }
    if (allGroupHeads) {
      matchingGroupHeads = allGroupHeadsCollector.retrieveGroupHeads(searcher.getIndexReader().maxDoc());
    } else {
      matchingGroupHeads = new Bits.MatchNoBits(searcher.getIndexReader().maxDoc());
    }

    Collection<SearchGroup> topSearchGroups = firstPassCollector.getTopGroups(groupOffset, fillSortFields);
    if (topSearchGroups == null) {
      return new TopGroups(new SortField[0], new SortField[0], 0, 0, new GroupDocs[0], Float.NaN);
    }

    int topNInsideGroup = groupDocsOffset + groupDocsLimit;
    AbstractSecondPassGroupingCollector secondPassCollector;
    if (groupFunction != null) {
      secondPassCollector = new FunctionSecondPassGroupingCollector((Collection) topSearchGroups, groupSort, sortWithinGroup, topNInsideGroup, includeScores, includeMaxScore, fillSortFields, groupFunction, valueSourceContext);
    } else {
      secondPassCollector = new TermSecondPassGroupingCollector(groupField, (Collection) topSearchGroups, groupSort, sortWithinGroup, topNInsideGroup, includeScores, includeMaxScore, fillSortFields);
    }

    if (cachedCollector != null && cachedCollector.isCached()) {
      cachedCollector.replay(secondPassCollector);
    } else {
      searcher.search(query, filter, secondPassCollector);
    }

    if (allGroups) {
      return new TopGroups(secondPassCollector.getTopGroups(groupDocsOffset), matchingGroups.size());
    } else {
      return secondPassCollector.getTopGroups(groupDocsOffset);
    }
  }

  protected TopGroups<?> groupByDocBlock(IndexSearcher searcher, Filter filter, Query query, int groupOffset, int groupLimit) throws IOException {
    int topN = groupOffset + groupLimit;
    BlockGroupingCollector c = new BlockGroupingCollector(groupSort, topN, includeScores, groupEndDocs);
    searcher.search(query, filter, c);
    int topNInsideGroup = groupDocsOffset + groupDocsLimit;
    return c.getTopGroups(sortWithinGroup, groupOffset, groupDocsOffset, topNInsideGroup, fillSortFields);
  }

  /**
   * Enables caching for the second pass search. The cache will not grow over a specified limit in MB.
   * The cache is filled during the first pass searched and then replayed during the second pass searched.
   * If the cache grows beyond the specified limit, then the cache is purged and not used in the second pass search.
   *
   * @param maxCacheRAMMB The maximum amount in MB the cache is allowed to hold
   * @param cacheScores   Whether to cache the scores
   * @return <code>this</code>
   */
  public GroupingSearch setCachingInMB(double maxCacheRAMMB, boolean cacheScores) {
    this.maxCacheRAMMB = maxCacheRAMMB;
    this.maxDocsToCache = null;
    this.cacheScores = cacheScores;
    return this;
  }

  /**
   * Enables caching for the second pass search. The cache will not contain more than the maximum specified documents.
   * The cache is filled during the first pass searched and then replayed during the second pass searched.
   * If the cache grows beyond the specified limit, then the cache is purged and not used in the second pass search.
   *
   * @param maxDocsToCache The maximum number of documents the cache is allowed to hold
   * @param cacheScores    Whether to cache the scores
   * @return <code>this</code>
   */
  public GroupingSearch setCaching(int maxDocsToCache, boolean cacheScores) {
    this.maxDocsToCache = maxDocsToCache;
    this.maxCacheRAMMB = null;
    this.cacheScores = cacheScores;
    return this;
  }

  /**
   * Disables any enabled cache.
   *
   * @return <code>this</code>
   */
  public GroupingSearch disableCaching() {
    this.maxCacheRAMMB = null;
    this.maxDocsToCache = null;
    return this;
  }

  /**
   * Specifies how groups are sorted.
   * Defaults to {@link Sort#RELEVANCE}.
   *
   * @param groupSort The sort for the groups.
   * @return <code>this</code>
   */
  public GroupingSearch setGroupSort(Sort groupSort) {
    this.groupSort = groupSort;
    return this;
  }

  /**
   * Specified how documents inside a group are sorted.
   * Defaults to {@link Sort#RELEVANCE}.
   *
   * @param sortWithinGroup The sort for documents inside a group
   * @return <code>this</code>
   */
  public GroupingSearch setSortWithinGroup(Sort sortWithinGroup) {
    this.sortWithinGroup = sortWithinGroup;
    return this;
  }

  /**
   * Specifies the offset for documents inside a group.
   *
   * @param groupDocsOffset The offset for documents inside a
   * @return <code>this</code>
   */
  public GroupingSearch setGroupDocsOffset(int groupDocsOffset) {
    this.groupDocsOffset = groupDocsOffset;
    return this;
  }

  /**
   * Specifies the number of documents to return inside a group from the specified groupDocsOffset.
   *
   * @param groupDocsLimit The number of documents to return inside a group
   * @return <code>this</code>
   */
  public GroupingSearch setGroupDocsLimit(int groupDocsLimit) {
    this.groupDocsLimit = groupDocsLimit;
    return this;
  }

  /**
   * Whether to also fill the sort fields per returned group and groups docs.
   *
   * @param fillSortFields Whether to also fill the sort fields per returned group and groups docs
   * @return <code>this</code>
   */
  public GroupingSearch setFillSortFields(boolean fillSortFields) {
    this.fillSortFields = fillSortFields;
    return this;
  }

  /**
   * Whether to include the scores per doc inside a group.
   *
   * @param includeScores Whether to include the scores per doc inside a group
   * @return <code>this</code>
   */
  public GroupingSearch setIncludeScores(boolean includeScores) {
    this.includeScores = includeScores;
    return this;
  }

  /**
   * Whether to include the score of the most relevant document per group.
   *
   * @param includeMaxScore Whether to include the score of the most relevant document per group
   * @return <code>this</code>
   */
  public GroupingSearch setIncludeMaxScore(boolean includeMaxScore) {
    this.includeMaxScore = includeMaxScore;
    return this;
  }

  /**
   * Whether to also compute all groups matching the query.
   * This can be used to determine the number of groups, which can be used for accurate pagination.
   * <p/>
   * When grouping by doc block the number of groups are automatically included in the {@link TopGroups} and this
   * option doesn't have any influence.
   *
   * @param allGroups to also compute all groups matching the query
   * @return <code>this</code>
   */
  public GroupingSearch setAllGroups(boolean allGroups) {
    this.allGroups = allGroups;
    return this;
  }

  /**
   * If {@link #setAllGroups(boolean)} was set to <code>true</code> then all matching groups are returned, otherwise
   * an empty collection is returned.
   *
   * @param <T> The group value type. This can be a {@link BytesRef} or a {@link MutableValue} instance. If grouping
   *            by doc block this the group value is always <code>null</code>.
   * @return all matching groups are returned, or an empty collection
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T> Collection<T> getAllMatchingGroups() {
    return (Collection<T>) matchingGroups;
  }

  /**
   * Whether to compute all group heads (most relevant document per group) matching the query.
   * <p/>
   * This feature isn't enabled when grouping by doc block.
   *
   * @param allGroupHeads Whether to compute all group heads (most relevant document per group) matching the query
   * @return <code>this</code>
   */
  public GroupingSearch setAllGroupHeads(boolean allGroupHeads) {
    this.allGroupHeads = allGroupHeads;
    return this;
  }

  /**
   * Returns the matching group heads if {@link #setAllGroupHeads(boolean)} was set to true or an empty bit set.
   *
   * @return The matching group heads if {@link #setAllGroupHeads(boolean)} was set to true or an empty bit set
   */
  public Bits getAllGroupHeads() {
    return matchingGroupHeads;
  }

  /**
   * Sets the initial size of some internal used data structures.
   * This prevents growing data structures many times. This can improve the performance of the grouping at the cost of
   * more initial RAM.
   * <p/>
   * The {@link #setAllGroups} and {@link #setAllGroupHeads} features use this option.
   * Defaults to 128.
   *
   * @param initialSize The initial size of some internal used data structures
   * @return <code>this</code>
   */
  public GroupingSearch setInitialSize(int initialSize) {
    this.initialSize = initialSize;
    return this;
  }
}
