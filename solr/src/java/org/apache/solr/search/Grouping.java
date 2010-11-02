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

package org.apache.solr.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.function.DocValues;
import org.apache.solr.search.function.ValueSource;

import java.io.IOException;
import java.util.*;

public class Grouping {


  public abstract class Command {
    public String key;       // the name to use for this group in the response
    public Sort groupSort;   // the sort of the documents *within* a single group.
    public Sort sort;        // the sort between groups
    public int docsPerGroup; // how many docs in each group - from "group.limit" param, default=1
    public int groupOffset;  // the offset within each group (for paging within each group)
    public int numGroups;    // how many groups - defaults to the "rows" parameter
    public int offset;       // offset into the list of groups


    abstract void prepare() throws IOException;
    abstract Collector createCollector() throws IOException;
    Collector createNextCollector() throws IOException {
      return null;
    }
    abstract void finish() throws IOException;

    abstract int getMatches();

    NamedList commonResponse() {
      NamedList groupResult = new SimpleOrderedMap();
      grouped.add(key, groupResult);  // grouped={ key={

      int this_matches = getMatches();
      groupResult.add("matches", this_matches);
      maxMatches = Math.max(maxMatches, this_matches);
      return groupResult;
    }

    DocList getDocList(TopDocsCollector collector) {
      int docsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);

      // TODO: implement a DocList impl that doesn't need to start at offset=0
      TopDocs topDocs = collector.topDocs(0, docsToCollect);

      int ids[] = new int[topDocs.scoreDocs.length];
      float[] scores = needScores ? new float[topDocs.scoreDocs.length] : null;
      for (int i=0; i<ids.length; i++) {
        ids[i] = topDocs.scoreDocs[i].doc;
        if (scores != null)
          scores[i] = topDocs.scoreDocs[i].score;
      }

      float score = topDocs.getMaxScore();
      maxScore = Math.max(maxScore, score);
      DocSlice docs = new DocSlice(groupOffset, Math.max(0, ids.length - groupOffset), ids, scores, topDocs.totalHits, score);

      if (getDocList) {
        DocIterator iter = docs.iterator();
        while (iter.hasNext())
          idSet.add(iter.nextDoc());
      }
      return docs;
    }

    void addDocList(NamedList rsp, TopDocsCollector collector) {
      rsp.add("doclist", getDocList(collector));
    }
  }

  public class CommandQuery extends Command {
    public Query query;

    TopDocsCollector topCollector;
    FilterCollector collector;

    @Override
    void prepare() throws IOException {
    }

    @Override
    Collector createCollector() throws IOException {
      int docsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      DocSet groupFilt = searcher.getDocSet(query);
      topCollector = newCollector(groupSort, docsToCollect, false, needScores);
      collector = new FilterCollector(groupFilt, topCollector);
      return collector;
    }

    @Override
    void finish() throws IOException {
      NamedList rsp = commonResponse();
      addDocList(rsp, (TopDocsCollector)collector.getCollector());
    }

    @Override
    int getMatches() {
      return collector.getMatches();
    }
  }

  
  public class CommandFunc extends Command {
    public ValueSource groupBy;


    int maxGroupToFind;
    Map context;
    TopGroupCollector collector = null;
    Phase2GroupCollector collector2;
    
    @Override
    void prepare() throws IOException {
        Map context = ValueSource.newContext();
        groupBy.createWeight(context, searcher);
    }

    @Override
    Collector createCollector() throws IOException {
      maxGroupToFind = getMax(offset, numGroups, maxDoc);

      if (compareSorts(sort, groupSort)) {
        collector = new TopGroupSortCollector(groupBy, context, normalizeSort(sort), normalizeSort(groupSort), maxGroupToFind);
      } else {
        collector = new TopGroupCollector(groupBy, context, normalizeSort(sort), maxGroupToFind);
      }
      return collector;
    }

    @Override
    Collector createNextCollector() throws IOException {
      int docsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      if (docsToCollect < 0 || docsToCollect > maxDoc) docsToCollect = maxDoc;

      collector2 = new Phase2GroupCollector(collector, groupBy, context, groupSort, docsToCollect, needScores, offset);
      return collector2;
    }

    @Override
    void finish() throws IOException {
      NamedList groupResult = commonResponse();

      if (collector.orderedGroups == null) collector.buildSet();

      List groupList = new ArrayList();
      groupResult.add("groups", groupList);        // grouped={ key={ groups=[

      int skipCount = offset;
      for (SearchGroup group : collector.orderedGroups) {
        if (skipCount > 0) {
          skipCount--;
          continue;
        }
        NamedList nl = new SimpleOrderedMap();
        groupList.add(nl);                         // grouped={ key={ groups=[ {

        nl.add("groupValue", group.groupValue.toObject());

        SearchGroupDocs groupDocs = collector2.groupMap.get(group.groupValue);
        addDocList(nl, groupDocs.collector);
      }
    }

    @Override
    int getMatches() {
      return collector.getMatches();
    }
  }



  static Sort byScoreDesc = new Sort();

  static boolean compareSorts(Sort sort1, Sort sort2) {
    return sort1 == sort2 || normalizeSort(sort1).equals(normalizeSort(sort2)); 
  }

  /** returns a sort by score desc if null */
  static Sort normalizeSort(Sort sort) {
    return sort==null ? byScoreDesc : sort;
  } 

  static int getMax(int offset, int len, int max) {
    int v = len<0 ? max : offset + len;
    if (v < 0 || v > max) v = max;
    return v;
  }

  static TopDocsCollector newCollector(Sort sort, int numHits, boolean fillFields, boolean needScores) throws IOException {
    if (sort==null || sort==byScoreDesc) {
      return TopScoreDocCollector.create(numHits, true);
    } else {
      return TopFieldCollector.create(sort, numHits, false, needScores, needScores, true);
    }
  }


  final SolrIndexSearcher searcher;
  final SolrIndexSearcher.QueryResult qr;
  final SolrIndexSearcher.QueryCommand cmd;
  final List<Command> commands = new ArrayList<Command>();

  public Grouping(SolrIndexSearcher searcher, SolrIndexSearcher.QueryResult qr, SolrIndexSearcher.QueryCommand cmd) {
    this.searcher = searcher;
    this.qr = qr;
    this.cmd = cmd;
  }

  public void add(Grouping.Command groupingCommand) {
    commands.add(groupingCommand);
  }

  int maxDoc;
  boolean needScores;
  boolean getDocSet;
  boolean getDocList; // doclist needed for debugging or highlighting
  Query query;
  DocSet filter;
  Filter luceneFilter;
  NamedList grouped = new SimpleOrderedMap();
  Set<Integer> idSet = new LinkedHashSet<Integer>();  // used for tracking unique docs when we need a doclist
  int maxMatches;  // max number of matches from any grouping command  
  float maxScore = Float.NEGATIVE_INFINITY;  // max score seen in any doclist
  
  public void execute() throws IOException {
    DocListAndSet out = new DocListAndSet();
    qr.setDocListAndSet(out);

    filter = cmd.getFilter()!=null ? cmd.getFilter() : searcher.getDocSet(cmd.getFilterList());

    maxDoc = searcher.maxDoc();

    needScores = (cmd.getFlags() & SolrIndexSearcher.GET_SCORES) != 0;
    getDocSet = (cmd.getFlags() & SolrIndexSearcher.GET_DOCSET) != 0;
    getDocList = (cmd.getFlags() & SolrIndexSearcher.GET_DOCLIST) != 0; // doclist needed for debugging or highlighting
    query = QueryUtils.makeQueryable(cmd.getQuery());

    for (Command cmd : commands) {
      cmd.prepare();
    }
    
    List<Collector> collectors = new ArrayList<Collector>(commands.size());
    for (Command cmd : commands) {
      Collector collector = cmd.createCollector();
      if (collector != null)
        collectors.add(collector);
    }

    Collector allCollectors = MultiCollector.wrap(collectors.toArray(new Collector[collectors.size()]));
    DocSetCollector setCollector = null;
    if (getDocSet) {
      setCollector = new DocSetDelegateCollector(maxDoc>>6, maxDoc, allCollectors);
      allCollectors = setCollector;
    }

    searcher.search(query, luceneFilter, allCollectors);

    if (getDocSet) {
      qr.setDocSet(setCollector.getDocSet());
    }

    collectors.clear();
    for (Command cmd : commands) {
      Collector collector = cmd.createNextCollector();
      if (collector != null)
        collectors.add(collector);
    }

    if (collectors.size() > 0) {
      searcher.search(query, luceneFilter, MultiCollector.wrap(collectors.toArray(new Collector[collectors.size()])));
    }

    for (Command cmd : commands) {
      cmd.finish();
    }

    qr.groupedResults = grouped;

    if (getDocList) {
      int sz = idSet.size();
      int[] ids = new int[sz];
      int idx = 0;
      for (int val : idSet) {
        ids[idx++] = val;
      }
      qr.setDocList(new DocSlice(0, sz, ids, null, maxMatches, maxScore));
    }
  }

}


class SearchGroup {
  public MutableValue groupValue;
  int matches;
  int topDoc;
  // float topDocScore;  // currently unused
  int comparatorSlot;

  // currently only used when sort != sort.group
  FieldComparator[] sortGroupComparators;
  int[] sortGroupReversed;

  /***
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return groupValue.equalsSameType(((SearchGroup)obj).groupValue);
  }
  ***/
}

abstract class GroupCollector extends Collector {
  /** get the number of matches before grouping or limiting have been applied */
  public abstract int getMatches();
}

class FilterCollector extends GroupCollector {
  private final DocSet filter;
  private final Collector collector;
  private int docBase;
  private int matches;

  public FilterCollector(DocSet filter, Collector collector) throws IOException {
    this.filter = filter;
    this.collector = collector;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    collector.setScorer(scorer);
  }

  @Override
  public void collect(int doc) throws IOException {
    matches++;
    if (filter.exists(doc + docBase))
      collector.collect(doc);
  }

  @Override
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    this.docBase = docBase;
    collector.setNextReader(reader, docBase);
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return collector.acceptsDocsOutOfOrder();
  }

  @Override
  public int getMatches() {
    return matches;
  }

  Collector getCollector() {
    return collector;
  }
}




/** Finds the top set of groups, grouped by groupByVS when sort == group.sort */
class TopGroupCollector extends GroupCollector {
  final int nGroups;
  final HashMap<MutableValue, SearchGroup> groupMap;
  TreeSet<SearchGroup> orderedGroups;
  final ValueSource vs;
  final Map context;
  final FieldComparator[] comparators;
  final int[] reversed;

  DocValues docValues;
  DocValues.ValueFiller filler;
  MutableValue mval;
  Scorer scorer;
  int docBase;
  int spareSlot;

  int matches;

  public TopGroupCollector(ValueSource groupByVS, Map vsContext, Sort sort, int nGroups) throws IOException {
    this.vs = groupByVS;
    this.context = vsContext;
    this.nGroups = nGroups;

    SortField[] sortFields = sort.getSort();
    this.comparators = new FieldComparator[sortFields.length];
    this.reversed = new int[sortFields.length];
    for (int i = 0; i < sortFields.length; i++) {
      SortField sortField = sortFields[i];
      reversed[i] = sortField.getReverse() ? -1 : 1;
      // use nGroups + 1 so we have a spare slot to use for comparing (tracked by this.spareSlot)
      comparators[i] = sortField.getComparator(nGroups + 1, i);
    }
    this.spareSlot = nGroups;

    this.groupMap = new HashMap<MutableValue, SearchGroup>(nGroups);
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
    for (FieldComparator fc : comparators)
      fc.setScorer(scorer);
  }

  @Override
  public void collect(int doc) throws IOException {
    matches++;

    // if orderedGroups != null, then we already have collected N groups and
    // can short circuit by comparing this document to the smallest group
    // without having to even find what group this document belongs to.
    // Even if this document belongs to a group in the top N, we know that
    // we don't have to update that group.
    //
    // Downside: if the number of unique groups is very low, this is
    // wasted effort as we will most likely be updating an existing group.
    if (orderedGroups != null) {
      for (int i = 0;; i++) {
        final int c = reversed[i] * comparators[i].compareBottom(doc);
        if (c < 0) {
          // Definitely not competitive. So don't even bother to continue
          return;
        } else if (c > 0) {
          // Definitely competitive.
          break;
        } else if (i == comparators.length - 1) {
          // Here c=0. If we're at the last comparator, this doc is not
          // competitive, since docs are visited in doc Id order, which means
          // this doc cannot compete with any other document in the queue.
          return;
        }
      }
    }

    filler.fillValue(doc);
    SearchGroup group = groupMap.get(mval);
    if (group == null) {
      int num = groupMap.size();
      if (groupMap.size() < nGroups) {
        SearchGroup sg = new SearchGroup();
        sg.groupValue = mval.duplicate();
        sg.comparatorSlot = num++;
        sg.matches = 1;
        sg.topDoc = docBase + doc;
        // sg.topDocScore = scorer.score();
        for (FieldComparator fc : comparators)
          fc.copy(sg.comparatorSlot, doc);
        groupMap.put(sg.groupValue, sg);
        if (groupMap.size() == nGroups) {
          buildSet();
        }
        return;
      }

      // we already tested that the document is competitive, so replace
      // the smallest group with this new group.

      // remove current smallest group
      SearchGroup smallest = orderedGroups.pollLast();
      groupMap.remove(smallest.groupValue);

      // reuse the removed SearchGroup
      smallest.groupValue.copy(mval);
      smallest.matches = 1;
      smallest.topDoc = docBase + doc;
      // smallest.topDocScore = scorer.score();
      for (FieldComparator fc : comparators)
        fc.copy(smallest.comparatorSlot, doc);

      groupMap.put(smallest.groupValue, smallest);
      orderedGroups.add(smallest);

      for (FieldComparator fc : comparators)
        fc.setBottom(orderedGroups.last().comparatorSlot);

      return;
    }

    //
    // update existing group
    //

    group.matches++; // TODO: these aren't valid if the group is every discarded then re-added.  keep track if there have been discards?

    for (int i = 0;; i++) {
      FieldComparator fc = comparators[i];
      fc.copy(spareSlot, doc);

      final int c = reversed[i] * fc.compare(group.comparatorSlot, spareSlot);
      if (c < 0) {
        // Definitely not competitive.
        return;
      } else if (c > 0) {
        // Definitely competitive.
        // Set remaining comparators
        for (int j=i+1; j<comparators.length; j++)
          comparators[j].copy(spareSlot, doc);
        break;
      } else if (i == comparators.length - 1) {
        // Here c=0. If we're at the last comparator, this doc is not
        // competitive, since docs are visited in doc Id order, which means
        // this doc cannot compete with any other document in the queue.
        return;
      }
    }

    // remove before updating the group since lookup is done via comparators
    // TODO: optimize this
    if (orderedGroups != null)
      orderedGroups.remove(group);

    group.topDoc = docBase + doc;
    // group.topDocScore = scorer.score();
    int tmp = spareSlot; spareSlot = group.comparatorSlot; group.comparatorSlot=tmp;  // swap slots

    // re-add the changed group
    if (orderedGroups != null)
      orderedGroups.add(group);
  }

  void buildSet() {
    Comparator<SearchGroup> comparator = new Comparator<SearchGroup>() {
      public int compare(SearchGroup o1, SearchGroup o2) {
        for (int i = 0;; i++) {
          FieldComparator fc = comparators[i];
          int c = reversed[i] * fc.compare(o1.comparatorSlot, o2.comparatorSlot);
          if (c != 0) {
            return c;
          } else if (i == comparators.length - 1) {
            return o1.topDoc - o2.topDoc;
          }
        }
      }
    };

    orderedGroups = new TreeSet<SearchGroup>(comparator);
    orderedGroups.addAll(groupMap.values());
    if (orderedGroups.size() == 0) return;
    for (FieldComparator fc : comparators)
      fc.setBottom(orderedGroups.last().comparatorSlot);
  }

  @Override
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    this.docBase = docBase;
    docValues = vs.getValues(context, reader);
    filler = docValues.getValueFiller();
    mval = filler.getValue();
    for (int i=0; i<comparators.length; i++)
      comparators[i] = comparators[i].setNextReader(reader, docBase);
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  @Override
  public int getMatches() {
    return matches;
  }
}


/**
 * This class allows a different sort within a group than what is used between groups.
 * Sorting between groups is done by the sort value of the first (highest ranking)
 * document in that group.
 */
class TopGroupSortCollector extends TopGroupCollector {

  IndexReader reader;
  Sort groupSort;

  public TopGroupSortCollector(ValueSource groupByVS, Map vsContext, Sort sort, Sort groupSort, int nGroups) throws IOException {
    super(groupByVS, vsContext, sort, nGroups);
    this.groupSort = groupSort;
  }

  void constructComparators(FieldComparator[] comparators, int[] reversed, SortField[] sortFields, int size) throws IOException {
    for (int i = 0; i < sortFields.length; i++) {
      SortField sortField = sortFields[i];
      reversed[i] = sortField.getReverse() ? -1 : 1;
      comparators[i] = sortField.getComparator(size, i);
      if (scorer != null) comparators[i].setScorer(scorer);
      if (reader != null) comparators[i] = comparators[i].setNextReader(reader, docBase);
    }
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    super.setScorer(scorer);
    for (SearchGroup searchGroup : groupMap.values()) {
      for (FieldComparator fc : searchGroup.sortGroupComparators) {
        fc.setScorer(scorer);
      }
    }
  }

  @Override
  public void collect(int doc) throws IOException {
    matches++;
    filler.fillValue(doc);
    SearchGroup group = groupMap.get(mval);
    if (group == null) {
      int num = groupMap.size();
      if (groupMap.size() < nGroups) {
        SearchGroup sg = new SearchGroup();
        SortField[] sortGroupFields = groupSort.getSort();
        sg.sortGroupComparators = new FieldComparator[sortGroupFields.length];
        sg.sortGroupReversed = new int[sortGroupFields.length];
        constructComparators(sg.sortGroupComparators, sg.sortGroupReversed, sortGroupFields, 1);

        sg.groupValue = mval.duplicate();
        sg.comparatorSlot = num++;
        sg.matches = 1;
        sg.topDoc = docBase + doc;
        // sg.topDocScore = scorer.score();
        for (FieldComparator fc : comparators)
          fc.copy(sg.comparatorSlot, doc);
        for (FieldComparator fc : sg.sortGroupComparators) {
          fc.copy(0, doc);
          fc.setBottom(0);
        }
        groupMap.put(sg.groupValue, sg);
        return;
      }

      if (orderedGroups == null) {
        buildSet();
      }

      SearchGroup leastSignificantGroup = orderedGroups.last();
      for (int i = 0;; i++) {
        final int c = leastSignificantGroup.sortGroupReversed[i] * leastSignificantGroup.sortGroupComparators[i].compareBottom(doc);
        if (c < 0) {
          // Definitely not competitive.
          return;
        } else if (c > 0) {
          // Definitely competitive.
          break;
        } else if (i == leastSignificantGroup.sortGroupComparators.length - 1) {
          // Here c=0. If we're at the last comparator, this doc is not
          // competitive, since docs are visited in doc Id order, which means
          // this doc cannot compete with any other document in the queue.
          return;
        }
      }

      // remove current smallest group
      SearchGroup smallest = orderedGroups.pollLast();
      groupMap.remove(smallest.groupValue);

      // reuse the removed SearchGroup
      smallest.groupValue.copy(mval);
      smallest.matches = 1;
      smallest.topDoc = docBase + doc;
      // smallest.topDocScore = scorer.score();
      for (FieldComparator fc : comparators)
        fc.copy(smallest.comparatorSlot, doc);
      for (FieldComparator fc : smallest.sortGroupComparators) {
        fc.copy(0, doc);
        fc.setBottom(0);
      }

      groupMap.put(smallest.groupValue, smallest);
      orderedGroups.add(smallest);

      for (FieldComparator fc : comparators)
        fc.setBottom(orderedGroups.last().comparatorSlot);
      for (FieldComparator fc : smallest.sortGroupComparators)
        fc.setBottom(0);

      return;
    }

    //
    // update existing group
    //

    group.matches++; // TODO: these aren't valid if the group is every discarded then re-added.  keep track if there have been discards?

    for (int i = 0;; i++) {
      FieldComparator fc = group.sortGroupComparators[i];

      final int c = group.sortGroupReversed[i] * fc.compareBottom(doc);
      if (c < 0) {
        // Definitely not competitive.
        return;
      } else if (c > 0) {
        // Definitely competitive.
        // Set remaining comparators
        for (int j = 0; j < group.sortGroupComparators.length; j++) {
          group.sortGroupComparators[j].copy(0, doc);
          group.sortGroupComparators[j].setBottom(0);
        }
        for (FieldComparator comparator : comparators) comparator.copy(spareSlot, doc);
        break;
      } else if (i == group.sortGroupComparators.length - 1) {
        // Here c=0. If we're at the last comparator, this doc is not
        // competitive, since docs are visited in doc Id order, which means
        // this doc cannot compete with any other document in the queue.
        return;
      }
    }

    // remove before updating the group since lookup is done via comparators
    // TODO: optimize this
    if (orderedGroups != null)
      orderedGroups.remove(group);

    group.topDoc = docBase + doc;
    // group.topDocScore = scorer.score();
    int tmp = spareSlot; spareSlot = group.comparatorSlot; group.comparatorSlot=tmp;  // swap slots

    // re-add the changed group
    if (orderedGroups != null)
      orderedGroups.add(group);
  }

  @Override
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    super.setNextReader(reader, docBase);
    this.reader = reader;
    for (SearchGroup searchGroup : groupMap.values()) {
      for (int i=0; i<searchGroup.sortGroupComparators.length; i++)
        searchGroup.sortGroupComparators[i] = searchGroup.sortGroupComparators[i].setNextReader(reader, docBase);
    }
  }

}


class Phase2GroupCollector extends Collector {
  final HashMap<MutableValue, SearchGroupDocs> groupMap;
  final ValueSource vs;
  final Map context;

  DocValues docValues;
  DocValues.ValueFiller filler;
  MutableValue mval;
  Scorer scorer;
  int docBase;

  // TODO: may want to decouple from the phase1 collector
  public Phase2GroupCollector(TopGroupCollector topGroups, ValueSource groupByVS, Map vsContext, Sort sort, int docsPerGroup, boolean getScores, int offset) throws IOException {
    boolean getSortFields = false;

    if (topGroups.orderedGroups == null)
      topGroups.buildSet();

    groupMap = new HashMap<MutableValue, SearchGroupDocs>(topGroups.groupMap.size());
    for (SearchGroup group : topGroups.orderedGroups) {
      if (offset > 0) {
        offset--;
        continue;
      }
      SearchGroupDocs groupDocs = new SearchGroupDocs();
      groupDocs.groupValue = group.groupValue;
      if (sort==null)
        groupDocs.collector = TopScoreDocCollector.create(docsPerGroup, true);        
      else
        groupDocs.collector = TopFieldCollector.create(sort, docsPerGroup, getSortFields, getScores, getScores, true);
      groupMap.put(groupDocs.groupValue, groupDocs);
    }

    this.vs = groupByVS;
    this.context = vsContext;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
    for (SearchGroupDocs group : groupMap.values())
      group.collector.setScorer(scorer);
  }

  @Override
  public void collect(int doc) throws IOException {
    filler.fillValue(doc);
    SearchGroupDocs group = groupMap.get(mval);
    if (group == null) return;
    group.collector.collect(doc);
  }

  @Override
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    this.docBase = docBase;
    docValues = vs.getValues(context, reader);
    filler = docValues.getValueFiller();
    mval = filler.getValue();
    for (SearchGroupDocs group : groupMap.values())
      group.collector.setNextReader(reader, docBase);
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }
}

// TODO: merge with SearchGroup or not?
// ad: don't need to build a new hashmap
// disad: blows up the size of SearchGroup if we need many of them, and couples implementations
class SearchGroupDocs {
  public MutableValue groupValue;
  TopDocsCollector collector;
}

