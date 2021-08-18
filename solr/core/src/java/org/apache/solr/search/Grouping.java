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
package org.apache.solr.search;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.CachingCollector;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.AllGroupHeadsCollector;
import org.apache.lucene.search.grouping.AllGroupsCollector;
import org.apache.lucene.search.grouping.FirstPassGroupingCollector;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TermGroupSelector;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.TopGroupsCollector;
import org.apache.lucene.search.grouping.ValueSourceGroupSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrFieldSource;
import org.apache.solr.search.grouping.collector.FilterCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic Solr Grouping infrastructure.
 * Warning NOT thread safe!
 *
 * @lucene.experimental
 */
public class Grouping {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrIndexSearcher searcher;
  private final QueryResult qr;
  private final QueryCommand cmd;
  @SuppressWarnings({"rawtypes"})
  private final List<Command> commands = new ArrayList<>();
  private final boolean main;
  private final boolean cacheSecondPassSearch;
  private final int maxDocsPercentageToCache;

  private Sort groupSort;
  private Sort withinGroupSort;
  private int limitDefault;
  private int docsPerGroupDefault;
  private int groupOffsetDefault;
  private Format defaultFormat;
  private TotalCount defaultTotalCount;

  private int maxDoc;
  private boolean needScores;
  private boolean getDocSet;
  private boolean getGroupedDocSet;
  private boolean getDocList; // doclist needed for debugging or highlighting
  private Query query;
  private DocSet filter;
  private Filter luceneFilter;
  private NamedList<Object> grouped = new SimpleOrderedMap<>();
  private Set<Integer> idSet = new LinkedHashSet<>();  // used for tracking unique docs when we need a doclist
  private int maxMatches;  // max number of matches from any grouping command
  private float maxScore = Float.NaN;  // max score seen in any doclist
  private boolean signalCacheWarning = false;
  private TimeLimitingCollector timeLimitingCollector;


  public DocList mainResult;  // output if one of the grouping commands should be used as the main result.

  /**
   * @param cacheSecondPassSearch    Whether to cache the documents and scores from the first pass search for the second
   *                                 pass search.
   * @param maxDocsPercentageToCache The maximum number of documents in a percentage relative from maxdoc
   *                                 that is allowed in the cache. When this threshold is met,
   *                                 the cache is not used in the second pass search.
   */
  public Grouping(SolrIndexSearcher searcher,
                  QueryResult qr,
                  QueryCommand cmd,
                  boolean cacheSecondPassSearch,
                  int maxDocsPercentageToCache,
                  boolean main) {
    this.searcher = searcher;
    this.qr = qr;
    this.cmd = cmd;
    this.cacheSecondPassSearch = cacheSecondPassSearch;
    this.maxDocsPercentageToCache = maxDocsPercentageToCache;
    this.main = main;
  }

  public void add(@SuppressWarnings({"rawtypes"})Grouping.Command groupingCommand) {
    commands.add(groupingCommand);
  }

  /**
   * Adds a field command based on the specified field.
   * If the field is not compatible with {@link CommandField} it invokes the
   * {@link #addFunctionCommand(String, org.apache.solr.request.SolrQueryRequest)} method.
   *
   * @param field The fieldname to group by.
   */
  public void addFieldCommand(String field, SolrQueryRequest request) throws SyntaxError {
    SchemaField schemaField = searcher.getSchema().getField(field); // Throws an exception when field doesn't exist. Bad request.
    FieldType fieldType = schemaField.getType();
    ValueSource valueSource = fieldType.getValueSource(schemaField, null);
    if (!(valueSource instanceof StrFieldSource)) {
      addFunctionCommand(field, request);
      return;
    }

    Grouping.CommandField gc = new CommandField();
    gc.withinGroupSort = withinGroupSort;
    gc.groupBy = field;
    gc.key = field;
    gc.numGroups = limitDefault;
    gc.docsPerGroup = docsPerGroupDefault;
    gc.groupOffset = groupOffsetDefault;
    gc.offset = cmd.getOffset();
    gc.groupSort = groupSort;
    gc.format = defaultFormat;
    gc.totalCount = defaultTotalCount;

    if (main) {
      gc.main = true;
      gc.format = Grouping.Format.simple;
    }

    if (gc.format == Grouping.Format.simple) {
      gc.groupOffset = 0;  // doesn't make sense
    }
    commands.add(gc);
  }

  public void addFunctionCommand(String groupByStr, SolrQueryRequest request) throws SyntaxError {
    QParser parser = QParser.getParser(groupByStr, FunctionQParserPlugin.NAME, request);
    Query q = parser.getQuery();
    @SuppressWarnings({"rawtypes"})
    final Grouping.Command gc;
    if (q instanceof FunctionQuery) {
      ValueSource valueSource = ((FunctionQuery) q).getValueSource();
      if (valueSource instanceof StrFieldSource) {
        String field = ((StrFieldSource) valueSource).getField();
        CommandField commandField = new CommandField();
        commandField.groupBy = field;
        gc = commandField;
      } else {
        CommandFunc commandFunc = new CommandFunc();
        commandFunc.groupBy = valueSource;
        gc = commandFunc;
      }
    } else {
      CommandFunc commandFunc = new CommandFunc();
      commandFunc.groupBy = new QueryValueSource(q, 0.0f);
      gc = commandFunc;
    }
    gc.withinGroupSort = withinGroupSort;
    gc.key = groupByStr;
    gc.numGroups = limitDefault;
    gc.docsPerGroup = docsPerGroupDefault;
    gc.groupOffset = groupOffsetDefault;
    gc.offset = cmd.getOffset();
    gc.groupSort = groupSort;
    gc.format = defaultFormat;
    gc.totalCount = defaultTotalCount;

    if (main) {
      gc.main = true;
      gc.format = Grouping.Format.simple;
    }

    if (gc.format == Grouping.Format.simple) {
      gc.groupOffset = 0;  // doesn't make sense
    }

    commands.add(gc);
  }

  public void addQueryCommand(String groupByStr, SolrQueryRequest request) throws SyntaxError {
    QParser parser = QParser.getParser(groupByStr, request);
    Query gq = parser.getQuery();

    if (gq == null) {
      // normalize a null query to a query that matches nothing
      gq = new MatchNoDocsQuery();
    }
    Grouping.CommandQuery gc = new CommandQuery();
    gc.query = gq;
    gc.withinGroupSort = withinGroupSort;
    gc.key = groupByStr;
    gc.numGroups = limitDefault;
    gc.docsPerGroup = docsPerGroupDefault;
    gc.groupOffset = groupOffsetDefault;

    // these two params will only be used if this is for the main result set
    gc.offset = cmd.getOffset();
    gc.numGroups = limitDefault;
    gc.format = defaultFormat;

    if (main) {
      gc.main = true;
      gc.format = Grouping.Format.simple;
    }
    if (gc.format == Grouping.Format.simple) {
      gc.docsPerGroup = gc.numGroups;  // doesn't make sense to limit to one
      gc.groupOffset = gc.offset;
    }
    commands.add(gc);
  }

  public Grouping setGroupSort(Sort groupSort) {
    this.groupSort = groupSort;
    return this;
  }

  public Grouping setWithinGroupSort(Sort withinGroupSort) {
    this.withinGroupSort = withinGroupSort;
    return this;
  }

  public Grouping setLimitDefault(int limitDefault) {
    this.limitDefault = limitDefault;
    return this;
  }

  public Grouping setDocsPerGroupDefault(int docsPerGroupDefault) {
    this.docsPerGroupDefault = docsPerGroupDefault;
    return this;
  }

  public Grouping setGroupOffsetDefault(int groupOffsetDefault) {
    this.groupOffsetDefault = groupOffsetDefault;
    return this;
  }

  public Grouping setDefaultFormat(Format defaultFormat) {
    this.defaultFormat = defaultFormat;
    return this;
  }

  public Grouping setDefaultTotalCount(TotalCount defaultTotalCount) {
    this.defaultTotalCount = defaultTotalCount;
    return this;
  }

  public Grouping setGetGroupedDocSet(boolean getGroupedDocSet) {
    this.getGroupedDocSet = getGroupedDocSet;
    return this;
  }

  @SuppressWarnings({"rawtypes"})
  public List<Command> getCommands() {
    return commands;
  }

  public void execute() throws IOException {
    if (commands.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Specify at least one field, function or query to group by.");
    }

    DocListAndSet out = new DocListAndSet();
    qr.setDocListAndSet(out);

    SolrIndexSearcher.ProcessedFilter pf = searcher.getProcessedFilter(cmd.getFilter(), cmd.getFilterList());
    final Filter luceneFilter = pf.filter;
    maxDoc = searcher.maxDoc();

    needScores = (cmd.getFlags() & SolrIndexSearcher.GET_SCORES) != 0;
    boolean cacheScores = false;
    // NOTE: Change this when withinGroupSort can be specified per group
    if (!needScores && !commands.isEmpty()) {
      Sort withinGroupSort = commands.get(0).withinGroupSort;
      cacheScores = withinGroupSort == null || withinGroupSort.needsScores();
    } else if (needScores) {
      cacheScores = needScores;
    }
    getDocSet = (cmd.getFlags() & SolrIndexSearcher.GET_DOCSET) != 0;
    getDocList = (cmd.getFlags() & SolrIndexSearcher.GET_DOCLIST) != 0;
    query = QueryUtils.makeQueryable(cmd.getQuery());

    for (@SuppressWarnings({"rawtypes"})Command cmd : commands) {
      cmd.prepare();
    }

    AllGroupHeadsCollector<?> allGroupHeadsCollector = null;
    List<Collector> collectors = new ArrayList<>(commands.size());
    for (@SuppressWarnings({"rawtypes"})Command cmd : commands) {
      Collector collector = cmd.createFirstPassCollector();
      if (collector != null) {
        collectors.add(collector);
      }
      if (getGroupedDocSet && allGroupHeadsCollector == null) {
        collectors.add(allGroupHeadsCollector = cmd.createAllGroupCollector());
      }
    }

    DocSetCollector setCollector = null;
    if (getDocSet && allGroupHeadsCollector == null) {
      setCollector = new DocSetCollector(maxDoc);
      collectors.add(setCollector);
    }
    Collector allCollectors = MultiCollector.wrap(collectors);

    CachingCollector cachedCollector = null;
    if (cacheSecondPassSearch && allCollectors != null) {
      int maxDocsToCache = (int) Math.round(maxDoc * (maxDocsPercentageToCache / 100.0d));
      // Only makes sense to cache if we cache more than zero.
      // Maybe we should have a minimum and a maximum, that defines the window we would like caching for.
      if (maxDocsToCache > 0) {
        allCollectors = cachedCollector = CachingCollector.create(allCollectors, cacheScores, maxDocsToCache);
      }
    }

    if (pf.postFilter != null) {
      pf.postFilter.setLastDelegate(allCollectors);
      allCollectors = pf.postFilter;
    }

    if (allCollectors != null) {
      searchWithTimeLimiter(luceneFilter, allCollectors);

      if(allCollectors instanceof DelegatingCollector) {
        ((DelegatingCollector) allCollectors).finish();
      }
    }

    if (getGroupedDocSet && allGroupHeadsCollector != null) {
      qr.setDocSet(new BitDocSet(allGroupHeadsCollector.retrieveGroupHeads(maxDoc)));
    } else if (getDocSet) {
      qr.setDocSet(setCollector.getDocSet());
    }

    collectors.clear();
    for (@SuppressWarnings({"rawtypes"})Command cmd : commands) {
      Collector collector = cmd.createSecondPassCollector();
      if (collector != null)
        collectors.add(collector);
    }

    if (!collectors.isEmpty()) {
      Collector secondPhaseCollectors = MultiCollector.wrap(collectors.toArray(new Collector[collectors.size()]));
      if (collectors.size() > 0) {
        if (cachedCollector != null) {
          if (cachedCollector.isCached()) {
            cachedCollector.replay(secondPhaseCollectors);
          } else {
            signalCacheWarning = true;
            log.warn(String.format(Locale.ROOT, "The grouping cache is active, but not used because it exceeded the max cache limit of %d percent", maxDocsPercentageToCache));
            log.warn("Please increase cache size or disable group caching.");
            searchWithTimeLimiter(luceneFilter, secondPhaseCollectors);
          }
        } else {
          if (pf.postFilter != null) {
            pf.postFilter.setLastDelegate(secondPhaseCollectors);
            secondPhaseCollectors = pf.postFilter;
          }
          searchWithTimeLimiter(luceneFilter, secondPhaseCollectors);
        }
        if (secondPhaseCollectors instanceof DelegatingCollector) {
          ((DelegatingCollector) secondPhaseCollectors).finish();
        }
      }
    }

    for (@SuppressWarnings({"rawtypes"})Command cmd : commands) {
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
      qr.setDocList(new DocSlice(0, sz, ids, null, maxMatches, maxScore, TotalHits.Relation.EQUAL_TO));
    }
  }

  /**
   * Invokes search with the specified filter and collector.  
   * If a time limit has been specified, wrap the collector in a TimeLimitingCollector
   */
  private void searchWithTimeLimiter(final Filter luceneFilter, Collector collector) throws IOException {
    if (cmd.getTimeAllowed() > 0) {
      if (timeLimitingCollector == null) {
        timeLimitingCollector = new TimeLimitingCollector(collector, TimeLimitingCollector.getGlobalCounter(), cmd.getTimeAllowed());
      } else {
        /*
         * This is so the same timer can be used for grouping's multiple phases.   
         * We don't want to create a new TimeLimitingCollector for each phase because that would 
         * reset the timer for each phase.  If time runs out during the first phase, the 
         * second phase should timeout quickly.
         */
        timeLimitingCollector.setCollector(collector);
      }
      collector = timeLimitingCollector;
    }
    try {
      searcher.search(QueryUtils.combineQueryAndFilter(query, luceneFilter), collector);
    } catch (TimeLimitingCollector.TimeExceededException | ExitableDirectoryReader.ExitingReaderException x) {
      log.warn("Query: {}; ", query, x);
      qr.setPartialResults(true);
    }
  }

  /**
   * Returns offset + len if len equals zero or higher. Otherwise returns max.
   *
   * @param offset The offset
   * @param len    The number of documents to return
   * @param max    The number of document to return if len &lt; 0 or if offset + len &gt; 0
   * @return offset + len if len equals zero or higher. Otherwise returns max
   */
  public static int getMax(int offset, int len, int max) {
    int v = len < 0 ? max : offset + len;
    if (v < 0 || v > max) v = max;
    return v;
  }

  /**
   * Returns whether a cache warning should be send to the client.
   * The value <code>true</code> is returned when the cache is emptied because the caching limits where met, otherwise
   * <code>false</code> is returned.
   *
   * @return whether a cache warning should be send to the client
   */
  public boolean isSignalCacheWarning() {
    return signalCacheWarning;
  }

  //======================================   Inner classes =============================================================

  public static enum Format {

    /**
     * Grouped result. Each group has its own result set.
     */
    grouped,

    /**
     * Flat result. All documents of all groups are put in one list.
     */
    simple
  }

  public static enum TotalCount {
    /**
     * Computations should be based on groups.
     */
    grouped,

    /**
     * Computations should be based on plain documents, so not taking grouping into account.
     */
    ungrouped
  }

  /**
   * General group command. A group command is responsible for creating the first and second pass collectors.
   * A group command is also responsible for creating the response structure.
   * <p>
   * Note: Maybe the creating the response structure should be done in something like a ReponseBuilder???
   * Warning NOT thread save!
   */
  public abstract class Command<T> {

    public String key;       // the name to use for this group in the response
    public Sort withinGroupSort;   // the sort of the documents *within* a single group.
    public Sort groupSort;        // the sort between groups
    public int docsPerGroup; // how many docs in each group - from "group.limit" param, default=1
    public int groupOffset;  // the offset within each group (for paging within each group)
    public int numGroups;    // how many groups - defaults to the "rows" parameter
    int actualGroupsToFind;  // How many groups should actually be found. Based on groupOffset and numGroups.
    public int offset;       // offset into the list of groups
    public Format format;
    public boolean main;     // use as the main result in simple format (grouped.main=true param)
    public TotalCount totalCount = TotalCount.ungrouped;

    TopGroups<T> result;


    /**
     * Prepare this <code>Command</code> for execution.
     *
     * @throws IOException If I/O related errors occur
     */
    protected abstract void prepare() throws IOException;

    /**
     * Returns one or more {@link Collector} instances that are needed to perform the first pass search.
     * If multiple Collectors are returned then these wrapped in a {@link org.apache.lucene.search.MultiCollector}.
     *
     * @return one or more {@link Collector} instances that are need to perform the first pass search
     * @throws IOException If I/O related errors occur
     */
    protected abstract Collector createFirstPassCollector() throws IOException;

    /**
     * Returns zero or more {@link Collector} instances that are needed to perform the second pass search.
     * In the case when no {@link Collector} instances are created <code>null</code> is returned.
     * If multiple Collectors are returned then these wrapped in a {@link org.apache.lucene.search.MultiCollector}.
     *
     * @return zero or more {@link Collector} instances that are needed to perform the second pass search
     * @throws IOException If I/O related errors occur
     */
    protected Collector createSecondPassCollector() throws IOException {
      return null;
    }

    /**
     * Returns a collector that is able to return the most relevant document of all groups.
     * Returns <code>null</code> if the command doesn't support this type of collector.
     *
     * @return a collector that is able to return the most relevant document of all groups.
     * @throws IOException If I/O related errors occur
     */
    public AllGroupHeadsCollector<?> createAllGroupCollector() throws IOException {
      return null;
    }

    /**
     * Performs any necessary post actions to prepare the response.
     *
     * @throws IOException If I/O related errors occur
     */
    protected abstract void finish() throws IOException;

    /**
     * Returns the number of matches for this <code>Command</code>.
     *
     * @return the number of matches for this <code>Command</code>
     */
    public abstract int getMatches();

    /**
     * Returns the number of groups found for this <code>Command</code>.
     * If the command doesn't support counting the groups <code>null</code> is returned.
     *
     * @return the number of groups found for this <code>Command</code>
     */
    protected Integer getNumberOfGroups() {
      return null;
    }

    protected void populateScoresIfNecessary() throws IOException {
      if (needScores) {
        for (GroupDocs<?> groups : result.groups) {
          TopFieldCollector.populateScores(groups.scoreDocs, searcher, query);
        }
      }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected NamedList commonResponse() {
      NamedList groupResult = new SimpleOrderedMap();
      grouped.add(key, groupResult);  // grouped={ key={

      int matches = getMatches();
      groupResult.add("matches", matches);
      if (totalCount == TotalCount.grouped) {
        Integer totalNrOfGroups = getNumberOfGroups();
        groupResult.add("ngroups", totalNrOfGroups == null ? Integer.valueOf(0) : totalNrOfGroups);
      }
      maxMatches = Math.max(maxMatches, matches);
      return groupResult;
    }

    protected DocList getDocList(@SuppressWarnings({"rawtypes"})GroupDocs groups) {
      assert groups.totalHits.relation == TotalHits.Relation.EQUAL_TO;
      int max = Math.toIntExact(groups.totalHits.value);
      int off = groupOffset;
      int len = docsPerGroup;
      if (format == Format.simple) {
        off = offset;
        len = numGroups;
      }
      int docsToCollect = getMax(off, len, max);

      // TODO: implement a DocList impl that doesn't need to start at offset=0
      int docsCollected = Math.min(docsToCollect, groups.scoreDocs.length);

      int ids[] = new int[docsCollected];
      float[] scores = needScores ? new float[docsCollected] : null;
      for (int i = 0; i < ids.length; i++) {
        ids[i] = groups.scoreDocs[i].doc;
        if (scores != null)
          scores[i] = groups.scoreDocs[i].score;
      }

      float score = groups.maxScore;
      maxScore = maxAvoidNaN(score, maxScore);
      DocSlice docs = new DocSlice(off, Math.max(0, ids.length - off), ids, scores, groups.totalHits.value, score, TotalHits.Relation.EQUAL_TO);

      if (getDocList) {
        DocIterator iter = docs.iterator();
        while (iter.hasNext())
          idSet.add(iter.nextDoc());
      }
      return docs;
    }

    @SuppressWarnings({"unchecked"})
    protected void addDocList(@SuppressWarnings({"rawtypes"})NamedList rsp
            , @SuppressWarnings({"rawtypes"})GroupDocs groups) {
      rsp.add("doclist", getDocList(groups));
    }

    // Flatten the groups and get up offset + rows documents
    protected DocList createSimpleResponse() {
      @SuppressWarnings({"rawtypes"})
      GroupDocs[] groups = result != null ? result.groups : new GroupDocs[0];

      List<Integer> ids = new ArrayList<>();
      List<Float> scores = new ArrayList<>();
      int docsToGather = getMax(offset, numGroups, maxDoc);
      int docsGathered = 0;
      float maxScore = Float.NaN;

      outer:
      for (@SuppressWarnings({"rawtypes"})GroupDocs group : groups) {
        maxScore = maxAvoidNaN(maxScore, group.maxScore);

        for (ScoreDoc scoreDoc : group.scoreDocs) {
          if (docsGathered >= docsToGather) {
            break outer;
          }

          ids.add(scoreDoc.doc);
          scores.add(scoreDoc.score);
          docsGathered++;
        }
      }

      int len = docsGathered > offset ? docsGathered - offset : 0;
      int[] docs = ArrayUtils.toPrimitive(ids.toArray(new Integer[ids.size()]));
      float[] docScores = ArrayUtils.toPrimitive(scores.toArray(new Float[scores.size()]));
      DocSlice docSlice = new DocSlice(offset, len, docs, docScores, getMatches(), maxScore, TotalHits.Relation.EQUAL_TO);

      if (getDocList) {
        for (int i = offset; i < docs.length; i++) {
          idSet.add(docs[i]);
        }
      }

      return docSlice;
    }

  }

  /** Differs from {@link Math#max(float, float)} in that if only one side is NaN, we return the other. */
  private float maxAvoidNaN(float valA, float valB) {
    if (Float.isNaN(valA) || valB > valA) {
      return valB;
    } else {
      return valA;
    }
  }

  /**
   * A group command for grouping on a field.
   */
  public class CommandField extends Command<BytesRef> {

    public String groupBy;
    FirstPassGroupingCollector<BytesRef> firstPass;
    TopGroupsCollector<BytesRef> secondPass;

    AllGroupsCollector<BytesRef> allGroupsCollector;

    // If offset falls outside the number of documents a group can provide use this collector instead of secondPass
    TotalHitCountCollector fallBackCollector;
    Collection<SearchGroup<BytesRef>> topGroups;

    @Override
    protected void prepare() throws IOException {
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    @Override
    protected Collector createFirstPassCollector() throws IOException {
      // Ok we don't want groups, but do want a total count
      if (actualGroupsToFind <= 0) {
        fallBackCollector = new TotalHitCountCollector();
        return fallBackCollector;
      }

      groupSort = groupSort == null ? Sort.RELEVANCE : groupSort;
      firstPass = new FirstPassGroupingCollector<>(new TermGroupSelector(groupBy), groupSort, actualGroupsToFind);
      return firstPass;
    }

    @Override
    protected Collector createSecondPassCollector() throws IOException {
      if (actualGroupsToFind <= 0) {
        allGroupsCollector = new AllGroupsCollector<>(new TermGroupSelector(groupBy));
        return totalCount == TotalCount.grouped ? allGroupsCollector : null;
      }

      topGroups = format == Format.grouped ? firstPass.getTopGroups(offset) : firstPass.getTopGroups(0);
      if (topGroups == null) {
        if (totalCount == TotalCount.grouped) {
          allGroupsCollector = new AllGroupsCollector<>(new TermGroupSelector(groupBy));
          fallBackCollector = new TotalHitCountCollector();
          return MultiCollector.wrap(allGroupsCollector, fallBackCollector);
        } else {
          fallBackCollector = new TotalHitCountCollector();
          return fallBackCollector;
        }
      }

      int groupedDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupedDocsToCollect = Math.max(groupedDocsToCollect, 1);
      Sort withinGroupSort = this.withinGroupSort != null ? this.withinGroupSort : Sort.RELEVANCE;
      secondPass = new TopGroupsCollector<>(new TermGroupSelector(groupBy),
          topGroups, groupSort, withinGroupSort, groupedDocsToCollect, needScores
      );

      if (totalCount == TotalCount.grouped) {
        allGroupsCollector = new AllGroupsCollector<>(new TermGroupSelector(groupBy));
        return MultiCollector.wrap(secondPass, allGroupsCollector);
      } else {
        return secondPass;
      }
    }

    @Override
    public AllGroupHeadsCollector<?> createAllGroupCollector() throws IOException {
      Sort sortWithinGroup = withinGroupSort != null ? withinGroupSort : Sort.RELEVANCE;
      return AllGroupHeadsCollector.newCollector(new TermGroupSelector(groupBy), sortWithinGroup);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    protected void finish() throws IOException {
      if (secondPass != null) {
        result = secondPass.getTopGroups(0);
        populateScoresIfNecessary();
      }
      if (main) {
        mainResult = createSimpleResponse();
        return;
      }

      @SuppressWarnings({"rawtypes"})
      NamedList groupResult = commonResponse();

      if (format == Format.simple) {
        groupResult.add("doclist", createSimpleResponse());
        return;
      }

      @SuppressWarnings({"rawtypes"})
      List groupList = new ArrayList();
      groupResult.add("groups", groupList);        // grouped={ key={ groups=[

      if (result == null) {
        return;
      }

      // handle case of rows=0
      if (numGroups == 0) return;

      for (GroupDocs<BytesRef> group : result.groups) {
        @SuppressWarnings({"rawtypes"})
        NamedList nl = new SimpleOrderedMap();
        groupList.add(nl);                         // grouped={ key={ groups=[ {


        // To keep the response format compatable with trunk.
        // In trunk MutableValue can convert an indexed value to its native type. E.g. string to int
        // The only option I currently see is the use the FieldType for this
        if (group.groupValue != null) {
          SchemaField schemaField = searcher.getSchema().getField(groupBy);
          FieldType fieldType = schemaField.getType();
          // use createFields so that fields having doc values are also supported
          // TODO: currently, this path is called only for string field, so
          // should we just use fieldType.toObject(schemaField, group.groupValue) here?
          List<IndexableField> fields = schemaField.createFields(group.groupValue.utf8ToString());
          if (CollectionUtils.isNotEmpty(fields)) {
            nl.add("groupValue", fieldType.toObject(fields.get(0)));
          } else {
            throw new SolrException(ErrorCode.INVALID_STATE,
                "Couldn't create schema field for grouping, group value: " + group.groupValue.utf8ToString()
                + ", field: " + schemaField);
          }
        } else {
          nl.add("groupValue", null);
        }

        addDocList(nl, group);
      }
    }

    @Override
    public int getMatches() {
      if (result == null && fallBackCollector == null) {
        return 0;
      }

      return result != null ? result.totalHitCount : fallBackCollector.getTotalHits();
    }

    @Override
    protected Integer getNumberOfGroups() {
      return allGroupsCollector == null ? null : allGroupsCollector.getGroupCount();
    }
  }

  /**
   * A group command for grouping on a query.
   */
  //NOTE: doesn't need to be generic. Maybe Command interface --> First / Second pass abstract impl.
  @SuppressWarnings({"rawtypes"})
  public class CommandQuery extends Command {

    public Query query;
    TopDocsCollector topCollector;
    MaxScoreCollector maxScoreCollector;
    FilterCollector collector;

    @Override
    protected void prepare() throws IOException {
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    @Override
    protected Collector createFirstPassCollector() throws IOException {
      DocSet groupFilt = searcher.getDocSet(query);
      int groupDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      Collector subCollector;
      if (withinGroupSort == null || withinGroupSort.equals(Sort.RELEVANCE)) {
        subCollector = topCollector = TopScoreDocCollector.create(groupDocsToCollect, Integer.MAX_VALUE);
      } else {
        topCollector = TopFieldCollector.create(searcher.weightSort(withinGroupSort), groupDocsToCollect, Integer.MAX_VALUE);
        if (needScores) {
          maxScoreCollector = new MaxScoreCollector();
          subCollector = MultiCollector.wrap(topCollector, maxScoreCollector);
        } else {
          subCollector = topCollector;
        }
      }
      collector = new FilterCollector(groupFilt, subCollector);
      return collector;
    }

    @Override
    protected void finish() throws IOException {
      TopDocs topDocs = topCollector.topDocs();
      float maxScore;
      if (withinGroupSort == null || withinGroupSort.equals(Sort.RELEVANCE)) {
        maxScore = topDocs.scoreDocs.length == 0 ? Float.NaN : topDocs.scoreDocs[0].score;
      } else if (needScores) {
        // use top-level query to populate the scores
        TopFieldCollector.populateScores(topDocs.scoreDocs, searcher, Grouping.this.query);
        maxScore = maxScoreCollector.getMaxScore();
      } else {
        maxScore = Float.NaN;
      }
      
      GroupDocs<String> groupDocs = new GroupDocs<>(Float.NaN, maxScore, topDocs.totalHits, topDocs.scoreDocs, query.toString(), null);
      if (main) {
        mainResult = getDocList(groupDocs);
      } else {
        NamedList rsp = commonResponse();
        addDocList(rsp, groupDocs);
      }
    }

    @Override
    public int getMatches() {
      return collector.getMatches();
    }
  }

  /**
   * A command for grouping on a function.
   */
  public class CommandFunc extends Command<MutableValue> {

    public ValueSource groupBy;
    @SuppressWarnings({"rawtypes"})
    Map context;

    @SuppressWarnings({"unchecked"})
    private ValueSourceGroupSelector newSelector() {
      return new ValueSourceGroupSelector(groupBy, context);
    }

    FirstPassGroupingCollector<MutableValue> firstPass;
    TopGroupsCollector<MutableValue> secondPass;
    // If offset falls outside the number of documents a group can provide use this collector instead of secondPass
    TotalHitCountCollector fallBackCollector;
    AllGroupsCollector<MutableValue> allGroupsCollector;
    Collection<SearchGroup<MutableValue>> topGroups;

    @Override
    @SuppressWarnings({"unchecked"})
    protected void prepare() throws IOException {
      context = ValueSource.newContext(searcher);
      groupBy.createWeight(context, searcher);
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    @Override
    protected Collector createFirstPassCollector() throws IOException {
      // Ok we don't want groups, but do want a total count
      if (actualGroupsToFind <= 0) {
        fallBackCollector = new TotalHitCountCollector();
        return fallBackCollector;
      }

      groupSort = groupSort == null ? Sort.RELEVANCE : groupSort;
      firstPass = new FirstPassGroupingCollector<>(newSelector(), searcher.weightSort(groupSort), actualGroupsToFind);
      return firstPass;
    }

    @Override
    protected Collector createSecondPassCollector() throws IOException {
      if (actualGroupsToFind <= 0) {
        allGroupsCollector = new AllGroupsCollector<>(newSelector());
        return totalCount == TotalCount.grouped ? allGroupsCollector : null;
      }

      topGroups = format == Format.grouped ? firstPass.getTopGroups(offset) : firstPass.getTopGroups(0);
      if (topGroups == null) {
        if (totalCount == TotalCount.grouped) {
          allGroupsCollector = new AllGroupsCollector<>(newSelector());
          fallBackCollector = new TotalHitCountCollector();
          return MultiCollector.wrap(allGroupsCollector, fallBackCollector);
        } else {
          fallBackCollector = new TotalHitCountCollector();
          return fallBackCollector;
        }
      }

      int groupdDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupdDocsToCollect = Math.max(groupdDocsToCollect, 1);
      Sort withinGroupSort = this.withinGroupSort != null ? this.withinGroupSort : Sort.RELEVANCE;
      secondPass = new TopGroupsCollector<>(newSelector(),
          topGroups, groupSort, withinGroupSort, groupdDocsToCollect, needScores
      );

      if (totalCount == TotalCount.grouped) {
        allGroupsCollector = new AllGroupsCollector<>(newSelector());
        return MultiCollector.wrap(secondPass, allGroupsCollector);
      } else {
        return secondPass;
      }
    }

    @Override
    public AllGroupHeadsCollector<?> createAllGroupCollector() throws IOException {
      Sort sortWithinGroup = withinGroupSort != null ? withinGroupSort : Sort.RELEVANCE;
      return AllGroupHeadsCollector.newCollector(newSelector(), sortWithinGroup);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    protected void finish() throws IOException {
      if (secondPass != null) {
        result = secondPass.getTopGroups(0);
        populateScoresIfNecessary();
      }
      if (main) {
        mainResult = createSimpleResponse();
        return;
      }

      @SuppressWarnings({"rawtypes"})
      NamedList groupResult = commonResponse();

      if (format == Format.simple) {
        groupResult.add("doclist", createSimpleResponse());
        return;
      }

      @SuppressWarnings({"rawtypes"})
      List groupList = new ArrayList();
      groupResult.add("groups", groupList);        // grouped={ key={ groups=[

      if (result == null) {
        return;
      }

      // handle case of rows=0
      if (numGroups == 0) return;

      for (GroupDocs<MutableValue> group : result.groups) {
        @SuppressWarnings({"rawtypes"})
        NamedList nl = new SimpleOrderedMap();
        groupList.add(nl);                         // grouped={ key={ groups=[ {
        nl.add("groupValue", group.groupValue.toObject());
        addDocList(nl, group);
      }
    }

    @Override
    public int getMatches() {
      if (result == null && fallBackCollector == null) {
        return 0;
      }

      return result != null ? result.totalHitCount : fallBackCollector.getTotalHits();
    }

    @Override
    protected Integer getNumberOfGroups() {
      return allGroupsCollector == null ? null : allGroupsCollector.getGroupCount();
    }

  }

}
