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

import org.apache.commons.lang.ArrayUtils;
import org.apache.lucene.common.mutable.MutableValue;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.queries.function.DocValues;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.search.grouping.*;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Basic Solr Grouping infrastructure.
 * Warning NOT thread save!
 *
 * @lucene.experimental
 */
public class Grouping {

  private final static Logger logger = LoggerFactory.getLogger(Grouping.class);

  private final SolrIndexSearcher searcher;
  private final SolrIndexSearcher.QueryResult qr;
  private final SolrIndexSearcher.QueryCommand cmd;
  private final List<Command> commands = new ArrayList<Command>();
  private final boolean main;
  private final boolean cacheSecondPassSearch;
  private final int maxDocsPercentageToCache;

  private Sort sort;
  private Sort groupSort;
  private int limitDefault;
  private int docsPerGroupDefault;
  private int groupOffsetDefault;
  private Format defaultFormat;
  private TotalCount defaultTotalCount;

  private int maxDoc;
  private boolean needScores;
  private boolean getDocSet;
  private boolean getDocList; // doclist needed for debugging or highlighting
  private Query query;
  private DocSet filter;
  private Filter luceneFilter;
  private NamedList grouped = new SimpleOrderedMap();
  private Set<Integer> idSet = new LinkedHashSet<Integer>();  // used for tracking unique docs when we need a doclist
  private int maxMatches;  // max number of matches from any grouping command
  private float maxScore = Float.NEGATIVE_INFINITY;  // max score seen in any doclist
  private boolean signalCacheWarning = false;


  public DocList mainResult;  // output if one of the grouping commands should be used as the main result.

  /**
   * @param searcher
   * @param qr
   * @param cmd
   * @param cacheSecondPassSearch Whether to cache the documents and scores from the first pass search for the second
   *                              pass search.
   * @param maxDocsPercentageToCache The maximum number of documents in a percentage relative from maxdoc
   *                                 that is allowed in the cache. When this threshold is met,
   *                                 the cache is not used in the second pass search.
   */
  public Grouping(SolrIndexSearcher searcher,
                  SolrIndexSearcher.QueryResult qr,
                  SolrIndexSearcher.QueryCommand cmd,
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

  public void add(Grouping.Command groupingCommand) {
    commands.add(groupingCommand);
  }

  /**
   * Adds a field command based on the specified field.
   * If the field is not compatible with {@link CommandField} it invokes the
   * {@link #addFunctionCommand(String, org.apache.solr.request.SolrQueryRequest)} method.
   *
   * @param field The fieldname to group by.
   */
  public void addFieldCommand(String field, SolrQueryRequest request) throws ParseException {
    SchemaField schemaField = searcher.getSchema().getField(field); // Throws an exception when field doesn't exist. Bad request.
    FieldType fieldType = schemaField.getType();
    ValueSource valueSource = fieldType.getValueSource(schemaField, null);
    if (!(valueSource instanceof StrFieldSource)) {
      addFunctionCommand(field, request);
      return;
    }

    Grouping.CommandField gc = new CommandField();
    gc.groupSort = groupSort;
    gc.groupBy = field;
    gc.key = field;
    gc.numGroups = limitDefault;
    gc.docsPerGroup = docsPerGroupDefault;
    gc.groupOffset = groupOffsetDefault;
    gc.offset = cmd.getOffset();
    gc.sort = sort;
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

  public void addFunctionCommand(String groupByStr, SolrQueryRequest request) throws ParseException {
    QParser parser = QParser.getParser(groupByStr, "func", request);
    Query q = parser.getQuery();
    final Grouping.Command gc;
    if (q instanceof FunctionQuery) {
      ValueSource valueSource = ((FunctionQuery)q).getValueSource();
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
    gc.groupSort = groupSort;
    gc.key = groupByStr;
    gc.numGroups = limitDefault;
    gc.docsPerGroup = docsPerGroupDefault;
    gc.groupOffset = groupOffsetDefault;
    gc.offset = cmd.getOffset();
    gc.sort = sort;
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

  public void addQueryCommand(String groupByStr, SolrQueryRequest request) throws ParseException {
    QParser parser = QParser.getParser(groupByStr, null, request);
    Query gq = parser.getQuery();
    Grouping.CommandQuery gc = new CommandQuery();
    gc.query = gq;
    gc.groupSort = groupSort;
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

  public Grouping setSort(Sort sort) {
    this.sort = sort;
    return this;
  }

  public Grouping setGroupSort(Sort groupSort) {
    this.groupSort = groupSort;
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

  public List<Command> getCommands() {
    return commands;
  }

  public void execute() throws IOException {
    if (commands.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Specify at least on field, function or query to group by.");
    }

    DocListAndSet out = new DocListAndSet();
    qr.setDocListAndSet(out);

    SolrIndexSearcher.ProcessedFilter pf = searcher.getProcessedFilter(cmd.getFilter(), cmd.getFilterList());
    final Filter luceneFilter = pf.filter;
    maxDoc = searcher.maxDoc();

    needScores = (cmd.getFlags() & SolrIndexSearcher.GET_SCORES) != 0;
    boolean cacheScores = false;
    // NOTE: Change this when groupSort can be specified per group
    if (!needScores && !commands.isEmpty()) {
      if (commands.get(0).groupSort == null) {
        cacheScores = true;
      } else {
        for (SortField field : commands.get(0).groupSort.getSort()) {
          if (field.getType() == SortField.Type.SCORE) {
            cacheScores = true;
            break;
          }
        }
      }
    } else if (needScores) {
      cacheScores = needScores;
    }
    getDocSet = (cmd.getFlags() & SolrIndexSearcher.GET_DOCSET) != 0;
    getDocList = (cmd.getFlags() & SolrIndexSearcher.GET_DOCLIST) != 0;
    query = QueryUtils.makeQueryable(cmd.getQuery());

    for (Command cmd : commands) {
      cmd.prepare();
    }

    List<Collector> collectors = new ArrayList<Collector>(commands.size());
    for (Command cmd : commands) {
      Collector collector = cmd.createFirstPassCollector();
      if (collector != null)
        collectors.add(collector);
    }

    Collector allCollectors = MultiCollector.wrap(collectors.toArray(new Collector[collectors.size()]));
    DocSetCollector setCollector = null;
    if (getDocSet) {
      setCollector = new DocSetDelegateCollector(maxDoc >> 6, maxDoc, allCollectors);
      allCollectors = setCollector;
    }

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
      searcher.search(query, luceneFilter, allCollectors);
    }

    if (getDocSet) {
      qr.setDocSet(setCollector.getDocSet());
    }

    collectors.clear();
    for (Command cmd : commands) {
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
            logger.warn(String.format("The grouping cache is active, but not used because it exceeded the max cache limit of %d percent", maxDocsPercentageToCache));
            logger.warn("Please increase cache size or disable group caching.");
            searcher.search(query, luceneFilter, secondPhaseCollectors);
          }
        } else {
          if (pf.postFilter != null) {
            pf.postFilter.setLastDelegate(secondPhaseCollectors);
            secondPhaseCollectors = pf.postFilter;
          }
          searcher.search(query, luceneFilter, secondPhaseCollectors);
        }
      }
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

  /**
   * Returns offset + len if len equals zero or higher. Otherwise returns max.
   *
   * @param offset The offset
   * @param len The number of documents to return
   * @param max The number of document to return if len < 0 or if offset + len < 0
   * @return offset + len if len equals zero or higher. Otherwise returns max
   */
  int getMax(int offset, int len, int max) {
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
   * <p/>
   * Note: Maybe the creating the response structure should be done in something like a ReponseBuilder???
   * Warning NOT thread save!
   */
  public abstract class Command<GROUP_VALUE_TYPE> {

    public String key;       // the name to use for this group in the response
    public Sort groupSort;   // the sort of the documents *within* a single group.
    public Sort sort;        // the sort between groups
    public int docsPerGroup; // how many docs in each group - from "group.limit" param, default=1
    public int groupOffset;  // the offset within each group (for paging within each group)
    public int numGroups;    // how many groups - defaults to the "rows" parameter
    int actualGroupsToFind;  // How many groups should actually be found. Based on groupOffset and numGroups.
    public int offset;       // offset into the list of groups
    public Format format;
    public boolean main;     // use as the main result in simple format (grouped.main=true param)
    public TotalCount totalCount = TotalCount.ungrouped;

    TopGroups<GROUP_VALUE_TYPE> result;


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

    protected NamedList commonResponse() {
      NamedList groupResult = new SimpleOrderedMap();
      grouped.add(key, groupResult);  // grouped={ key={

      int matches = getMatches();
      groupResult.add("matches", matches);
      if (totalCount == TotalCount.grouped) {
        Integer totalNrOfGroups = getNumberOfGroups();
        groupResult.add("ngroups", totalNrOfGroups == null ? 0 : totalNrOfGroups);
      }
      maxMatches = Math.max(maxMatches, matches);
      return groupResult;
    }

    protected DocList getDocList(GroupDocs groups) {
      int max = groups.totalHits;
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
      maxScore = Math.max(maxScore, score);
      DocSlice docs = new DocSlice(off, Math.max(0, ids.length - off), ids, scores, groups.totalHits, score);

      if (getDocList) {
        DocIterator iter = docs.iterator();
        while (iter.hasNext())
          idSet.add(iter.nextDoc());
      }
      return docs;
    }

    protected void addDocList(NamedList rsp, GroupDocs groups) {
      rsp.add("doclist", getDocList(groups));
    }

    // Flatten the groups and get up offset + rows documents
    protected DocList createSimpleResponse() {
      GroupDocs[] groups = result != null ? result.groups : new GroupDocs[0];

      List<Integer> ids = new ArrayList<Integer>();
      List<Float> scores = new ArrayList<Float>();
      int docsToGather = getMax(offset, numGroups, maxDoc);
      int docsGathered = 0;
      float maxScore = Float.NEGATIVE_INFINITY;

      outer:
      for (GroupDocs group : groups) {
        if (group.maxScore > maxScore) {
          maxScore = group.maxScore;
        }

        for (ScoreDoc scoreDoc : group.scoreDocs) {
          if (docsGathered >= docsToGather) {
            break outer;
          }

          ids.add(scoreDoc.doc);
          scores.add(scoreDoc.score);
          docsGathered++;
        }
      }

      int len = Math.min(numGroups, docsGathered);
      if (offset > len) {
        len = 0;
      }

      int[] docs = ArrayUtils.toPrimitive(ids.toArray(new Integer[ids.size()]));
      float[] docScores = ArrayUtils.toPrimitive(scores.toArray(new Float[scores.size()]));
      DocSlice docSlice = new DocSlice(offset, len, docs, docScores, getMatches(), maxScore);

      if (getDocList) {
        for (int i = offset; i < docs.length; i++) {
          idSet.add(docs[i]);
        }
      }

      return docSlice;
    }

  }

  /**
   * A group command for grouping on a field.
   */
  public class CommandField extends Command<BytesRef> {

    public String groupBy;
    TermFirstPassGroupingCollector firstPass;
    TermSecondPassGroupingCollector secondPass;

    TermAllGroupsCollector allGroupsCollector;

    // If offset falls outside the number of documents a group can provide use this collector instead of secondPass
    TotalHitCountCollector fallBackCollector;
    Collection<SearchGroup<BytesRef>> topGroups;

    /**
     * {@inheritDoc}
     */
    protected void prepare() throws IOException {
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    /**
     * {@inheritDoc}
     */
    protected Collector createFirstPassCollector() throws IOException {
      // Ok we don't want groups, but do want a total count
      if (actualGroupsToFind <= 0) {
        fallBackCollector = new TotalHitCountCollector();
        return fallBackCollector;
      }

      sort = sort == null ? Sort.RELEVANCE : sort;
      firstPass = new TermFirstPassGroupingCollectorJava6(groupBy, sort, actualGroupsToFind);
      return firstPass;
    }

    /**
     * {@inheritDoc}
     */
    protected Collector createSecondPassCollector() throws IOException {
      if (actualGroupsToFind <= 0) {
        allGroupsCollector = new TermAllGroupsCollector(groupBy);
        return totalCount == TotalCount.grouped ? allGroupsCollector : null;
      }

      topGroups = format == Format.grouped ? firstPass.getTopGroups(offset, false) : firstPass.getTopGroups(0, false);
      if (topGroups == null) {
        if (totalCount == TotalCount.grouped) {
          allGroupsCollector = new TermAllGroupsCollector(groupBy);
          fallBackCollector = new TotalHitCountCollector();
          return MultiCollector.wrap(allGroupsCollector, fallBackCollector);
        } else {
          fallBackCollector = new TotalHitCountCollector();
          return fallBackCollector;
        }
      }

      int groupedDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupedDocsToCollect = Math.max(groupedDocsToCollect, 1);
      secondPass = new TermSecondPassGroupingCollector(
          groupBy, topGroups, sort, groupSort, groupedDocsToCollect, needScores, needScores, false
      );

      if (totalCount == TotalCount.grouped) {
        allGroupsCollector = new TermAllGroupsCollector(groupBy);
        return MultiCollector.wrap(secondPass, allGroupsCollector);
      } else {
        return secondPass;
      }
    }

    /**
     * {@inheritDoc}
     */
    protected void finish() throws IOException {
      result = secondPass != null ? secondPass.getTopGroups(0) : null;
      if (main) {
        mainResult = createSimpleResponse();
        return;
      }

      NamedList groupResult = commonResponse();

      if (format == Format.simple) {
        groupResult.add("doclist", createSimpleResponse());
        return;
      }

      List groupList = new ArrayList();
      groupResult.add("groups", groupList);        // grouped={ key={ groups=[

      if (result == null) {
        return;
      }

      // handle case of rows=0
      if (numGroups == 0) return;

      for (GroupDocs<BytesRef> group : result.groups) {
        NamedList nl = new SimpleOrderedMap();
        groupList.add(nl);                         // grouped={ key={ groups=[ {


        // To keep the response format compatable with trunk.
        // In trunk MutableValue can convert an indexed value to its native type. E.g. string to int
        // The only option I currently see is the use the FieldType for this
        if (group.groupValue != null) {
          SchemaField schemaField = searcher.getSchema().getField(groupBy);
          FieldType fieldType = schemaField.getType();
          String readableValue = fieldType.indexedToReadable(group.groupValue.utf8ToString());
          Fieldable field = schemaField.createField(readableValue, 0.0f);
          nl.add("groupValue", fieldType.toObject(field));
        } else {
          nl.add("groupValue", null);
        }

        addDocList(nl, group);
      }
    }

    /**
     * {@inheritDoc}
     */
    public int getMatches() {
      if (result == null && fallBackCollector == null) {
        return 0;
      }

      return result != null ? result.totalHitCount : fallBackCollector.getTotalHits();
    }

    /**
     * {@inheritDoc}
     */
    protected Integer getNumberOfGroups() {
      return allGroupsCollector == null ? null : allGroupsCollector.getGroupCount();
    }
  }

  /**
   * A group command for grouping on a query.
   */
  //NOTE: doesn't need to be generic. Maybe Command interface --> First / Second pass abstract impl.
  public class CommandQuery extends Command {

    public Query query;
    TopDocsCollector topCollector;
    FilterCollector collector;

    /**
     * {@inheritDoc}
     */
    protected void prepare() throws IOException {
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    /**
     * {@inheritDoc}
     */
    protected Collector createFirstPassCollector() throws IOException {
      DocSet groupFilt = searcher.getDocSet(query);
      topCollector = newCollector(groupSort, needScores);
      collector = new FilterCollector(groupFilt, topCollector);
      return collector;
    }

    TopDocsCollector newCollector(Sort sort, boolean needScores) throws IOException {
      int groupDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      if (sort == null || sort == Sort.RELEVANCE) {
        return TopScoreDocCollector.create(groupDocsToCollect, true);
      } else {
        return TopFieldCollector.create(searcher.weightSort(sort), groupDocsToCollect, false, needScores, needScores, true);
      }
    }

    /**
     * {@inheritDoc}
     */
    protected void finish() throws IOException {
      TopDocsCollector topDocsCollector = (TopDocsCollector) collector.collector;
      TopDocs topDocs = topDocsCollector.topDocs();
      GroupDocs<String> groupDocs = new GroupDocs<String>(topDocs.getMaxScore(), topDocs.totalHits, topDocs.scoreDocs, query.toString(), null);
      if (main) {
        mainResult = getDocList(groupDocs);
      } else {
        NamedList rsp = commonResponse();
        addDocList(rsp, groupDocs);
      }
    }

    /**
     * {@inheritDoc}
     */
    public int getMatches() {
      return collector.matches;
    }
  }

  /**
   * A command for grouping on a function.
   */
  public class CommandFunc extends Command<MutableValue> {

    public ValueSource groupBy;
    Map context;

    FunctionFirstPassGroupingCollector firstPass;
    FunctionSecondPassGroupingCollector secondPass;
    // If offset falls outside the number of documents a group can provide use this collector instead of secondPass
    TotalHitCountCollector fallBackCollector;
    FunctionAllGroupsCollector allGroupsCollector;
    Collection<SearchGroup<MutableValue>> topGroups;

    /**
     * {@inheritDoc}
     */
    protected void prepare() throws IOException {
      Map context = ValueSource.newContext(searcher);
      groupBy.createWeight(context, searcher);
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    /**
     * {@inheritDoc}
     */
    protected Collector createFirstPassCollector() throws IOException {
      // Ok we don't want groups, but do want a total count
      if (actualGroupsToFind <= 0) {
        fallBackCollector = new TotalHitCountCollector();
        return fallBackCollector;
      }

      sort = sort == null ? Sort.RELEVANCE : sort;
      firstPass = new FunctionFirstPassGroupingCollector(groupBy, context, searcher.weightSort(sort), actualGroupsToFind);
      return firstPass;
    }

    /**
     * {@inheritDoc}
     */
    protected Collector createSecondPassCollector() throws IOException {
      if (actualGroupsToFind <= 0) {
        allGroupsCollector = new FunctionAllGroupsCollector(groupBy, context);
        return totalCount == TotalCount.grouped ? allGroupsCollector : null;
      }

      topGroups = format == Format.grouped ? firstPass.getTopGroups(offset, false) : firstPass.getTopGroups(0, false);
      if (topGroups == null) {
        if (totalCount == TotalCount.grouped) {
          allGroupsCollector = new FunctionAllGroupsCollector(groupBy, context);
          fallBackCollector = new TotalHitCountCollector();
          return MultiCollector.wrap(allGroupsCollector, fallBackCollector);
        } else {
          fallBackCollector = new TotalHitCountCollector();
          return fallBackCollector;
        }
      }

      int groupdDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupdDocsToCollect = Math.max(groupdDocsToCollect, 1);
      secondPass = new FunctionSecondPassGroupingCollector(
          topGroups, sort, groupSort, groupdDocsToCollect, needScores, needScores, false, groupBy, context
      );

      if (totalCount == TotalCount.grouped) {
        allGroupsCollector = new FunctionAllGroupsCollector(groupBy, context);
        return MultiCollector.wrap(secondPass, allGroupsCollector);
      } else {
        return secondPass;
      }
    }

    /**
     * {@inheritDoc}
     */
    protected void finish() throws IOException {
      result = secondPass != null ? secondPass.getTopGroups(0) : null;
      if (main) {
        mainResult = createSimpleResponse();
        return;
      }

      NamedList groupResult = commonResponse();

      if (format == Format.simple) {
        groupResult.add("doclist", createSimpleResponse());
        return;
      }

      List groupList = new ArrayList();
      groupResult.add("groups", groupList);        // grouped={ key={ groups=[

      if (result == null) {
        return;
      }

      // handle case of rows=0
      if (numGroups == 0) return;

      for (GroupDocs<MutableValue> group : result.groups) {
        NamedList nl = new SimpleOrderedMap();
        groupList.add(nl);                         // grouped={ key={ groups=[ {
        nl.add("groupValue", group.groupValue.toObject());
        addDocList(nl, group);
      }
    }

    /**
     * {@inheritDoc}
     */
    public int getMatches() {
      if (result == null && fallBackCollector == null) {
        return 0;
      }

      return result != null ? result.totalHitCount : fallBackCollector.getTotalHits();
    }

    /**
     * {@inheritDoc}
     */
    protected Integer getNumberOfGroups() {
      return allGroupsCollector == null ? null : allGroupsCollector.getGroupCount();
    }

  }

  /**
   * A collector that filters incoming doc ids that are not in the filter
   */
  static class FilterCollector extends Collector {

    final DocSet filter;
    final Collector collector;
    int docBase;
    int matches;

    public FilterCollector(DocSet filter, Collector collector) throws IOException {
      this.filter = filter;
      this.collector = collector;
    }

    public void setScorer(Scorer scorer) throws IOException {
      collector.setScorer(scorer);
    }

    public void collect(int doc) throws IOException {
      matches++;
      if (filter.exists(doc + docBase)) {
        collector.collect(doc);
      }
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.docBase = context.docBase;
      collector.setNextReader(context);
    }

    public boolean acceptsDocsOutOfOrder() {
      return collector.acceptsDocsOutOfOrder();
    }
  }

  static class FunctionFirstPassGroupingCollector extends AbstractFirstPassGroupingCollector<MutableValue> {

    private final ValueSource groupByVS;
    private final Map vsContext;

    private DocValues docValues;
    private DocValues.ValueFiller filler;
    private MutableValue mval;

    FunctionFirstPassGroupingCollector(ValueSource groupByVS, Map vsContext, Sort groupSort, int topNGroups) throws IOException {
      super(groupSort, topNGroups);
      this.groupByVS = groupByVS;
      this.vsContext = vsContext;
    }

    @Override
    protected MutableValue getDocGroupValue(int doc) {
      filler.fillValue(doc);
      return mval;
    }

    @Override
    protected MutableValue copyDocGroupValue(MutableValue groupValue, MutableValue reuse) {
      if (reuse != null) {
        reuse.copy(groupValue);
        return reuse;
      }
      return groupValue.duplicate();
    }

    @Override
    public void setNextReader(AtomicReaderContext readerContext) throws IOException {
      super.setNextReader(readerContext);
      docValues = groupByVS.getValues(vsContext, readerContext);
      filler = docValues.getValueFiller();
      mval = filler.getValue();
    }

    @Override
    protected CollectedSearchGroup<MutableValue> pollLast() {
      return orderedGroups.pollLast();
    }
  }

  static class TermFirstPassGroupingCollectorJava6 extends TermFirstPassGroupingCollector {
    public TermFirstPassGroupingCollectorJava6(String groupField, Sort groupSort, int topNGroups) throws IOException {
      super(groupField, groupSort, topNGroups);
    }

    @Override
    protected CollectedSearchGroup<BytesRef> pollLast() {
      return orderedGroups.pollLast();
    }
  }

  static class FunctionSecondPassGroupingCollector extends AbstractSecondPassGroupingCollector<MutableValue> {

    private final ValueSource groupByVS;
    private final Map vsContext;

    private DocValues docValues;
    private DocValues.ValueFiller filler;
    private MutableValue mval;

    FunctionSecondPassGroupingCollector(Collection<SearchGroup<MutableValue>> searchGroups, Sort groupSort, Sort withinGroupSort, int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields, ValueSource groupByVS, Map vsContext) throws IOException {
      super(searchGroups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
      this.groupByVS = groupByVS;
      this.vsContext = vsContext;
    }

    /**
     * {@inheritDoc}
     */
    protected SearchGroupDocs<MutableValue> retrieveGroup(int doc) throws IOException {
      filler.fillValue(doc);
      return groupMap.get(mval);
    }

    /**
     * {@inheritDoc}
     */
    public void setNextReader(AtomicReaderContext readerContext) throws IOException {
      super.setNextReader(readerContext);
      docValues = groupByVS.getValues(vsContext, readerContext);
      filler = docValues.getValueFiller();
      mval = filler.getValue();
    }
  }


  static class FunctionAllGroupsCollector extends AbstractAllGroupsCollector<MutableValue> {

    private final Map vsContext;
    private final ValueSource groupBy;
    private final SortedSet<MutableValue> groups = new TreeSet<MutableValue>();

    private DocValues docValues;
    private DocValues.ValueFiller filler;
    private MutableValue mval;

    FunctionAllGroupsCollector(ValueSource groupBy, Map vsContext) {
      this.vsContext = vsContext;
      this.groupBy = groupBy;
    }

    public Collection<MutableValue> getGroups() {
      return groups;
    }

    public void collect(int doc) throws IOException {
      filler.fillValue(doc);
      if (!groups.contains(mval)) {
        groups.add(mval.duplicate());
      }
    }

    /**
     * {@inheritDoc}
     */
    public void setNextReader(AtomicReaderContext context) throws IOException {
      docValues = groupBy.getValues(vsContext, context);
      filler = docValues.getValueFiller();
      mval = filler.getValue();
    }

  }

}
