package org.apache.solr.search.grouping;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.grouping.AbstractAllGroupHeadsCollector;
import org.apache.lucene.search.grouping.term.TermAllGroupHeadsCollector;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSetCollector;
import org.apache.solr.search.DocSetDelegateCollector;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrIndexSearcher.ProcessedFilter;
import org.apache.solr.search.grouping.distributed.shardresultserializer.ShardResultTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for executing a search with a number of {@link Command} instances.
 * A typical search can have more then one {@link Command} instances.
 *
 * @lucene.experimental
 */
public class CommandHandler {

  public static class Builder {

    private SolrIndexSearcher.QueryCommand queryCommand;
    private List<Command> commands = new ArrayList<>();
    private SolrIndexSearcher searcher;
    private boolean needDocSet = false;
    private boolean truncateGroups = false;
    private boolean includeHitCount = false;

    public Builder setQueryCommand(SolrIndexSearcher.QueryCommand queryCommand) {
      this.queryCommand = queryCommand;
      this.needDocSet = (queryCommand.getFlags() & SolrIndexSearcher.GET_DOCSET) != 0;
      return this;
    }

    public Builder addCommandField(Command commandField) {
      commands.add(commandField);
      return this;
    }

    public Builder setSearcher(SolrIndexSearcher searcher) {
      this.searcher = searcher;
      return this;
    }

    /**
     * Sets whether to compute a {@link DocSet}.
     * May override the value set by {@link #setQueryCommand(org.apache.solr.search.SolrIndexSearcher.QueryCommand)}.
     *
     * @param needDocSet Whether to compute a {@link DocSet}
     * @return this
     */
    public Builder setNeedDocSet(boolean needDocSet) {
      this.needDocSet = needDocSet;
      return this;
    }

    public Builder setTruncateGroups(boolean truncateGroups) {
      this.truncateGroups = truncateGroups;
      return this;
    }

    public Builder setIncludeHitCount(boolean includeHitCount) {
      this.includeHitCount = includeHitCount;
      return this;
    }

    public CommandHandler build() {
      if (queryCommand == null || searcher == null) {
        throw new IllegalStateException("All fields must be set");
      }

      return new CommandHandler(queryCommand, commands, searcher, needDocSet, truncateGroups, includeHitCount);
    }

  }

  private final static Logger logger = LoggerFactory.getLogger(CommandHandler.class);

  private final SolrIndexSearcher.QueryCommand queryCommand;
  private final List<Command> commands;
  private final SolrIndexSearcher searcher;
  private final boolean needDocset;
  private final boolean truncateGroups;
  private final boolean includeHitCount;
  private boolean partialResults = false;
  private int totalHitCount;

  private DocSet docSet;

  private CommandHandler(SolrIndexSearcher.QueryCommand queryCommand,
                         List<Command> commands,
                         SolrIndexSearcher searcher,
                         boolean needDocset,
                         boolean truncateGroups,
                         boolean includeHitCount) {
    this.queryCommand = queryCommand;
    this.commands = commands;
    this.searcher = searcher;
    this.needDocset = needDocset;
    this.truncateGroups = truncateGroups;
    this.includeHitCount = includeHitCount;
  }

  @SuppressWarnings("unchecked")
  public void execute() throws IOException {
    final int nrOfCommands = commands.size();
    List<Collector> collectors = new ArrayList<>(nrOfCommands);
    for (Command command : commands) {
      collectors.addAll(command.create());
    }

    ProcessedFilter filter = searcher.getProcessedFilter
      (queryCommand.getFilter(), queryCommand.getFilterList());
    Query query = QueryUtils.makeQueryable(queryCommand.getQuery());

    if (truncateGroups) {
      docSet = computeGroupedDocSet(query, filter, collectors);
    } else if (needDocset) {
      docSet = computeDocSet(query, filter, collectors);
    } else if (!collectors.isEmpty()) {
      searchWithTimeLimiter(query, filter, MultiCollector.wrap(collectors.toArray(new Collector[nrOfCommands])));
    } else {
      searchWithTimeLimiter(query, filter, null);
    }
  }

  private DocSet computeGroupedDocSet(Query query, ProcessedFilter filter, List<Collector> collectors) throws IOException {
    Command firstCommand = commands.get(0);
    AbstractAllGroupHeadsCollector termAllGroupHeadsCollector =
        TermAllGroupHeadsCollector.create(firstCommand.getKey(), firstCommand.getSortWithinGroup());
    if (collectors.isEmpty()) {
      searchWithTimeLimiter(query, filter, termAllGroupHeadsCollector);
    } else {
      collectors.add(termAllGroupHeadsCollector);
      searchWithTimeLimiter(query, filter, MultiCollector.wrap(collectors.toArray(new Collector[collectors.size()])));
    }

    return new BitDocSet(termAllGroupHeadsCollector.retrieveGroupHeads(searcher.maxDoc()));
  }

  private DocSet computeDocSet(Query query, ProcessedFilter filter, List<Collector> collectors) throws IOException {
    int maxDoc = searcher.maxDoc();
    DocSetCollector docSetCollector;
    if (collectors.isEmpty()) {
      docSetCollector = new DocSetCollector(maxDoc >> 6, maxDoc);
    } else {
      Collector wrappedCollectors = MultiCollector.wrap(collectors.toArray(new Collector[collectors.size()]));
      docSetCollector = new DocSetDelegateCollector(maxDoc >> 6, maxDoc, wrappedCollectors);
    }
    searchWithTimeLimiter(query, filter, docSetCollector);
    return docSetCollector.getDocSet();
  }

  @SuppressWarnings("unchecked")
  public NamedList processResult(SolrIndexSearcher.QueryResult queryResult, ShardResultTransformer transformer) throws IOException {
    if (docSet != null) {
      queryResult.setDocSet(docSet);
    }
    queryResult.setPartialResults(partialResults);
    return transformer.transform(commands);
  }

  /**
   * Invokes search with the specified filter and collector.  
   * If a time limit has been specified then wrap the collector in the TimeLimitingCollector
   */
  private void searchWithTimeLimiter(final Query query, 
                                     final ProcessedFilter filter, 
                                     Collector collector) throws IOException {
    if (queryCommand.getTimeAllowed() > 0 ) {
      collector = new TimeLimitingCollector(collector, TimeLimitingCollector.getGlobalCounter(), queryCommand.getTimeAllowed());
    }

    TotalHitCountCollector hitCountCollector = new TotalHitCountCollector();
    if (includeHitCount) {
      collector = MultiCollector.wrap(collector, hitCountCollector);
    }

    Filter luceneFilter = filter.filter;
    if (filter.postFilter != null) {
      filter.postFilter.setLastDelegate(collector);
      collector = filter.postFilter;
    }

    try {
      searcher.search(query, luceneFilter, collector);
    } catch (TimeLimitingCollector.TimeExceededException x) {
      partialResults = true;
      logger.warn( "Query: " + query + "; " + x.getMessage() );
    }

    if (includeHitCount) {
      totalHitCount = hitCountCollector.getTotalHits();
    }
  }

  public int getTotalHitCount() {
    return totalHitCount;
  }
}
