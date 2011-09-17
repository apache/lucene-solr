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

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.*;
import org.apache.solr.search.grouping.distributed.shardresultserializer.ShardResultTransformer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class CommandHandler {

  public static class Builder {

    private SolrIndexSearcher.QueryCommand queryCommand;
    private List<Command> commands = new ArrayList<Command>();
    private SolrIndexSearcher searcher;
    private boolean needDocSet = false;

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

    public CommandHandler build() {
      if (queryCommand == null || searcher == null) {
        throw new IllegalStateException("All fields must be set");
      }

      return new CommandHandler(queryCommand, commands, searcher, needDocSet);
    }

  }

  private final SolrIndexSearcher.QueryCommand queryCommand;
  private final List<Command> commands;
  private final SolrIndexSearcher searcher;
  private final boolean needDocset;

  private DocSet docSet;

  private CommandHandler(SolrIndexSearcher.QueryCommand queryCommand,
                         List<Command> commands,
                         SolrIndexSearcher searcher,
                         boolean needDocset) {
    this.queryCommand = queryCommand;
    this.commands = commands;
    this.searcher = searcher;
    this.needDocset = needDocset;
  }

  @SuppressWarnings("unchecked")
  public void execute() throws IOException {
    final int nrOfCommands = commands.size();
    List<Collector> collectors = new ArrayList<Collector>(nrOfCommands);
    for (Command command : commands) {
      collectors.addAll(command.create());
    }

    SolrIndexSearcher.ProcessedFilter pf = searcher.getProcessedFilter(
        queryCommand.getFilter(), queryCommand.getFilterList()
    );
    Filter luceneFilter = pf.filter;
    Query query = QueryUtils.makeQueryable(queryCommand.getQuery());
    Collector wrappedCollectors;
    if (collectors.isEmpty()) {
      wrappedCollectors = null;
    } else {
      wrappedCollectors = MultiCollector.wrap(collectors.toArray(new Collector[nrOfCommands]));
    }

    if (wrappedCollectors == null && needDocset) {
      int maxDoc = searcher.maxDoc();
      DocSetCollector docSetCollector = new DocSetCollector(maxDoc >> 6, maxDoc);
      searcher.search(query, luceneFilter, docSetCollector);
      docSet = docSetCollector.getDocSet();
    } else if (needDocset) {
      int maxDoc = searcher.maxDoc();
      DocSetCollector docSetCollector = new DocSetDelegateCollector(maxDoc >> 6, maxDoc, wrappedCollectors);
      searcher.search(query, luceneFilter, docSetCollector);
      docSet = docSetCollector.getDocSet();
    } else {
      searcher.search(query, luceneFilter, wrappedCollectors);
    }
  }

  @SuppressWarnings("unchecked")
  public NamedList processResult(SolrIndexSearcher.QueryResult queryResult, ShardResultTransformer transformer) throws IOException {
    if (needDocset) {
      queryResult.setDocSet(docSet);
    }
    return transformer.transform(commands);
  }

}
