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
 * Benchmarking Lucene By Tasks
 * <p>
 * This package provides "task based" performance benchmarking of Lucene.
 * One can use the predefined benchmarks, or create new ones.
 * </p>
 * <p>
 * Contained packages:
 * </p>
 * 
 * <table border=1 cellpadding=4 summary="table of benchmark packages">
 *  <tr>
 *    <td><b>Package</b></td>
 *    <td><b>Description</b></td>
 *  </tr>
 *  <tr>
 *    <td><a href="stats/package-summary.html">stats</a></td>
 *    <td>Statistics maintained when running benchmark tasks.</td>
 *  </tr>
 *  <tr>
 *    <td><a href="tasks/package-summary.html">tasks</a></td>
 *    <td>Benchmark tasks.</td>
 *  </tr>
 *  <tr>
 *    <td><a href="feeds/package-summary.html">feeds</a></td>
 *    <td>Sources for benchmark inputs: documents and queries.</td>
 *  </tr>
 *  <tr>
 *    <td><a href="utils/package-summary.html">utils</a></td>
 *    <td>Utilities used for the benchmark, and for the reports.</td>
 *  </tr>
 *  <tr>
 *    <td><a href="programmatic/package-summary.html">programmatic</a></td>
 *    <td>Sample performance test written programmatically.</td>
 *  </tr>
 * </table>
 * 
 * <h2>Table Of Contents</h2>
 *     <ol>
 *         <li><a href="#concept">Benchmarking By Tasks</a></li>
 *         <li><a href="#usage">How to use</a></li>
 *         <li><a href="#algorithm">Benchmark "algorithm"</a></li>
 *         <li><a href="#tasks">Supported tasks/commands</a></li>
 *         <li><a href="#properties">Benchmark properties</a></li>
 *         <li><a href="#example">Example input algorithm and the result benchmark
 *                     report.</a></li>
 *         <li><a href="#recsCounting">Results record counting clarified</a></li>
 *     </ol>
 * <a name="concept"></a>
 * <h2>Benchmarking By Tasks</h2>
 * <p>
 * Benchmark Lucene using task primitives.
 * </p>
 * 
 * <p>
 * A benchmark is composed of some predefined tasks, allowing for creating an
 * index, adding documents,
 * optimizing, searching, generating reports, and more. A benchmark run takes an
 * "algorithm" file
 * that contains a description of the sequence of tasks making up the run, and some
 * properties defining a few
 * additional characteristics of the benchmark run.
 * </p>
 * 
 * <a name="usage"></a>
 * <h2>How to use</h2>
 * <p>
 * Easiest way to run a benchmarks is using the predefined ant task:
 * <ul>
 *  <li>ant run-task
 *      <br>- would run the <code>micro-standard.alg</code> "algorithm".
 *  </li>
 *  <li>ant run-task -Dtask.alg=conf/compound-penalty.alg
 *      <br>- would run the <code>compound-penalty.alg</code> "algorithm".
 *  </li>
 *  <li>ant run-task -Dtask.alg=[full-path-to-your-alg-file]
 *      <br>- would run <code>your perf test</code> "algorithm".
 *  </li>
 *  <li>java org.apache.lucene.benchmark.byTask.programmatic.Sample
 *      <br>- would run a performance test programmatically - without using an alg
 *      file. This is less readable, and less convenient, but possible.
 *  </li>
 * </ul>
 * 
 * <p>
 * You may find existing tasks sufficient for defining the benchmark <i>you</i>
 * need, otherwise, you can extend the framework to meet your needs, as explained
 * herein.
 * </p>
 * 
 * <p>
 * Each benchmark run has a DocMaker and a QueryMaker. These two should usually
 * match, so that "meaningful" queries are used for a certain collection.
 * Properties set at the header of the alg file define which "makers" should be
 * used. You can also specify your own makers, extending DocMaker and implementing
 * QueryMaker.
 *   <blockquote>
 *     <b>Note:</b> since 2.9, DocMaker is a concrete class which accepts a 
 *     ContentSource. In most cases, you can use the DocMaker class to create 
 *     Documents, while providing your own ContentSource implementation. For 
 *     example, the current Benchmark package includes ContentSource 
 *     implementations for TREC, Enwiki and Reuters collections, as well as 
 *     others like LineDocSource which reads a 'line' file produced by 
 *     WriteLineDocTask.
 *   </blockquote>
 * 
 * <p>
 * Benchmark .alg file contains the benchmark "algorithm". The syntax is described
 * below. Within the algorithm, you can specify groups of commands, assign them
 * names, specify commands that should be repeated,
 * do commands in serial or in parallel,
 * and also control the speed of "firing" the commands.
 * </p>
 * 
 * <p>
 * This allows, for instance, to specify
 * that an index should be opened for update,
 * documents should be added to it one by one but not faster than 20 docs a minute,
 * and, in parallel with this,
 * some N queries should be searched against that index,
 * again, no more than 2 queries a second.
 * You can have the searches all share an index reader,
 * or have them each open its own reader and close it afterwords.
 * </p>
 * 
 * <p>
 * If the commands available for use in the algorithm do not meet your needs,
 * you can add commands by adding a new task under
 * org.apache.lucene.benchmark.byTask.tasks -
 * you should extend the PerfTask abstract class.
 * Make sure that your new task class name is suffixed by Task.
 * Assume you added the class "WonderfulTask" - doing so also enables the
 * command "Wonderful" to be used in the algorithm.
 * </p>
 * 
 * <p>
 * <u>External classes</u>: It is sometimes useful to invoke the benchmark
 * package with your external alg file that configures the use of your own
 * doc/query maker and or html parser. You can work this out without
 * modifying the benchmark package code, by passing your class path
 * with the benchmark.ext.classpath property:
 * <ul>
 *   <li>ant run-task -Dtask.alg=[full-path-to-your-alg-file]
 *       <span style="color: #FF0000">-Dbenchmark.ext.classpath=/mydir/classes
 *       </span> -Dtask.mem=512M</li>
 * </ul>
 * <p>
 * <u>External tasks</u>: When writing your own tasks under a package other than 
 * <b>org.apache.lucene.benchmark.byTask.tasks</b> specify that package thru the
 * <span style="color: #FF0000">alt.tasks.packages</span> property.
 * 
 * <a name="algorithm"></a>
 * <h2>Benchmark "algorithm"</h2>
 * 
 * <p>
 * The following is an informal description of the supported syntax.
 * </p>
 * 
 * <ol>
 *  <li>
 *  <b>Measuring</b>: When a command is executed, statistics for the elapsed
 *  execution time and memory consumption are collected.
 *  At any time, those statistics can be printed, using one of the
 *  available ReportTasks.
 *  </li>
 *  <li>
 *  <b>Comments</b> start with '<span style="color: #FF0066">#</span>'.
 *  </li>
 *  <li>
 *  <b>Serial</b> sequences are enclosed within '<span style="color: #FF0066">{ }</span>'.
 *  </li>
 *  <li>
 *  <b>Parallel</b> sequences are enclosed within
 *  '<span style="color: #FF0066">[ ]</span>'
 *  </li>
 *  <li>
 *  <b>Sequence naming:</b> To name a sequence, put
 *  '<span style="color: #FF0066">"name"</span>' just after
 *  '<span style="color: #FF0066">{</span>' or '<span style="color: #FF0066">[</span>'.
 *  <br>Example - <span style="color: #FF0066">{ "ManyAdds" AddDoc } : 1000000</span> -
 *  would
 *  name the sequence of 1M add docs "ManyAdds", and this name would later appear
 *  in statistic reports.
 *  If you don't specify a name for a sequence, it is given one: you can see it as
 *  the  algorithm is printed just before benchmark execution starts.
 *  </li>
 *  <li>
 *  <b>Repeating</b>:
 *  To repeat sequence tasks N times, add '<span style="color: #FF0066">: N</span>' just
 *  after the
 *  sequence closing tag - '<span style="color: #FF0066">}</span>' or
 *  '<span style="color: #FF0066">]</span>' or '<span style="color: #FF0066">&gt;</span>'.
 *  <br>Example -  <span style="color: #FF0066">[ AddDoc ] : 4</span>  - would do 4 addDoc
 *  in parallel, spawning 4 threads at once.
 *  <br>Example -  <span style="color: #FF0066">[ AddDoc AddDoc ] : 4</span>  - would do
 *  8 addDoc in parallel, spawning 8 threads at once.
 *  <br>Example -  <span style="color: #FF0066">{ AddDoc } : 30</span> - would do addDoc
 *  30 times in a row.
 *  <br>Example -  <span style="color: #FF0066">{ AddDoc AddDoc } : 30</span> - would do
 *  addDoc 60 times in a row.
 *  <br><b>Exhaustive repeating</b>: use <span style="color: #FF0066">*</span> instead of
 *  a number to repeat exhaustively.
 *  This is sometimes useful, for adding as many files as a doc maker can create,
 *  without iterating over the same file again, especially when the exact
 *  number of documents is not known in advance. For instance, TREC files extracted
 *  from a zip file. Note: when using this, you must also set
 *  <span style="color: #FF0066">content.source.forever</span> to false.
 *  <br>Example -  <span style="color: #FF0066">{ AddDoc } : *</span>  - would add docs
 *  until the doc maker is "exhausted".
 *  </li>
 *  <li>
 *  <b>Command parameter</b>: a command can optionally take a single parameter.
 *  If the certain command does not support a parameter, or if the parameter is of
 *  the wrong type,
 *  reading the algorithm will fail with an exception and the test would not start.
 *  Currently the following tasks take optional parameters:
 *  <ul>
 *    <li><b>AddDoc</b> takes a numeric parameter, indicating the required size of
 *        added document. Note: if the DocMaker implementation used in the test
 *        does not support makeDoc(size), an exception would be thrown and the test
 *        would fail.
 *    </li>
 *    <li><b>DeleteDoc</b> takes numeric parameter, indicating the docid to be
 *        deleted. The latter is not very useful for loops, since the docid is
 *        fixed, so for deletion in loops it is better to use the
 *        <code>doc.delete.step</code> property.
 *    </li>
 *    <li><b>SetProp</b> takes a <code>name,value</code> mandatory param,
 *        ',' used as a separator.
 *    </li>
 *    <li><b>SearchTravRetTask</b> and <b>SearchTravTask</b> take a numeric
 *               parameter, indicating the required traversal size.
 *    </li>
 *    <li><b>SearchTravRetLoadFieldSelectorTask</b> takes a string
 *               parameter: a comma separated list of Fields to load.
 *    </li>
 *    <li><b>SearchTravRetHighlighterTask</b> takes a string
 *               parameter: a comma separated list of parameters to define highlighting.  See that
 *      tasks javadocs for more information
 *    </li>
 *  </ul>
 *  <br>Example - <span style="color: #FF0066">AddDoc(2000)</span> - would add a document
 *  of size 2000 (~bytes).
 *  <br>See conf/task-sample.alg for how this can be used, for instance, to check
 *  which is faster, adding
 *  many smaller documents, or few larger documents.
 *  Next candidates for supporting a parameter may be the Search tasks,
 *  for controlling the query size.
 *  </li>
 *  <li>
 *  <b>Statistic recording elimination</b>: - a sequence can also end with
 *  '<span style="color: #FF0066">&gt;</span>',
 *  in which case child tasks would not store their statistics.
 *  This can be useful to avoid exploding stats data, for adding say 1M docs.
 *  <br>Example - <span style="color: #FF0066">{ "ManyAdds" AddDoc &gt; : 1000000</span> -
 *  would add million docs, measure that total, but not save stats for each addDoc.
 *  <br>Notice that the granularity of System.currentTimeMillis() (which is used
 *  here) is system dependant,
 *  and in some systems an operation that takes 5 ms to complete may show 0 ms
 *  latency time in performance measurements.
 *  Therefore it is sometimes more accurate to look at the elapsed time of a larger
 *  sequence, as demonstrated here.
 *  </li>
 *  <li>
 *  <b>Rate</b>:
 *  To set a rate (ops/sec or ops/min) for a sequence, add
 *  '<span style="color: #FF0066">: N : R</span>' just after sequence closing tag.
 *  This would specify repetition of N with rate of R operations/sec.
 *  Use '<span style="color: #FF0066">R/sec</span>' or
 *  '<span style="color: #FF0066">R/min</span>'
 *  to explicitly specify that the rate is per second or per minute.
 *  The default is per second,
 *  <br>Example -  <span style="color: #FF0066">[ AddDoc ] : 400 : 3</span> - would do 400
 *  addDoc in parallel, starting up to 3 threads per second.
 *  <br>Example -  <span style="color: #FF0066">{ AddDoc } : 100 : 200/min</span> - would
 *  do 100 addDoc serially,
 *  waiting before starting next add, if otherwise rate would exceed 200 adds/min.
 *  </li>
 *  <li>
 *  <b>Disable Counting</b>: Each task executed contributes to the records count.
 *  This count is reflected in reports under recs/s and under recsPerRun.
 *  Most tasks count 1, some count 0, and some count more.
 *  (See <a href="#recsCounting">Results record counting clarified</a> for more details.)
 *  It is possible to disable counting for a task by preceding it with <span style="color: #FF0066">-</span>.
 *  <br>Example -  <span style="color: #FF0066"> -CreateIndex </span> - would count 0 while
 *  the default behavior for CreateIndex is to count 1.
 *  </li>
 *  <li>
 *  <b>Command names</b>: Each class "AnyNameTask" in the
 *  package org.apache.lucene.benchmark.byTask.tasks,
 *  that extends PerfTask, is supported as command "AnyName" that can be
 *  used in the benchmark "algorithm" description.
 *  This allows to add new commands by just adding such classes.
 *  </li>
 * </ol>
 * 
 * 
 * <a name="tasks"></a>
 * <h2>Supported tasks/commands</h2>
 * 
 * <p>
 * Existing tasks can be divided into a few groups:
 * regular index/search work tasks, report tasks, and control tasks.
 * </p>
 * 
 * <ol>
 * 
 *  <li>
 *  <b>Report tasks</b>: There are a few Report commands for generating reports.
 *  Only task runs that were completed are reported.
 *  (The 'Report tasks' themselves are not measured and not reported.)
 *  <ul>
 *              <li>
 *             <span style="color: #FF0066">RepAll</span> - all (completed) task runs.
 *             </li>
 *             <li>
 *             <span style="color: #FF0066">RepSumByName</span> - all statistics,
 *             aggregated by name. So, if AddDoc was executed 2000 times,
 *             only 1 report line would be created for it, aggregating all those
 *             2000 statistic records.
 *             </li>
 *             <li>
 *             <span style="color: #FF0066">RepSelectByPref &nbsp; prefixWord</span> - all
 *             records for tasks whose name start with
 *             <span style="color: #FF0066">prefixWord</span>.
 *             </li>
 *             <li>
 *             <span style="color: #FF0066">RepSumByPref &nbsp; prefixWord</span> - all
 *             records for tasks whose name start with
 *             <span style="color: #FF0066">prefixWord</span>,
 *             aggregated by their full task name.
 *             </li>
 *             <li>
 *             <span style="color: #FF0066">RepSumByNameRound</span> - all statistics,
 *             aggregated by name and by <span style="color: #FF0066">Round</span>.
 *             So, if AddDoc was executed 2000 times in each of 3
 *             <span style="color: #FF0066">rounds</span>, 3 report lines would be
 *             created for it,
 *             aggregating all those 2000 statistic records in each round.
 *             See more about rounds in the <span style="color: #FF0066">NewRound</span>
 *             command description below.
 *             </li>
 *             <li>
 *             <span style="color: #FF0066">RepSumByPrefRound &nbsp; prefixWord</span> -
 *             similar to <span style="color: #FF0066">RepSumByNameRound</span>,
 *             just that only tasks whose name starts with
 *             <span style="color: #FF0066">prefixWord</span> are included.
 *             </li>
 *  </ul>
 *  If needed, additional reports can be added by extending the abstract class
 *  ReportTask, and by
 *  manipulating the statistics data in Points and TaskStats.
 *  </li>
 * 
 *  <li><b>Control tasks</b>: Few of the tasks control the benchmark algorithm
 *  all over:
 *  <ul>
 *      <li>
 *      <span style="color: #FF0066">ClearStats</span> - clears the entire statistics.
 *      Further reports would only include task runs that would start after this
 *      call.
 *      </li>
 *      <li>
 *      <span style="color: #FF0066">NewRound</span> - virtually start a new round of
 *      performance test.
 *      Although this command can be placed anywhere, it mostly makes sense at
 *      the end of an outermost sequence.
 *      <br>This increments a global "round counter". All task runs that
 *      would start now would
 *      record the new, updated round counter as their round number.
 *      This would appear in reports.
 *      In particular, see <span style="color: #FF0066">RepSumByNameRound</span> above.
 *      <br>An additional effect of NewRound, is that numeric and boolean
 *      properties defined (at the head
 *      of the .alg file) as a sequence of values, e.g. <span style="color: #FF0066">
 *      merge.factor=mrg:10:100:10:100</span> would
 *      increment (cyclic) to the next value.
 *      Note: this would also be reflected in the reports, in this case under a
 *      column that would be named "mrg".
 *      </li>
 *      <li>
 *      <span style="color: #FF0066">ResetInputs</span> - DocMaker and the
 *      various QueryMakers
 *      would reset their counters to start.
 *      The way these Maker interfaces work, each call for makeDocument()
 *      or makeQuery() creates the next document or query
 *      that it "knows" to create.
 *      If that pool is "exhausted", the "maker" start over again.
 *      The ResetInputs command
 *      therefore allows to make the rounds comparable.
 *      It is therefore useful to invoke ResetInputs together with NewRound.
 *      </li>
 *      <li>
 *      <span style="color: #FF0066">ResetSystemErase</span> - reset all index
 *      and input data and call gc.
 *      Does NOT reset statistics. This contains ResetInputs.
 *      All writers/readers are nullified, deleted, closed.
 *      Index is erased.
 *      Directory is erased.
 *      You would have to call CreateIndex once this was called...
 *      </li>
 *      <li>
 *      <span style="color: #FF0066">ResetSystemSoft</span> -  reset all
 *      index and input data and call gc.
 *      Does NOT reset statistics. This contains ResetInputs.
 *      All writers/readers are nullified, closed.
 *      Index is NOT erased.
 *      Directory is NOT erased.
 *      This is useful for testing performance on an existing index,
 *      for instance if the construction of a large index
 *      took a very long time and now you would to test
 *      its search or update performance.
 *      </li>
 *  </ul>
 *  </li>
 * 
 *  <li>
 *  Other existing tasks are quite straightforward and would
 *  just be briefly described here.
 *  <ul>
 *      <li>
 *      <span style="color: #FF0066">CreateIndex</span> and
 *      <span style="color: #FF0066">OpenIndex</span> both leave the
 *      index open for later update operations.
 *      <span style="color: #FF0066">CloseIndex</span> would close it.
 *      <li>
 *      <span style="color: #FF0066">OpenReader</span>, similarly, would
 *      leave an index reader open for later search operations.
 *      But this have further semantics.
 *      If a Read operation is performed, and an open reader exists,
 *      it would be used.
 *      Otherwise, the read operation would open its own reader
 *      and close it when the read operation is done.
 *      This allows testing various scenarios - sharing a reader,
 *      searching with "cold" reader, with "warmed" reader, etc.
 *      The read operations affected by this are:
 *      <span style="color: #FF0066">Warm</span>,
 *      <span style="color: #FF0066">Search</span>,
 *      <span style="color: #FF0066">SearchTrav</span> (search and traverse),
 *      and <span style="color: #FF0066">SearchTravRet</span> (search
 *      and traverse and retrieve).
 *      Notice that each of the 3 search task types maintains
 *      its own queryMaker instance.
 *    <li>
 *    <span style="color: #FF0066">CommitIndex</span> and 
 *    <span style="color: #FF0066">ForceMerge</span> can be used to commit
 *    changes to the index then merge the index segments. The integer
 *    parameter specifies how many segments to merge down to (default
 *    1).
 *    <li>
 *    <span style="color: #FF0066">WriteLineDoc</span> prepares a 'line'
 *    file where each line holds a document with <i>title</i>, 
 *    <i>date</i> and <i>body</i> elements, separated by [TAB].
 *    A line file is useful if one wants to measure pure indexing
 *    performance, without the overhead of parsing the data.<br>
 *    You can use LineDocSource as a ContentSource over a 'line'
 *    file.
 *    <li>
 *    <span style="color: #FF0066">ConsumeContentSource</span> consumes
 *    a ContentSource. Useful for e.g. testing a ContentSource
 *    performance, without the overhead of preparing a Document
 *    out of it.
 *  </ul>
 *  </li>
 *  </ol>
 * 
 * <a name="properties"></a>
 * <h2>Benchmark properties</h2>
 * 
 * <p>
 * Properties are read from the header of the .alg file, and
 * define several parameters of the performance test.
 * As mentioned above for the <span style="color: #FF0066">NewRound</span> task,
 * numeric and boolean properties that are defined as a sequence
 * of values, e.g. <span style="color: #FF0066">merge.factor=mrg:10:100:10:100</span>
 * would increment (cyclic) to the next value,
 * when NewRound is called, and would also
 * appear as a named column in the reports (column
 * name would be "mrg" in this example).
 * </p>
 * 
 * <p>
 * Some of the currently defined properties are:
 * </p>
 * 
 * <ol>
 *     <li>
 *     <span style="color: #FF0066">analyzer</span> - full
 *     class name for the analyzer to use.
 *     Same analyzer would be used in the entire test.
 *     </li>
 * 
 *     <li>
 *     <span style="color: #FF0066">directory</span> - valid values are
 *     This tells which directory to use for the performance test.
 *     </li>
 * 
 *     <li>
 *     <b>Index work parameters</b>:
 *     Multi int/boolean values would be iterated with calls to NewRound.
 *     There would be also added as columns in the reports, first string in the
 *     sequence is the column name.
 *     (Make sure it is no shorter than any value in the sequence).
 *     <ul>
 *         <li><span style="color: #FF0066">max.buffered</span>
 *         <br>Example: max.buffered=buf:10:10:100:100 -
 *         this would define using maxBufferedDocs of 10 in iterations 0 and 1,
 *         and 100 in iterations 2 and 3.
 *         </li>
 *         <li>
 *         <span style="color: #FF0066">merge.factor</span> - which
 *         merge factor to use.
 *         </li>
 *         <li>
 *         <span style="color: #FF0066">compound</span> - whether the index is
 *         using the compound format or not. Valid values are "true" and "false".
 *         </li>
 *     </ul>
 * </ol>
 * 
 * <p>
 * Here is a list of currently defined properties:
 * </p>
 * <ol>
 * 
 *   <li><b>Root directory for data and indexes:</b>
 *     <ul><li>work.dir (default is System property "benchmark.work.dir" or "work".)
 *     </li></ul>
 *   </li>
 * 
 *   <li><b>Docs and queries creation:</b>
 *     <ul><li>analyzer
 *     </li><li>doc.maker
 *     </li><li>content.source.forever
 *     </li><li>html.parser
 *     </li><li>doc.stored
 *     </li><li>doc.tokenized
 *     </li><li>doc.term.vector
 *     </li><li>doc.term.vector.positions
 *     </li><li>doc.term.vector.offsets
 *     </li><li>doc.store.body.bytes
 *     </li><li>docs.dir
 *     </li><li>query.maker
 *     </li><li>file.query.maker.file
 *     </li><li>file.query.maker.default.field
 *     </li><li>search.num.hits
 *     </li></ul>
 *   </li>
 * 
 *   <li><b>Logging</b>:
 *     <ul><li>log.step
 *   </li><li>log.step.[class name]Task ie log.step.DeleteDoc (e.g. log.step.Wonderful for the WonderfulTask example above).
 *     </li><li>log.queries
 *     </li><li>task.max.depth.log
 *     </li></ul>
 *   </li>
 * 
 *   <li><b>Index writing</b>:
 *     <ul><li>compound
 *     </li><li>merge.factor
 *     </li><li>max.buffered
 *     </li><li>directory
 *     </li><li>ram.flush.mb
 *     </li><li>codec.postingsFormat (eg Direct) Note: no codec should be specified through default.codec
 *     </li></ul>
 *   </li>
 * 
 *   <li><b>Doc deletion</b>:
 *     <ul><li>doc.delete.step
 *     </li></ul>
 *   </li>
 * 
 *   <li><b>Spatial</b>: Numerous; see spatial.alg
 *   </li>
 *   
 *   <li><b>Task alternative packages</b>:
 *     <ul><li>alt.tasks.packages
 *       - comma separated list of additional packages where tasks classes will be looked for
 *       when not found in the default package (that of PerfTask).  If the same task class 
 *       appears in more than one package, the package indicated first in this list will be used.
 *     </li></ul> 
 *   </li>
 * 
 * </ol>
 * 
 * <p>
 * For sample use of these properties see the *.alg files under conf.
 * </p>
 * 
 * <a name="example"></a>
 * <h2>Example input algorithm and the result benchmark report</h2>
 * <p>
 * The following example is in conf/sample.alg:
 * <pre>
 * <span style="color: #003333"># --------------------------------------------------------
 * #
 * # Sample: what is the effect of doc size on indexing time?
 * #
 * # There are two parts in this test:
 * # - PopulateShort adds 2N documents of length  L
 * # - PopulateLong  adds  N documents of length 2L
 * # Which one would be faster?
 * # The comparison is done twice.
 * #
 * # --------------------------------------------------------
 * </span>
 * <span style="color: #990066"># -------------------------------------------------------------------------------------
 * # multi val params are iterated by NewRound's, added to reports, start with column name.
 * merge.factor=mrg:10:20
 * max.buffered=buf:100:1000
 * compound=true
 * 
 * analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
 * directory=FSDirectory
 * 
 * doc.stored=true
 * doc.tokenized=true
 * doc.term.vector=false
 * doc.add.log.step=500
 * 
 * docs.dir=reuters-out
 * 
 * doc.maker=org.apache.lucene.benchmark.byTask.feeds.SimpleDocMaker
 * 
 * query.maker=org.apache.lucene.benchmark.byTask.feeds.SimpleQueryMaker
 * 
 * # task at this depth or less would print when they start
 * task.max.depth.log=2
 * 
 * log.queries=false
 * # -------------------------------------------------------------------------------------</span>
 * <span style="color: #3300FF">{
 * 
 *     { "PopulateShort"
 *         CreateIndex
 *         { AddDoc(4000) &gt; : 20000
 *         Optimize
 *         CloseIndex
 *     &gt;
 * 
 *     ResetSystemErase
 * 
 *     { "PopulateLong"
 *         CreateIndex
 *         { AddDoc(8000) &gt; : 10000
 *         Optimize
 *         CloseIndex
 *     &gt;
 * 
 *     ResetSystemErase
 * 
 *     NewRound
 * 
 * } : 2
 * 
 * RepSumByName
 * RepSelectByPref Populate
 * </span>
 * </pre>
 * 
 * <p>
 * The command line for running this sample:
 * <br><code>ant run-task -Dtask.alg=conf/sample.alg</code>
 * </p>
 * 
 * <p>
 * The output report from running this test contains the following:
 * <pre>
 * Operation     round mrg  buf   runCnt   recsPerRun        rec/s  elapsedSec    avgUsedMem    avgTotalMem
 * PopulateShort     0  10  100        1        20003        119.6      167.26    12,959,120     14,241,792
 * PopulateLong -  - 0  10  100 -  -   1 -  -   10003 -  -  - 74.3 -  - 134.57 -  17,085,208 -   20,635,648
 * PopulateShort     1  20 1000        1        20003        143.5      139.39    63,982,040     94,756,864
 * PopulateLong -  - 1  20 1000 -  -   1 -  -   10003 -  -  - 77.0 -  - 129.92 -  87,309,608 -  100,831,232
 * </pre>
 * 
 * <a name="recsCounting"></a>
 * <h2>Results record counting clarified</h2>
 * <p>
 * Two columns in the results table indicate records counts: records-per-run and
 * records-per-second. What does it mean?
 * </p><p>
 * Almost every task gets 1 in this count just for being executed.
 * Task sequences aggregate the counts of their child tasks,
 * plus their own count of 1.
 * So, a task sequence containing 5 other task sequences, each running a single
 * other task 10 times, would have a count of 1 + 5 * (1 + 10) = 56.
 * </p><p>
 * The traverse and retrieve tasks "count" more: a traverse task
 * would add 1 for each traversed result (hit), and a retrieve task would
 * additionally add 1 for each retrieved doc. So, regular Search would
 * count 1, SearchTrav that traverses 10 hits would count 11, and a
 * SearchTravRet task that retrieves (and traverses) 10, would count 21.
 * </p><p>
 * Confusing? this might help: always examine the <code>elapsedSec</code> column,
 * and always compare "apples to apples", .i.e. it is interesting to check how the
 * <code>rec/s</code> changed for the same task (or sequence) between two
 * different runs, but it is not very useful to know how the <code>rec/s</code>
 * differs between <code>Search</code> and <code>SearchTrav</code> tasks. For
 * the latter, <code>elapsedSec</code> would bring more insight.
 * </p>
 */
package org.apache.lucene.benchmark.byTask;
