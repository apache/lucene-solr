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
 * <h1>Files replication framework</h1>
 *
 * The
 * <a href="Replicator.html">Replicator</a> allows replicating files between a server and client(s). Producers publish
 * <a href="Revision.html">revisions</a> and consumers update to the latest revision available.
 * <a href="ReplicationClient.html">ReplicationClient</a> is a helper utility for performing the update operation. It can
 * be invoked either
 * <a href="ReplicationClient.html#updateNow()">manually</a> or periodically by
 * <a href="ReplicationClient.html#startUpdateThread(long,+java.lang.String)">starting an update thread</a>.
 * <a href="http/HttpReplicator.html">HttpReplicator</a> can be used to replicate revisions by consumers that reside on
 * a different node than the producer.
 *
 * <p>
 * The replication framework supports replicating any type of files, with built-in support for a single search index as
 * well as an index and taxonomy pair. For a single index, the application should publish an
 * <a href="IndexRevision.html">IndexRevision</a> and set
 * <a href="IndexReplicationHandler.html">IndexReplicationHandler</a> on the client. For an index and taxonomy pair, the
 * application should publish an <a href="IndexAndTaxonomyRevision.html">IndexAndTaxonomyRevision</a> and set 
 * <a href="IndexAndTaxonomyReplicationHandler.html">IndexAndTaxonomyReplicationHandler</a> on the client.
 *
 * <p>
 * When the replication client detects that there is a newer revision available, it copies the files of the revision and
 * then invokes the handler to complete the operation (e.g. copy the files to the index directory, fsync them, reopen an
 * index reader etc.). By default, only files that do not exist in the handler's
 * <a href="ReplicationClient.ReplicationHandler.html#currentRevisionFiles()">current revision files</a> are copied,
 * however this can be overridden by extending the client.
 *
 * <p>
 * An example usage of the Replicator:
 * 
 * <pre class="prettyprint lang-java">
 * // ++++++++++++++ SERVER SIDE ++++++++++++++ // 
 * IndexWriter publishWriter; // the writer used for indexing
 * Replicator replicator = new LocalReplicator();
 * replicator.publish(new IndexRevision(publishWriter));
 *
 * // ++++++++++++++ CLIENT SIDE ++++++++++++++ // 
 * // either LocalReplictor, or HttpReplicator if client and server are on different nodes
 * Replicator replicator;
 * 
 * // callback invoked after handler finished handling the revision and e.g. can reopen the reader.
 * Callable&lt;Boolean&gt; callback = null; // can also be null if no callback is needed
 * ReplicationHandler handler = new IndexReplicationHandler(indexDir, callback);
 * SourceDirectoryFactory factory = new PerSessionDirectoryFactory(workDir);
 * ReplicationClient client = new ReplicationClient(replicator, handler, factory);
 *
 * // invoke client manually
 * client.updateNow();
 * 
 * // or, periodically
 * client.startUpdateThread(100); // check for update every 100 milliseconds
 * </pre>
 */
package org.apache.lucene.replicator;