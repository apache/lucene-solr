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

package org.apache.solr.schema;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.ZkIndexSchemaReader.SchemaWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class SchemaWatcherTest {

  private ZkIndexSchemaReader mockSchemaReader;
  private SchemaWatcher schemaWatcher;

  @Before
  public void setUp() throws Exception {
    SolrTestCaseJ4.assumeWorkingMockito();
    
    mockSchemaReader = mock(ZkIndexSchemaReader.class);
    schemaWatcher = new SchemaWatcher(mockSchemaReader);
  }

  @Test
  public void testProcess() throws Exception {
    schemaWatcher.process(new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, "/test"));
    verify(mockSchemaReader).updateSchema(schemaWatcher, -1);
  }

  @Test
  public void testDiscardReaderReference() throws Exception {
    schemaWatcher.discardReaderReference();

    schemaWatcher.process(new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, "/test"));
    // after discardReaderReference, SchemaWatcher should no longer hold a ref to the reader
    verifyZeroInteractions(mockSchemaReader);
  }
}
