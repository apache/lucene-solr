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

package org.apache.solr.update;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.update.TransactionLog.LogReader;
import org.junit.Test;

public class TransactionLogTest extends SolrTestCase {

  @Test
  public void testBigLastAddSize() {
    String tlogFileName = String.format(Locale.ROOT, UpdateLog.LOG_FILENAME_PATTERN, UpdateLog.TLOG_NAME,
        Long.MAX_VALUE);
    Path path = createTempDir();
    File logFile = new File(path.toFile(), tlogFileName);
    try (TransactionLog transactionLog = new TransactionLog(logFile, new ArrayList<>())) {
      transactionLog.lastAddSize = 2000000000;
      AddUpdateCommand updateCommand = new AddUpdateCommand(null);
      updateCommand.solrDoc = new SolrInputDocument();
      transactionLog.write(updateCommand);
    }
  }

  @Test
  public void testUUID() throws IOException, InterruptedException {
    String tlogFileName = String.format(Locale.ROOT, UpdateLog.LOG_FILENAME_PATTERN, UpdateLog.TLOG_NAME,
        Long.MAX_VALUE);
    Path path = createTempDir();
    File logFile = new File(path.toFile(), tlogFileName);
    UUID uuid = UUID.randomUUID();
    try (TransactionLog tlog = new TransactionLog(logFile, new ArrayList<>())) {
      tlog.deleteOnClose = false;
      AddUpdateCommand updateCommand = new AddUpdateCommand(null);

      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("uuid", uuid);
      updateCommand.solrDoc = doc;

      tlog.write(updateCommand);
    }

    try (TransactionLog tlog = new TransactionLog(logFile, new ArrayList<>(), true)) {
      LogReader reader = tlog.getReader(0);
      Object entry = reader.next();
      assertNotNull(entry);
      SolrInputDocument doc = (SolrInputDocument) ((List<?>) entry).get(2);
      assertEquals(uuid, (UUID) doc.getFieldValue("uuid"));
    }
  }
}
