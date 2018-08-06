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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Locale;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

public class TransactionLogTest extends LuceneTestCase {

  @Test
  public void testBigLastAddSize() {
    String tlogFileName = String.format(Locale.ROOT, UpdateLog.LOG_FILENAME_PATTERN, UpdateLog.TLOG_NAME, Long.MAX_VALUE);
    Path path = createTempDir();
    File logFile = new File(path.toFile(), tlogFileName);
    try (TransactionLog transactionLog = new TransactionLog(logFile, new ArrayList<>())) {
      transactionLog.lastAddSize = 2000000000;
      AddUpdateCommand updateCommand = new AddUpdateCommand(null);
      updateCommand.solrDoc = new SolrInputDocument();
      transactionLog.write(updateCommand);
    }
  }

}
