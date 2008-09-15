package com.sleepycat.db;

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

import com.sleepycat.db.internal.Db;
import com.sleepycat.db.internal.DbTxn;


/**
 * This class is a hack to workaround the need to rewrite the entire
 * org.apache.lucene.store.db package after Sleepycat radically changed its
 * Java API from version 4.2.52 to version 4.3.21.
 * 
 * The code below extracts the package-accessible internal handle instances
 * that were the entrypoint objects in the pre-4.3 Java API and that wrap the
 * actual Berkeley DB C objects via SWIG.
 *
 */

public class DbHandleExtractor {

    private DbHandleExtractor()
    {
    }

    static public Db getDb(Database database)
    {
        return database.db;
    }

    static public DbTxn getDbTxn(Transaction transaction)
    {
        return transaction.txn;
    }
}
