/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

require
(
  [
    'lib/order!lib/console',
    'lib/order!jquery',
    'lib/order!lib/jquery.autogrow',
    'lib/order!lib/jquery.cookie',
    'lib/order!lib/jquery.form',
    'lib/order!lib/jquery.jstree',
    'lib/order!lib/jquery.sammy',
    'lib/order!lib/jquery.sparkline',
    'lib/order!lib/jquery.timeago',
    'lib/order!lib/jquery.blockUI',
    'lib/order!lib/highlight',
    'lib/order!lib/linker',
    'lib/order!lib/ZeroClipboard',
    'lib/order!lib/d3',
    'lib/order!lib/chosen',
    'lib/order!scripts/app',

    'lib/order!scripts/analysis',
    'lib/order!scripts/cloud',
    'lib/order!scripts/cores',
    'lib/order!scripts/dataimport',
    'lib/order!scripts/dashboard',
    'lib/order!scripts/file',
    'lib/order!scripts/index',
    'lib/order!scripts/java-properties',
    'lib/order!scripts/logging',
    'lib/order!scripts/ping',
    'lib/order!scripts/plugins',
    'lib/order!scripts/query',
    'lib/order!scripts/replication',
    'lib/order!scripts/schema-browser',
    'lib/order!scripts/threads'
  ],
  function( $ )
  {
    app.run();
  }
);