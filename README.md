<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->
![Star Burst](/starburst.jpeg) 

# The Star Burst Upgrade

The themes of the Star Burst Upgrade are: Communication, Resource Usage, and Performance + Scale.

- A full on move from HTTP/1.1 to HTTP/2 resulting in resource usage and stability improvements across the board.
- A new high performance and more feature packed low level HttpClient capable of non blocking IO as well as HTTP/1.1 and HTTP/2.
- A brand new and improved HTTP/2 capable SolrJ SolrClient, offering asynchronous requests and performance improvements to the whole SolrJ client family.
- All external and internal communication takes advantage of the above.
- Configurable, dynamic and efficient resource usage throttling without massive thread waste or distributed deadlock. 
- Communication efficiency improvements and hardening across the board.
- Tuned and improved resource usage across the board.


# Dev Notes

Things are still rapidly changing. Don't count on much until more starts to stabilize.

Tests now need to have test specific timeouts if over 45 seconds might be required.

# Testing State

always run tests with -Dtests.badapples=false

ant -Dtests.badapples=false test should be passing and should remain passing

