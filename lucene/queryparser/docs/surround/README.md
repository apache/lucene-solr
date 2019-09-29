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

# Surround:

Surround consists of operators (uppercase/lowercase):

```AND/OR/NOT/nW/nN/()``` as infix and
```AND/OR/nW/nN```       as prefix.

Distance operators W and N have default n=1, max 99.
Implemented as SpanQuery with slop = (n - 1).
An example prefix form is:

```20n(aa*, bb*, cc*)```

## Description of Surround:

The name Surround was chosen because of this prefix form
and because it uses the newly introduced span queries
to implement the proximity operators.
The names of the operators and the prefix and suffix
forms have been borrowed from various other query
languages described on the internet.


Query terms from the Lucene standard query parser:

`field:termtext`

`^ boost`

`* internal and suffix truncation`

`? one character`


###Some examples:

`aa`

`aa and bb`

`aa and bb or cc`        same effect as:  `(aa and bb) or cc`

`aa NOT bb NOT cc`      same effect as:  `(aa NOT bb) NOT cc`

`and(aa,bb,cc)`          `aa and bb and cc`

`99w(aa,bb,cc)          ordered span query with slop 98`

`99n(aa,bb,cc)`         `unordered span query with slop 98`

`20n(aa*,bb*)`

`3w(a?a or bb?, cc*)`

`title: text: aa`

`title : text : aa or bb`

`title:text: aa not bb`

`title:aa not text:bb`

`cc 3w dd`              infix: dual.

`cc N dd N ee`           same effect as:   `(cc N dd) N ee`

`text: aa 3d bb`

For examples on using the Surround language, see the
test packages.


##Development status

Not tested: multiple fields, internally mapped to OR queries,
not compared to Lucene's MultipleFieldQuery.

* suffix truncation is implemented very similar to Lucene's PrefixQuery.

    Wildcards (`?` and internal `*`) are implemented with regular expressions
    to allow further variations. A reimplementation using
    WildCardTermEnum (correct name?) should be no problem.
    
    Warnings about missing terms are sent to System.out, this might
    be replaced by another stream, and tested for in the tests.
    
    BooleanQueryTst.TestCollector uses a results checking method that should
    be replaced by the checking method from Lucene's TestBasics.java.
