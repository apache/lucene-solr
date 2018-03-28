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

# bin/solr Tests

This directory contains tests for the `bin/solr` command-line scripts.  For
instructions on running these tests, run `bin-test/test -h`.

## Test Harness/Infrastructure

Where possible, these tests model themselves after the pattern well-established
by JUnit.
  - JUnit's `@Test` is emulated using the function name prefix: `solr_test_`
    Any bash functions starting with that prefix are identified as tests.
  - JUnit's `@Before` and `@After` are imitated using the function names
    `solr_unit_test_before`, and `solr_unit_test_after`.  If a suite contains
    these functions, they will be run before and after each test.
  - JUnit's `@BeforeClass` and `@AfterClass` are imitated using the function
    names: `solr_suite_before`, and `solr_suite_after`.  If a suite contains
    these functions, they will be run at the very beginning and end of suite
    execution.
  - Test success/failure is judged by the test's return value. 0 indicates
    success; non-zero indicates failure.  Unlike in JUnit/Java which has
    exceptions, bash assertions have no way to suspend test execution on
    failure.  Because of this, assertions are often followed by ` || return 1`,
    which ensures the test exists immediately if the assertion fails.  Existing
    tests provided examples of this.

## Test Helpers

A variety of assertions and general utilities are available for use in
`bin-test/utils/`.

## Limitations

1. Currently this test suite is only available for \*nix environments
2. Tests written in bash are both slow, and harder to maintain than traditional
   JUnit tests.  If a test _can_ be written as a JUnit test, it should be.  This
   suite should only be used to test things that cannot be tested by JUnit.
