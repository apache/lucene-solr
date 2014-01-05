package org.apache.lucene.server;

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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;

/** Extends {@link IndexSearcher}, adding state and public
 *  methods. */
public class MyIndexSearcher extends IndexSearcher {

  /** Sole constructor. */
  public MyIndexSearcher(IndexReader r) {
    super(r);
  }

  // nocommit ugly that we need to do this, to handle the
  // in order vs out of order chicken/egg issue:

  /** Runs a search, from a provided {@link Weight} and
   *  {@link Collector}; this method is not public in
   *  {@link IndexSearcher}. */
  public void search(Weight w, Collector c) throws IOException {
    super.search(getIndexReader().leaves(), w, c);
  }
}
