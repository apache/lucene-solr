package org.apache.lucene.index;

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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;

/** 
 * Setup a large compound file with a number of components, each of
 * which is a sequential file (so that we can easily tell that we are
 * reading in the right byte). The methods sets up 20 files - f0 to f19,
 * the size of each file is 1000 bytes.
 */
public class TestCompoundFile2 extends LuceneTestCase {
  /* nocommit: fold all these tests into BaseCompoundFormatTestCase */

}
