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
package org.apache.lucene.codecs.simpletext;


import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * plain text doc values format.
 * <p>
 * <b>FOR RECREATIONAL USE ONLY</b>
 * <p>
 * the .dat file contains the data.
 *  for numbers this is a "fixed-width" file, for example a single byte range:
 *  <pre>
 *  field myField
 *    type NUMERIC
 *    minvalue 0
 *    pattern 000
 *  005
 *  T
 *  234
 *  T
 *  123
 *  T
 *  ...
 *  </pre>
 *  so a document's value (delta encoded from minvalue) can be retrieved by 
 *  seeking to startOffset + (1+pattern.length()+2)*docid. The extra 1 is the newline. 
 *  The extra 2 is another newline and 'T' or 'F': true if the value is real, false if missing.
 *  
 *  for bytes this is also a "fixed-width" file, for example:
 *  <pre>
 *  field myField
 *    type BINARY
 *    maxlength 6
 *    pattern 0
 *  length 6
 *  foobar[space][space]
 *  T
 *  length 3
 *  baz[space][space][space][space][space]
 *  T
 *  ...
 *  </pre>
 *  so a doc's value can be retrieved by seeking to startOffset + (9+pattern.length+maxlength+2)*doc
 *  the extra 9 is 2 newlines, plus "length " itself.
 *  the extra 2 is another newline and 'T' or 'F': true if the value is real, false if missing.
 *  
 *  for sorted bytes this is a fixed-width file, for example:
 *  <pre>
 *  field myField
 *    type SORTED
 *    numvalues 10
 *    maxLength 8
 *    pattern 0
 *    ordpattern 00
 *  length 6
 *  foobar[space][space]
 *  length 3
 *  baz[space][space][space][space][space]
 *  ...
 *  03
 *  06
 *  01
 *  10
 *  ...
 *  </pre>
 *  so the "ord section" begins at startOffset + (9+pattern.length+maxlength)*numValues.
 *  a document's ord can be retrieved by seeking to "ord section" + (1+ordpattern.length())*docid
 *  an ord's value can be retrieved by seeking to startOffset + (9+pattern.length+maxlength)*ord
 *  
 *  for sorted set this is a fixed-width file very similar to the SORTED case, for example:
 *  <pre>
 *  field myField
 *    type SORTED_SET
 *    numvalues 10
 *    maxLength 8
 *    pattern 0
 *    ordpattern XXXXX
 *  length 6
 *  foobar[space][space]
 *  length 3
 *  baz[space][space][space][space][space]
 *  ...
 *  0,3,5   
 *  1,2
 *  
 *  10
 *  ...
 *  </pre>
 *  so the "ord section" begins at startOffset + (9+pattern.length+maxlength)*numValues.
 *  a document's ord list can be retrieved by seeking to "ord section" + (1+ordpattern.length())*docid
 *  this is a comma-separated list, and it's padded with spaces to be fixed width. so trim() and split() it.
 *  and beware the empty string!
 *  an ord's value can be retrieved by seeking to startOffset + (9+pattern.length+maxlength)*ord
 *  
 *  for sorted numerics, it's encoded (not very creatively) as a comma-separated list of strings the same as binary.
 *  beware the empty string!
 *   
 *  the reader can just scan this file when it opens, skipping over the data blocks
 *  and saving the offset/etc for each field. 
 *  @lucene.experimental
 */
class SimpleTextDocValuesFormat extends DocValuesFormat {
  
  public SimpleTextDocValuesFormat() {
    super("SimpleText");
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new SimpleTextDocValuesWriter(state, "dat");
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new SimpleTextDocValuesReader(state, "dat");
  }
}
