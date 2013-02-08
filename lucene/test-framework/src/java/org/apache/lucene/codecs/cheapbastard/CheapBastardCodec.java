package org.apache.lucene.codecs.cheapbastard;

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

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40TermVectorsFormat;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;

/** Codec that tries to use as little ram as possible because he spent all his money on beer */
// TODO: better name :) 
// but if we named it "LowMemory" in codecs/ package, it would be irresistible like optimize()!
public class CheapBastardCodec extends FilterCodec {
  
  // TODO: would be better to have no terms index at all and bsearch a terms dict
  private final PostingsFormat postings = new Lucene41PostingsFormat(100, 200);
  // uncompressing versions, waste lots of disk but no ram
  private final StoredFieldsFormat storedFields = new Lucene40StoredFieldsFormat();
  private final TermVectorsFormat termVectors = new Lucene40TermVectorsFormat();
  // these go to disk for all docvalues/norms datastructures
  private final DocValuesFormat docValues = new CheapBastardDocValuesFormat();
  private final NormsFormat norms = new CheapBastardNormsFormat();

  public CheapBastardCodec() {
    super("CheapBastard", new Lucene42Codec());
  }
  
  public PostingsFormat postingsFormat() {
    return postings;
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    return docValues;
  }
  
  @Override
  public NormsFormat normsFormat() {
    return norms;
  }
  
  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return storedFields;
  }

  @Override
  public TermVectorsFormat termVectorsFormat() {
    return termVectors;
  }
}
