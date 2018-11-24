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

package org.apache.lucene.luke.models;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.luke.models.util.IndexUtils;
import org.apache.lucene.store.Directory;

/**
 * Abstract model class. It holds index reader object and provides basic features for all concrete sub classes.
 */
public abstract class LukeModel {

  protected Directory dir;

  protected IndexReader reader;

  protected IndexCommit commit;

  protected LukeModel(@Nonnull IndexReader reader) {
    this.reader = reader;

    if (reader instanceof DirectoryReader) {
      DirectoryReader dr = (DirectoryReader) reader;
      this.dir = dr.directory();
      try {
        this.commit = dr.getIndexCommit();
      } catch (IOException e) {
        throw new LukeException(e.getMessage(), e);
      }
    } else {
      this.dir = null;
      this.commit = null;
    }

  }

  protected LukeModel(@Nonnull Directory dir) {
    this.dir = dir;
  }

  public Collection<String> getFieldNames() {
    return IndexUtils.getFieldNames(reader);
  }

}
