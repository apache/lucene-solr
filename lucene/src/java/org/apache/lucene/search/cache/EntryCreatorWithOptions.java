package org.apache.lucene.search.cache;


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


public abstract class EntryCreatorWithOptions<T> extends EntryCreator<T>
{
  public static final int OPTION_VALIDATE = 1;

  private int flags;

  public EntryCreatorWithOptions( int flag ) {
    this.flags = flag;
  }

  @Override
  public boolean shouldValidate() {
    return hasOption( OPTION_VALIDATE );
  }

  public boolean hasOption( int key )
  {
    return (flags & key) == key;
  }

  public void setFlag(int flag) {
    this.flags |= flag;
  }
}
