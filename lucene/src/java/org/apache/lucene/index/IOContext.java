package org.apache.lucene.index;

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

/**
 * IOContext holds additional details on the merge/search context. A IOContext
 * object can never be initialized as null as passed as a parameter to either
 * {@link #org.apache.lucene.store.Directory.openInput()} or
 * {@link #org.apache.lucene.store.Directory.createInput()}
 */
public class IOContext {

  /**
   * Context is a enumerator which specifies the context in which the Directory is being used for.
   */
  public enum Context {MERGE,READ,FLUSH,DEFAULT};
  
  /**
   * An object of a enumerator Context type
   */
  public final Context context;
  
  public final MergeInfo mergeInfo;
  
  public final boolean readOnce;
  
  public static final IOContext DEFAULT = new IOContext(Context.DEFAULT);
  
  public static final IOContext READONCE = new IOContext(true);
  
  public static final IOContext READ = new IOContext(false);
  
  public IOContext () {
    this(false);
  }
  
  public IOContext(Context context) {
    this(context, null);    
  }
  
  private IOContext(boolean readOnce) {
    this.context = Context.READ;
    this.mergeInfo = null;    
    this.readOnce = readOnce;
  }
  
  public IOContext (MergeInfo mergeInfo) {    
    this(Context.MERGE, mergeInfo);
  }

  private IOContext (Context context, MergeInfo mergeInfo ) {
    assert context != Context.MERGE || mergeInfo != null;
    this.context = context;
    this.readOnce = false;
    this.mergeInfo = mergeInfo;
  }
  
}