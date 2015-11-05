package org.apache.solr.common.params;

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

import java.util.Locale;

public interface CollectionParams 
{
  /** What action **/
  public final static String ACTION = "action";
  public final static String NAME = "name";
  


  public enum CollectionAction {
    CREATE(true),
    DELETE(true),
    RELOAD(true),
    SYNCSHARD(true),
    CREATEALIAS(true),
    DELETEALIAS(true),
    SPLITSHARD(true),
    DELETESHARD(true),
    CREATESHARD(true),
    DELETEREPLICA(true),
    FORCELEADER(true),
    MIGRATE(true),
    ADDROLE(true),
    REMOVEROLE(true),
    CLUSTERPROP(true),
    REQUESTSTATUS(false),
    ADDREPLICA(true),
    OVERSEERSTATUS(false),
    LIST(false),
    CLUSTERSTATUS(false),
    ADDREPLICAPROP(true),
    DELETEREPLICAPROP(true),
    BALANCESHARDUNIQUE(true),
    REBALANCELEADERS(true),
    MODIFYCOLLECTION(true),
    MIGRATESTATEFORMAT(true);
    
    public final boolean isWrite;

    CollectionAction(boolean isWrite) {
      this.isWrite = isWrite;
    }

    public static CollectionAction get(String p) {
      if( p != null ) {
        try {
          return CollectionAction.valueOf( p.toUpperCase(Locale.ROOT) );
        }
        catch( Exception ex ) {}
      }
      return null;
    }
    public boolean isEqual(String s){
      if(s == null) return false;
      return toString().equals(s.toUpperCase(Locale.ROOT));
    }
    public String toLower(){
      return toString().toLowerCase(Locale.ROOT);
    }

  }
}
