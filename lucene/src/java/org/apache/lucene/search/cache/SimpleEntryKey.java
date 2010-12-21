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


public class SimpleEntryKey extends EntryKey
{
  public final Class clazz;
  public final Object[] args;
  public final int hash;

  public SimpleEntryKey( Class clazz, Object ... args ) {
    this.clazz = clazz;
    this.args = args;

    int hash = clazz.hashCode();
    if( args != null ) {
      for( Object obj : args ) {
        hash ^= obj.hashCode();
      }
    }
    this.hash = hash;
  }

  @Override
  public boolean equals(Object obj) {
    if( obj instanceof SimpleEntryKey ) {
      SimpleEntryKey key = (SimpleEntryKey)obj;
      if( key.hash != hash ||
          key.clazz != clazz ||
          key.args.length != args.length ) {
        return false;
      }

      // In the off chance that the hash etc is all the same
      // we should actually check the values
      for( int i=0; i<args.length; i++ ) {
        if( !args[i].equals( key.args[i] ) ) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append( '[' ).append( clazz.getName() ).append( ':' );
    for( Object v : args ) {
      str.append( v ).append( ':' );
    }
    str.append( hash ).append( ']' );
    return str.toString();
  }
}
