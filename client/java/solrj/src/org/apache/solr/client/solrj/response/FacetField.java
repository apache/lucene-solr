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

package org.apache.solr.client.solrj.response;

import java.util.ArrayList;
import java.util.List;
 
 /**
  * A utility class to hold the facet response.  It could use the NamedList container,
  * but for JSTL, it is nice to have something that implements List so it can be iterated
  * 
  * @author ryan
  * @version $Id$
  * @since solr 1.3
  */
 public class FacetField 
 {
   public static class Count 
   {
     private String _name = null;
     private int _count = 0;
     // hang onto the FacetField for breadcrumb creation convenience
     private FacetField _ff = null;
     
     public Count( FacetField ff, String n, int c )
     {
       _name = n;
       _count = c;
       _ff = ff;
     }
     
     public String getName() {
       return _name;
     }
     
     public void setName( String n )
     {
       _name = n;
     }

     public int getCount() {
       return _count;
     }
     
     public void setCount( int c )
     {
       _count = c;
     }
     
     public FacetField getFacetField() {
       return _ff;
     }
     
     @Override
     public String toString()
     {
       return _name+" ("+_count+")";
     }
     
     public String getAsFilterQuery() {
       if (_ff.getName().equals("facet_queries")) {
         return _name;
       } else {
         return _ff._name+":\""+_name+"\"";
       }
     }
   }
   
   private String      _name   = null;
   private List<Count> _values = null;
   
   public FacetField( final String n )
   {
     _name = n;
   }

   /**
    * Insert at the end of the list
    */
   public void add( String name, int cnt )
   {
     if( _values == null ) {
       _values = new ArrayList<Count>( 30 );
     }
     _values.add( new Count( this, name, cnt ) );
   }

   /**
    * Insert at the beginning of the list.
    */
   public void insert( String name, int cnt )
   {
     if( _values == null ) {
       _values = new ArrayList<Count>( 30 );
     }
     _values.add( 0, new Count( this, name, cnt ) );
   }

   public String getName() {
     return _name;
   }

   public List<Count> getValues() {
     return _values;
   }
   
   public int getValueCount()
   {
     return _values == null ? 0 : _values.size();
   }

   public FacetField getLimitingFields(int max) 
   {
     FacetField ff = new FacetField( _name );
     if( _values != null ) {
       ff._values = new ArrayList<Count>( _values.size() );
       for( Count c : _values ) {
         if( c._count < max ) { // !equal to
           ff._values.add( c );
         }
       }
     }
     return ff;
   }
   
   @Override
   public String toString()
   {
     return _name + ":" + _values;
   }
 }
