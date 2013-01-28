/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

$( '.ping a', app.core_menu )
  .live
  (
    'click',
    function( event )
    {
      $.ajax
      (
        {
          url : $( this ).attr( 'rel' ) + '?wt=json&ts=' + (new Date).getTime(),
          dataType : 'json',
          context: this,
          beforeSend : function( arr, form, options )
          {
            loader.show( this );
          },
          success : function( response, text_status, xhr )
          {
            $( this )
              .removeAttr( 'title' );
                        
            $( this ).parents( 'li' )
              .removeClass( 'error' );
                            
            var qtime_element = $( '.qtime', this );
                        
            if( 0 === qtime_element.size() )
            {
              qtime_element = $( '<small class="qtime"> (<span></span>)</small>' );
                            
              $( this )
                .append( qtime_element );
            }
                        
            $( 'span', qtime_element )
              .html( response.responseHeader.QTime + 'ms' );
          },
          error : function( xhr, text_status, error_thrown )
          {
            $( this )
              .attr( 'title', '/admin/ping is not configured (' + xhr.status + ': ' + error_thrown + ')' );
                        
            $( this ).parents( 'li' )
              .addClass( 'error' );
          },
          complete : function( xhr, text_status )
          {
            loader.hide( this );
          }
        }
      );
            
      return false;
    }
  );