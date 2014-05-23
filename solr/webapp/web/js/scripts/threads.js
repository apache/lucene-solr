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

// #/~threads
sammy.get
(
  /^#\/(~threads)$/,
  function( context )
  {
    var content_element = $( '#content' );

    $.get
    (
      'tpl/threads.html',
      function( template )
      {
        content_element
          .html( template );

        $.ajax
        (
          {
            url : app.config.solr_path + '/admin/info/threads?wt=json',
            dataType : 'json',
            context : $( '#threads', content_element ),
            beforeSend : function( xhr, settings )
            {
            },
            success : function( response, text_status, xhr )
            {
              var self = this;

              var threadDumpData = response.system.threadDump;
              var threadDumpContent = [];
              var c = 0;
              for( var i = 1; i < threadDumpData.length; i += 2 )
              {
                var state = threadDumpData[i].state.esc();
                var name = '<a title="' + state +'"><span>' + threadDumpData[i].name.esc() + ' (' + threadDumpData[i].id.esc() + ')</span></a>';

                var classes = [state];
                var details = '';

                if( 0 !== c % 2 )
                {
                  classes.push( 'odd' );
                }

                if( threadDumpData[i].lock )
                {
                  classes.push( 'lock' );
                  name += "\n" + '<p title="Waiting on">' + threadDumpData[i].lock.esc() + '</p>';
                }

                if( threadDumpData[i].stackTrace && 0 !== threadDumpData[i].stackTrace.length )
                {
                  classes.push( 'stacktrace' );

                  var stack_trace = threadDumpData[i].stackTrace
                            .join( '###' )
                            .esc()
                            .replace( /\(/g, '&#8203;(' )
                            .replace( /###/g, '</li><li>' );

                  name += '<div>' + "\n"
                       + '<ul>' + "\n"
                       + '<li>' + stack_trace + '</li>'
                       + '</ul>' + "\n"
                       + '</div>';
                }

                var item = '<tr class="' + classes.join( ' ' ) +'">' + "\n"
                         + '<td class="name">' + name + '</td>' + "\n"
                         + '<td class="time">' + threadDumpData[i].cpuTime.esc() + '<br>' + threadDumpData[i].userTime.esc() + '</td>' + "\n"
                         + '</tr>';
                                
                threadDumpContent.push( item );
                c++;
              }

              var threadDumpBody = $( '#thread-dump tbody', this );

              threadDumpBody
                .html( threadDumpContent.join( "\n" ) );
                            
              $( '.name a', threadDumpBody )
                .die( 'click' )
                .live
                (
                  'click',
                  function( event )
                  {
                    $( this ).closest( 'tr' )
                      .toggleClass( 'open' );
                  }
                );
                            
              $( '.controls a', this )
                .die( 'click' )
                .live
                (
                  'click',
                  function( event )
                  {
                    var threads_element = $( self );
                    var is_collapsed = threads_element.hasClass( 'collapsed' );
                    var thread_rows = $( 'tr', threads_element );

                    thread_rows
                      .each
                      (
                        function( index, element )
                        {
                          if( is_collapsed )
                          {
                            $( element )
                              .addClass( 'open' );
                          }
                          else
                          {
                            $( element )
                              .removeClass( 'open' );
                          }
                        }
                      );

                    threads_element
                      .toggleClass( 'collapsed' )
                      .toggleClass( 'expanded' );
                  }
                );
            },
            error : function( xhr, text_status, error_thrown)
            {
            },
            complete : function( xhr, text_status )
            {
            }
          }
        );
      }
    );
  }
);