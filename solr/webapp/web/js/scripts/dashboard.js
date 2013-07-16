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

var set_healthcheck_status = function( status )
{
  var hc_button = $( '.healthcheck-status' )
  if ( status == 'enable' )
  {
    hc_button.parents( 'dd' )
      .removeClass( 'ico-0' )
      .addClass( 'ico-1' );
    hc_button
      .addClass( 'enabled' )
      .html( 'disable ping' );
  } else {
    hc_button.parents( 'dd' )
      .removeClass( 'ico-1')
      .addClass( 'ico-0' );
    hc_button
      .removeClass( 'enabled' )
      .html( 'enable ping' );
  }
};

// #/:core
sammy.get
(
  new RegExp( app.core_regex_base + '$' ),
  function( context )
  {
    var core_basepath = this.active_core.attr( 'data-basepath' );
    var content_element = $( '#content' );
        
    content_element
      .removeClass( 'single' );
    
    if( !app.core_menu.data( 'admin-extra-loaded' ) )
    {
      app.core_menu.data( 'admin-extra-loaded', new Date() );

      $.get
      (
        core_basepath + '/admin/file/?file=admin-extra.menu-top.html&contentType=text/html;charset=utf-8',
        function( menu_extra )
        {
          app.core_menu
            .prepend( menu_extra );
        }
      );
      
      $.get
      (
        core_basepath + '/admin/file/?file=admin-extra.menu-bottom.html&contentType=text/html;charset=utf-8',
        function( menu_extra )
        {
          app.core_menu
            .append( menu_extra );
        }
      );
    }
        
    $.get
    (
      'tpl/dashboard.html',
      function( template )
      {
        content_element
          .html( template );
                    
        var dashboard_element = $( '#dashboard' );
                                     
        $.ajax
        (
          {
            url : core_basepath + '/admin/luke?wt=json&show=index&numTerms=0',
            dataType : 'json',
            context : $( '#statistics', dashboard_element ),
            beforeSend : function( xhr, settings )
            {
              $( 'h2', this )
                .addClass( 'loader' );
                            
              $( '.message', this )
                .show()
                .html( 'Loading ...' );
                            
              $( '.content', this )
                .hide();
            },
            success : function( response, text_status, xhr )
            {
              $( '.message', this )
                .empty()
                .hide();
                            
              $( '.content', this )
                .show();
                                
              var data = {
                'index_num-docs' : response['index']['numDocs'],
                'index_max-doc' : response['index']['maxDoc'],
                'index_deleted-docs' : response['index']['deletedDocs'],
                'index_version' : response['index']['version'],
                'index_segmentCount' : response['index']['segmentCount'],
                'index_last-modified' : response['index']['lastModified']
              };
                            
              for( var key in data )
              {
                $( '.' + key, this )
                  .show();
                                
                $( '.value.' + key, this )
                  .html( data[key] );
              }

              var optimized_element = $( '.value.index_optimized', this );
              if( !response['index']['hasDeletions'] )
              {
                optimized_element
                  .addClass( 'ico-1' );

                $( 'span', optimized_element )
                  .html( 'yes' );
              }
              else
              {
                optimized_element
                  .addClass( 'ico-0' );

                $( 'span', optimized_element )
                  .html( 'no' );
              }

              var current_element = $( '.value.index_current', this );
              if( response['index']['current'] )
              {
                current_element
                  .addClass( 'ico-1' );

                $( 'span', current_element )
                  .html( 'yes' );
              }
              else
              {
                current_element
                  .addClass( 'ico-0' );

                $( 'span', current_element )
                  .html( 'no' );
              }

              $( 'a', optimized_element )
                .die( 'click' )
                .live
                (
                  'click',
                  function( event )
                  {                        
                    $.ajax
                    (
                      {
                      url : core_basepath + '/update?optimize=true&waitFlush=true&wt=json',
                      dataType : 'json',
                      context : $( this ),
                      beforeSend : function( xhr, settings )
                      {
                        this
                          .addClass( 'loader' );
                      },
                      success : function( response, text_status, xhr )
                      {
                        this.parents( 'dd' )
                          .removeClass( 'ico-0' )
                          .addClass( 'ico-1' );
                      },
                      error : function( xhr, text_status, error_thrown)
                      {
                        console.warn( 'd0h, optimize broken!' );
                      },
                      complete : function( xhr, text_status )
                      {
                        this
                          .removeClass( 'loader' );
                      }
                      }
                    );
                  }
                );

              $( '.timeago', this )
                                 .timeago();
            },
            error : function( xhr, text_status, error_thrown )
            {
              this
                .addClass( 'disabled' );
                            
              $( '.message', this )
                .show()
                .html( 'Luke is not configured' );
            },
            complete : function( xhr, text_status )
            {
              $( 'h2', this )
                .removeClass( 'loader' );
            }
          }
        );
                
        $.ajax
        (
          {
            url : core_basepath + '/replication?command=details&wt=json',
            dataType : 'json',
            context : $( '#replication', dashboard_element ),
            beforeSend : function( xhr, settings )
            {
              $( 'h2', this )
                .addClass( 'loader' );
                            
              $( '.message', this )
                .show()
                .html( 'Loading' );

              $( '.content', this )
                .hide();
            },
            success : function( response, text_status, xhr )
            {
              $( '.message', this )
                .empty()
                .hide();

              $( '.content', this )
                .show();
                            
              $( '.replication', context.active_core )
                .show();
                            
              var data = response.details;
              var is_slave = 'undefined' !== typeof( data.slave );
              var headline = $( 'h2 span', this );
              var details_element = $( '#details', this );
              var current_type_element = $( ( is_slave ? '.slave' : '.masterSearch' ), this );
              var master_data = is_slave ? data.slave.masterDetails : data;

              if( is_slave )
              {
                this
                  .addClass( 'slave' );
                                
                headline
                  .html( headline.html() + ' (Slave)' );
              }
              else
              {
                this
                  .addClass( 'master' );
                                
                headline
                  .html( headline.html() + ' (Master)' );
              }

              // the currently searchable commit regardless of type
              $( '.version div', current_type_element )
                .html( data.indexVersion );
              $( '.generation div', current_type_element )
                .html( data.generation );
              $( '.size div', current_type_element )
                .html( data.indexSize );
                            
              // what's replicable on the master
              var master_element = $( '.master', details_element );
              $( '.version div', master_element )
                .html( master_data.master.replicableVersion || '-' );
              $( '.generation div', master_element )
                .html( master_data.master.replicableGeneration || '-' );
              $( '.size div', master_element )
                .html( "-" );

              if( is_slave )
              {
                var master_element = $( '.masterSearch', details_element );
                $( '.version div', master_element )
                  .html( data.slave.masterDetails.indexVersion );
                $( '.generation div', master_element )
                  .html( data.slave.masterDetails.generation );
                $( '.size div', master_element )
                  .html( data.slave.masterDetails.indexSize );
                                
                // warnings if slave version|gen doesn't match what's replicable
                if( data.indexVersion !== master_data.master.replicableVersion )
                {
                  $( '.version', details_element )
                    .addClass( 'diff' );
                }
                else
                {
                  $( '.version', details_element )
                    .removeClass( 'diff' );
                }
                                
                if( data.generation !== master_data.master.replicableGeneration )
                {
                  $( '.generation', details_element )
                    .addClass( 'diff' );
                }
                else
                {
                  $( '.generation', details_element )
                    .removeClass( 'diff' );
                }
              }
            },
            error : function( xhr, text_status, error_thrown)
            {
              this
                .addClass( 'disabled' );
                            
              $( '.message', this )
                .show()
                .html( 'Replication is not configured' );
            },
            complete : function( xhr, text_status )
            {
              $( 'h2', this )
                .removeClass( 'loader' );
            }
          }
        );

        $.ajax
        (
          {
            url : core_basepath + '/admin/system?wt=json',
            dataType : 'json',
            context : $( '#instance', dashboard_element ),
            beforeSend : function( xhr, settings )
            {
              $( 'h2', this )
                .addClass( 'loader' );

              $( '.message', this )
                .show()
                .html( 'Loading' );

              $( '.content', this )
                .hide();
            },
            success : function( response, text_status, xhr )
            {
              $( '.message', this )
                .empty()
                .hide();

              $( '.content', this )
                .show();
                            
              $( 'dl', this )
                .show();
                            
              var data = {
                'dir_cwd' : response.core.directory.cwd,
                'dir_instance' : response.core.directory.instance,
                'dir_data' : response.core.directory.data,
                'dir_index' : response.core.directory.index,
                'dir_impl' : response.core.directory.dirimpl
              };
                            
              for( var key in data )
              {
                $( '.' + key, this )
                  .show();
                                
                $( '.value.' + key, this )
                  .html( data[key] );
              }
            },
            error : function( xhr, text_status, error_thrown)
            {
              this
                .addClass( 'disabled' );
                            
              $( '.message', this )
                .show()
                .html( '/admin/system Handler is not configured' );
            },
            complete : function( xhr, text_status )
            {
              $( 'h2', this )
                .removeClass( 'loader' );
            }
          }
        );
                
        $.ajax
        (
          {
            url : core_basepath + '/admin/file/?file=admin-extra.html',
            dataType : 'html',
            context : $( '#admin-extra', dashboard_element ),
            beforeSend : function( xhr, settings )
            {
              $( 'h2', this )
                .addClass( 'loader' );
                            
              $( '.message', this )
                .show()
                .html( 'Loading' );

              $( '.content', this )
                .hide();
            },
            success : function( response, text_status, xhr )
            {
              $( '.message', this )
                .hide()
                .empty();

              $( '.content', this )
                .show()
                .html( response );
            },
            error : function( xhr, text_status, error_thrown)
            {
              this
                .addClass( 'disabled' );
                            
              $( '.message', this )
                .show()
                .html( 'We found no "admin-extra.html" file.' );
            },
            complete : function( xhr, text_status )
            {
              $( 'h2', this )
                .removeClass( 'loader' );
            }
          }
        );

        $.ajax
        (
          {
            url : core_basepath + '/admin/ping?action=status&wt=json',
            dataType : 'json',
            context : $( '#healthcheck', dashboard_element ),
            beforeSend : function( xhr, settings )
            {
              $( 'h2', this )
                .addClass( 'loader' );
                            
              $( '.message', this )
                .show()
                .html( 'Loading' );

              $( '.content', this )
                .hide();
            },
            success : function( response, text_status, xhr )
            {
              $( '.message', this )
                .empty()
                .hide();
                            
              $( '.content', this )
                .show();

              var status_element = $( '.value.status', this );
              var toggle_button = $( '.healthcheck-status', this );
              var status = response['status'];
              $( 'span', status_element ).html( status );

              var action = ( response['status'] == 'enabled' ) ? 'enable' : 'disable';  
              set_healthcheck_status(action);

              if( response['status'] == 'enabled' )
              {
                status_element
                  .addClass( 'ico-1' );
                toggle_button
                  .addClass( 'enabled' );
              }
              else
              {
                status_element
                  .addClass( 'ico-0' );
              }
              
              $( '.healthcheck-status', status_element )
                .die( 'click' )
                .live
                (
                  'click',
                  function( event )
                  {                      
                    var action = $(this).hasClass( 'enabled' ) ? 'disable' : 'enable';  
                    $.ajax
                    (
                      {
                        url : core_basepath + '/admin/ping?action=' + action + '&wt=json',
                        dataType : 'json',
                        context : $( this ),
                        beforeSend : function( xhr, settings )
                        {
                          this
                            .addClass( 'loader' );
                        },
                        success : function( response, text_status, xhr )
                        {
                          set_healthcheck_status(action);
                        },
                        error : function( xhr, text_status, error_thrown)
                        {
                          console.warn( 'd0h, enable broken!' );
                        },
                        complete : function( xhr, text_status )
                        {
                          this
                            .removeClass( 'loader' );
                        }
                      }
                    );
                  }
                );
            },
            error : function( xhr, text_status, error_thrown)
            {
              this
                .addClass( 'disabled' );
                            
              $( '.message', this )
                .show()
                .html( 'Ping request handler is not configured with a healthcheck file.' );
            },
            complete : function( xhr, text_status )
            {
              $( 'h2', this )
                .removeClass( 'loader' );
            }
          }
        );
                
      }
    );
  }
);
