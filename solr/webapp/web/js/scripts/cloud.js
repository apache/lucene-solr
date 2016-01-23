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

var zk_error = function zk_error( xhr, text_status, error_thrown )
{
  var zk = null;
  try
  {
    eval( 'zk = ' + xhr.responseText + ';' );
  }
  catch( e ) {}

  var message = '<p class="txt">Loading of "<code>' + xhr.url + '</code>" '
              + 'failed (HTTP-Status <code>' + xhr.status + '</code>)</p>' + "\n";

  if( zk.error )
  {
    message += '<p class="msg">"' + zk.error.esc() + '"</p>' + "\n";
  }
  
  this.closest( '#cloud' )
    .html( '<div class="block" id="error">' + message + '</div>' );
};

var init_debug = function( cloud_element )
{
  var debug_element = $( '#debug', cloud_element );
  var debug_button = $( '#menu #cloud .dump a' );

  var clipboard_element = $( '.clipboard', debug_element );
  var clipboard_button = $( 'a', clipboard_element );

  debug_button
    .die( 'click' )
    .live
    (
      'click',
      function( event )
      {
        debug_element.trigger( 'show' );
        return false;
      }
    );

  $( '.close', debug_element )
    .die( 'click' )
    .live
    (
      'click',
      function( event )
      {
        debug_element.trigger( 'hide' );
        return false;
      }
    );

  $( '.clipboard', debug_element )
    .die( 'click' )
    .live
    (
      'click',
      function( event )
      {
        return false;
      }
    );

  debug_element
    .die( 'show' )
    .live
    (
      'show',
      function( event )
      {
        debug_element.show();

        $.ajax
        (
          {
            url : app.config.solr_path + '/admin/zookeeper?wt=json&dump=true',
            dataType : 'text',
            context : debug_element,
            beforeSend : function( xhr, settings )
            {
              $( '.debug', debug_element )
                .html( '<span class="loader">Loading Dump ...</span>' );

              ZeroClipboard.setMoviePath( 'img/ZeroClipboard.swf' );

              clipboard_client = new ZeroClipboard.Client();
                              
              clipboard_client.addEventListener
              (
                'load',
                function( client )
                {
                }
              );

              clipboard_client.addEventListener
              (
                'complete',
                function( client, text )
                {
                  clipboard_element
                    .addClass( 'copied' );

                  clipboard_button
                    .data( 'text', clipboard_button.text() )
                    .text( clipboard_button.data( 'copied' ) );
                }
              );
            },
            success : function( response, text_status, xhr )
            {
              clipboard_client.glue
              (
                clipboard_element.get(0),
                clipboard_button.get(0)
              );

              clipboard_client.setText( response.replace( /\\/g, '\\\\' ) );

              $( '.debug', debug_element )
                .removeClass( 'loader' )
                .text( response );
            },
            error : function( xhr, text_status, error_thrown )
            {
            },
            complete : function( xhr, text_status )
            {
            }
          }
        );
      }
    )
    .die( 'hide' )
    .live
    (
      'hide',
      function( event )
      {
        $( '.debug', debug_element )
          .empty();

        clipboard_element
          .removeClass( 'copied' );

        clipboard_button
          .data( 'copied', clipboard_button.text() )
          .text( clipboard_button.data( 'text' ) );

        clipboard_client.destroy();

        debug_element.hide();
      }
    );
};

var helper_path_class = function( p )
{
  var classes = [ 'link' ];
  classes.push( 'lvl-' + p.target.depth );

  if( p.target.data && p.target.data.leader )
  {
    classes.push( 'leader' );
  }

  if( p.target.data && p.target.data.state )
  {
    classes.push( p.target.data.state );
  }

  return classes.join( ' ' );
};

var helper_node_class = function( d )
{
  var classes = [ 'node' ];
  classes.push( 'lvl-' + d.depth );

  if( d.data && d.data.leader )
  {
    classes.push( 'leader' );
  }

  if( d.data && d.data.state )
  {
    classes.push( d.data.state );
  }

  return classes.join( ' ' );
};

var helper_data = {
  protocol: [],
  host: [],
  hostname: [],
  port: [],
  pathname: []
};

var helper_node_text = function( d )
{
  if( !d.data || !d.data.uri )
  {
    return d.name;
  }

  var name = d.data.uri.hostname;

  if( 1 !== helper_data.protocol.length )
  {
    name = d.data.uri.protocol + '//' + name;
  }

  if( 1 !== helper_data.port.length )
  {
    name += ':' + d.data.uri.port;
  }

  if( 1 !== helper_data.pathname.length )
  {
    name += d.data.uri.pathname;
  }

  return name;
};

var generate_graph = function( graph_element, graph_data, leaf_count )
{
  var w = graph_element.width(),
      h = leaf_count * 20;

  var tree = d3.layout.tree()
    .size([h, w - 400]);

  var diagonal = d3.svg.diagonal()
    .projection(function(d) { return [d.y, d.x]; });

  var vis = d3.select( '#canvas' ).append( 'svg' )
    .attr( 'width', w )
    .attr( 'height', h)
    .append( 'g' )
      .attr( 'transform', 'translate(100, 0)' );

  var nodes = tree.nodes( graph_data );

  var link = vis.selectAll( 'path.link' )
    .data( tree.links( nodes ) )
    .enter().append( 'path' )
      .attr( 'class', helper_path_class )
      .attr( 'd', diagonal );

  var node = vis.selectAll( 'g.node' )
    .data( nodes )
    .enter().append( 'g' )
      .attr( 'class', helper_node_class )
      .attr( 'transform', function(d) { return 'translate(' + d.y + ',' + d.x + ')'; } )

  node.append( 'circle' )
    .attr( 'r', 4.5 );

  node.append( 'text' )
    .attr( 'dx', function( d ) { return 0 === d.depth ? -8 : 8; } )
    .attr( 'dy', function( d ) { return 5; } )
    .attr( 'text-anchor', function( d ) { return 0 === d.depth ? 'end' : 'start'; } )
    .attr( 'data-href', function( d ) { return d.name; } )
    .text( helper_node_text );

  $( 'text[data-href*="//"]', graph_element )
    .die( 'click' )
    .live
    (
      'click',
      function()
      {
        location.href = $( this ).data( 'href' );
      }
    );
};

var generate_rgraph = function( graph_element, graph_data, leaf_count )
{
  var max_val = Math.min( graph_element.width(), $( 'body' ).height() )
  var r = max_val / 2;

  var cluster = d3.layout.cluster()
    .size([360, r - 160]);

  var diagonal = d3.svg.diagonal.radial()
    .projection(function(d) { return [d.y, d.x / 180 * Math.PI]; });

  var vis = d3.select( '#canvas' ).append( 'svg' )
    .attr( 'width', r * 2 )
    .attr( 'height', r * 2 )
    .append( 'g' )
      .attr( 'transform', 'translate(' + r + ',' + r + ')' );

  var nodes = cluster.nodes( graph_data );

  var link = vis.selectAll( 'path.link' )
    .data( cluster.links( nodes ) )
    .enter().append( 'path' )
      .attr( 'class', helper_path_class )
      .attr( 'd', diagonal );

  var node = vis.selectAll( 'g.node' )
    .data( nodes )
    .enter().append( 'g' )
      .attr( 'class', helper_node_class )
      .attr( 'transform', function(d) { return 'rotate(' + (d.x - 90) + ')translate(' + d.y + ')'; } )

  node.append( 'circle' )
    .attr( 'r', 4.5 );

  node.append( 'text' )
    .attr( 'dx', function(d) { return d.x < 180 ? 8 : -8; } )
    .attr( 'dy', '.31em' )
    .attr( 'text-anchor', function(d) { return d.x < 180 ? 'start' : 'end'; } )
    .attr( 'transform', function(d) { return d.x < 180 ? null : 'rotate(180)'; } )
    .attr( 'data-href', function( d ) { return d.name; } )
    .text( helper_node_text );

  $( 'text[data-href*="//"]', graph_element )
    .die( 'click' )
    .live
    (
      'click',
      function()
      {
        location.href = $( this ).data( 'href' );
      }
    );
};

var prepare_graph_data = function( response, graph_element, live_nodes, callback )
{  
    var state = null;
    eval( 'state = ' + response.znode.data + ';' );
    
    var leaf_count = 0;
    var graph_data = {
      name: null,
      children : []
    };

    for( var c in state )
    {
      var shards = [];
      for( var s in state[c].shards )
      {
        var nodes = [];
        for( var n in state[c].shards[s].replicas )
        {
          leaf_count++;
          var replica = state[c].shards[s].replicas[n]

          var uri = replica.base_url;
          var parts = uri.match( /^(\w+:)\/\/(([\w\d\.-]+)(:(\d+))?)(.+)$/ );
          var uri_parts = {
            protocol: parts[1],
            host: parts[2],
            hostname: parts[3],
            port: parseInt( parts[5] || 80, 10 ),
            pathname: parts[6]
          };
          
          helper_data.protocol.push( uri_parts.protocol );
          helper_data.host.push( uri_parts.host );
          helper_data.hostname.push( uri_parts.hostname );
          helper_data.port.push( uri_parts.port );
          helper_data.pathname.push( uri_parts.pathname );

          var status = replica.state;

          if( !live_nodes[replica.node_name] )
          {
            status = 'gone';
          }

          var node = {
            name: uri,
            data: {
              type : 'node',
              state : status,
              leader : 'true' === replica.leader,
              uri : uri_parts
            }
          };
          nodes.push( node );
        }

        var shard = {
          name: s,
          data: {
            type : 'shard'
          },
          children: nodes
        };
        shards.push( shard );
      }

      var collection = {
        name: c,
        data: {
          type : 'collection'
        },
        children: shards
      };
      graph_data.children.push( collection );
    }
    
    helper_data.protocol = $.unique( helper_data.protocol );
    helper_data.host = $.unique( helper_data.host );
    helper_data.hostname = $.unique( helper_data.hostname );
    helper_data.port = $.unique( helper_data.port );
    helper_data.pathname = $.unique( helper_data.pathname );

    callback( graph_element, graph_data, leaf_count );  
}

var update_status_filter = function(filterType, filterVal) {
  if (filterType == 'status') {
    $( '#cloudGraphPagingStatusFilter' ).val(filterVal);
    $( '#cloudGraphPagingStatusFilter' ).show();
    $( '#cloudGraphPagingFilter' ).hide();
    $( '#cloudGraphPagingFilter' ).val('');
  } else {
    $( '#cloudGraphPagingStatusFilter' ).hide();
    $( '#cloudGraphPagingStatusFilter' ).val('');
    $( '#cloudGraphPagingFilter' ).val(filterVal);
    $( '#cloudGraphPagingFilter' ).show();                  
  }  
};

var prepare_graph = function( graph_element, callback )
{
  $.ajax
  (
    {
      url : app.config.solr_path + '/admin/zookeeper?wt=json&path=%2Flive_nodes',
      dataType : 'json',
      success : function( response, text_status, xhr )
      {
        var live_nodes = {};
        for( var c in response.tree[0].children )
        {
          live_nodes[response.tree[0].children[c].data.title] = true;
        }

        var start = $( '#cloudGraphPagingStart' ).val();
        var rows = $( '#cloudGraphPagingRows' ).val();
        var clusterStateUrl = app.config.solr_path + '/admin/zookeeper?wt=json&detail=true&path=%2Fclusterstate.json&view=graph';
        if (start && rows)
          clusterStateUrl += ('&start='+start+'&rows='+rows);
        
        var filterType = $( '#cloudGraphPagingFilterType' ).val();
        if (filterType) {
          var filter = (filterType == 'status')
                         ? $( '#cloudGraphPagingStatusFilter' ).val() 
                         : $( '#cloudGraphPagingFilter' ).val();  
          if (filter)
            clusterStateUrl += ('&filterType='+filterType+'&filter='+filter);
        }
                
        $.ajax
        (
          {
            url : clusterStateUrl,
            dataType : 'json',
            context : graph_element,
            beforeSend : function( xhr, settings )
            {
              this.show();
            },
            success : function( response, text_status, xhr )
            {              
              prepare_graph_data(response, graph_element, live_nodes, callback)

              if (response.znode && response.znode.paging) {
                var parr = response.znode.paging.split('|');
                if (parr.length < 3) {
                  $( '#cloudGraphPaging' ).hide();
                  return;
                }
                
                var start = Math.max(parseInt(parr[0]),0);                  
                var prevEnabled = (start > 0);
                $('#cloudGraphPagingPrev').prop('disabled', !prevEnabled);
                if (prevEnabled)
                  $('#cloudGraphPagingPrev').show();                    
                else
                  $('#cloudGraphPagingPrev').hide();
                
                var rows = parseInt(parr[1])
                var total = parseInt(parr[2])
                $( '#cloudGraphPagingStart' ).val(start);
                $( '#cloudGraphPagingRows' ).val(rows);
                if (rows == -1)
                  $( '#cloudGraphPaging' ).hide();
                                  
                var filterType = parr.length > 3 ? parr[3] : '';
                if (filterType == '' || filterType == 'none') filterType = 'status';
                
                $( '#cloudGraphPagingFilterType' ).val(filterType);                  
                var filter = parr.length > 4 ? parr[4] : '';

                update_status_filter(filterType, filter);
                
                var page = Math.floor(start/rows)+1;
                var pages = Math.ceil(total/rows);
                var last = Math.min(start+rows,total);
                var nextEnabled = (last < total);                  
                $('#cloudGraphPagingNext').prop('disabled', !nextEnabled);
                if (nextEnabled)
                  $('#cloudGraphPagingNext').show();
                else
                  $('#cloudGraphPagingNext').hide();                    
                
                var status = (total > 0) 
                               ? 'Collections '+(start+1)+' - '+last+' of '+total+'. ' 
                               : 'No collections found.';
                $( '#cloudGraphPagingStatus' ).html(status);
              } else {
                $( '#cloudGraphPaging' ).hide();
              }            
            },
            error : function( xhr, text_status, error_thrown)
            {
            },
            complete : function( xhr, text_status )
            {
            }
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

};

var init_graph = function( graph_element )
{
  prepare_graph
  (
    graph_element,
    function( graph_element, graph_data, leaf_count )
    {
      generate_graph( graph_element, graph_data, leaf_count );
    }
  );
}

var init_rgraph = function( graph_element )
{
  prepare_graph
  (
    graph_element,
    function( graph_element, graph_data, leaf_count )
    {
      generate_rgraph( graph_element, graph_data, leaf_count );
    }
  );
}

var init_tree = function( tree_element )
{
  $.ajax
  (
    {
      url : app.config.solr_path + '/admin/zookeeper?wt=json',
      dataType : 'json',
      context : tree_element,
      beforeSend : function( xhr, settings )
      {
        this
          .show();
      },
      success : function( response, text_status, xhr )
      {
        var self = this;
                      
        $( '#tree', this )
          .jstree
          (
            {
              "plugins" : [ "json_data" ],
              "json_data" : {
                "data" : response.tree,
                "progressive_render" : true
              },
              "core" : {
                "animation" : 0
              }
            }
          )
          .jstree
          (
            'open_node',
            'li:first'
          );

        var tree_links = $( '#tree a', this );

        tree_links
          .die( 'click' )
          .live
          (
            'click',
            function( event )
            {
              $( 'a.active', $( this ).parents( '#tree' ) )
                .removeClass( 'active' );
                                  
              $( this )
                .addClass( 'active' );

              tree_element
                .addClass( 'show' );

              var file_content = $( '#file-content' );

              $( 'a.close', file_content )
                .die( 'click' )
                .live
                (
                  'click',
                  function( event )
                  {
                    $( '#tree a.active' )
                      .removeClass( 'active' );
                                      
                    tree_element
                      .removeClass( 'show' );

                    return false;
                  }
                );

              $.ajax
              (
                {
                  url : this.href,
                  dataType : 'json',
                  context : file_content,
                  beforeSend : function( xhr, settings )
                  {
                  },
                  success : function( response, text_status, xhr )
                  {
                    var props = [];
                    for( var key in response.znode.prop )
                    {
                      props.push
                      (
                        '<li><dl class="clearfix">' + "\n" +
                          '<dt>' + key.esc() + '</dt>' + "\n" +
                          '<dd>' + response.znode.prop[key].esc() + '</dd>' + "\n" +
                        '</dl></li>'
                      );
                    }

                    $( '#prop ul', this )
                      .empty()
                      .html( props.join( "\n" ) );

                    $( '#prop ul li:odd', this )
                      .addClass( 'odd' );

                    var data_element = $( '#data', this );

                    var highlight = false;
                    var data = '<em>Node "' + response.znode.path + '" has no utf8 Content</em>';

                    if( response.znode.data )
                    {
                      var classes = '';
                      var path = response.znode.path.split( '.' );
                      
                      if( 1 < path.length )
                      {
                        highlight = true;
                        classes = 'syntax language-' + path.pop().esc();
                      }

                      data = '<pre class="' + classes + '">'
                           + response.znode.data.esc()
                           + '</pre>';
                    }
                               

                    data_element
                        .show()
                        .html( data );

                    if( highlight )
                    {
                      hljs.highlightBlock( data_element.get(0) );
                    }
                    
                  },
                  error : function( xhr, text_status, error_thrown)
                  {
                  },
                  complete : function( xhr, text_status )
                  {
                  }
                }
              );

              return false;
            }
          );
      },
      error : zk_error,
      complete : function( xhr, text_status )
      {
      }
    }
  );
};

// updates the starting position for paged navigation
// and then rebuilds the graph based on the selected page
var update_start = function(direction, cloud_element) {
  var start = $( '#cloudGraphPagingStart' ).val();
  var rows = $( '#cloudGraphPagingRows' ).val();
  var startAt = start ? parseInt(start) : 0;
  var numRows = rows ? parseInt(rows) : 20;
  var newStart = Math.max(startAt + (rows * direction),0); 
  $( '#cloudGraphPagingStart' ).val(newStart);
  
  var graph_element = $( '#graph-content', cloud_element );
  $( '#canvas', graph_element).empty();
  init_graph( graph_element );  
};

// #/~cloud
sammy.get
(
  /^#\/(~cloud)$/,
  function( context )
  {
    var content_element = $( '#content' );

    $.get
    (
      'tpl/cloud.html',
      function( template )
      {
        content_element
          .html( template );

        var cloud_element = $( '#cloud', content_element );
        var navigation_element = $( '#menu #cloud' );

        init_debug( cloud_element );

        $( '.tree', navigation_element )
          .die( 'activate' )
          .live
          (
            'activate',
            function( event )
            {
              $( this ).addClass( 'active' );
              init_tree( $( '#tree-content', cloud_element ) );
            }
          );

        $( '.graph', navigation_element )
          .die( 'activate' )
          .live
          (
            'activate',
            function( event )
            {
              $( this ).addClass( 'active' );
              init_graph( $( '#graph-content', cloud_element ) );
              
              $('#cloudGraphPagingNext').click(function() {
                update_start(1, cloud_element);                  
              });
              
              $('#cloudGraphPagingPrev').click(function() {
                update_start(-1, cloud_element);                                    
              });              

              $('#cloudGraphPagingRows').change(function() {
                var rows = $( this ).val();
                if (!rows || rows == '')
                  $( this ).val("20");
                
                // ? restart the start position when rows changes?
                $( '#cloudGraphPagingStart' ).val(0);                  
                update_start(-1, cloud_element);                
              });              
              
              $('#cloudGraphPagingFilter').change(function() {
                var filter = $( this ).val();
                // reset the start position when the filter changes
                $( '#cloudGraphPagingStart' ).val(0);
                update_start(-1, cloud_element);
              });

              $( '#cloudGraphPagingStatusFilter' ).show();
              $( '#cloudGraphPagingFilter' ).hide();
              
              $('#cloudGraphPagingFilterType').change(function() {
                update_status_filter($( this ).val(), '');
              });
              
              $('#cloudGraphPagingStatusFilter').change(function() {
                // just reset the paged navigation controls based on this update
                $( '#cloudGraphPagingStart' ).val(0);                  
                update_start(-1, cloud_element);                                    
              });
              
            }
          );

        $( '.rgraph', navigation_element )
          .die( 'activate' )
          .live
          (
            'activate',
            function( event )
            {
              $( "#cloudGraphPaging" ).hide(); // TODO: paging for rgraph too
              
              $( this ).addClass( 'active' );
              init_rgraph( $( '#graph-content', cloud_element ) );
            }
          );

        $.ajax
        (
          {
            url : app.config.solr_path + '/admin/zookeeper?wt=json',
            dataType : 'json',
            context : cloud_element,
            success : function( response, text_status, xhr )
            {
              $( 'a[href="' + context.path + '"]', navigation_element )
                .trigger( 'activate' );
            },
            error : zk_error
          }
        );
        
      }
    );
  }
);
