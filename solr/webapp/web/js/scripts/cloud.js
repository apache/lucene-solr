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

var core_basepath = null;

var init_debug = function( cloud_element )
{
  var debug_element = $( '#debug', cloud_element );
  var debug_button = $( '.dump a', cloud_element );

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
        debug_button.hide();
        debug_element.show();

        $.ajax
        (
          {
            url : core_basepath + '/zookeeper?wt=json&dump=true',
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

        debug_button.show();
        debug_element.hide();
      }
    );
};

var generate_graph = function( graph_element, graph_data )
{
  var w = 900,
      h = 300;

  var tree = d3.layout.tree()
      .size([h, w - 400]);

  var diagonal = d3.svg.diagonal()
      .projection(function(d) { return [d.y, d.x]; });

  var vis = d3.select("#canvas").append("svg")
      .attr("width", w)
      .attr("height", h)
    .append("g")
      .attr("transform", "translate(100, 0)");

  var nodes = tree.nodes(graph_data);

  var link = vis.selectAll("path.link")
      .data(tree.links(nodes))
    .enter().append("path")
      .attr("class", "link")
      .attr("d", diagonal);

  var node = vis.selectAll("g.node")
      .data(nodes)
    .enter().append("g")
      .attr("class", "node")
      .attr("transform", function(d) { return "translate(" + d.y + "," + d.x + ")"; })

  node.append("circle")
      .attr("r", 4.5);

  node.append("text")
      .attr("dx", function(d) { return d.children ? -8 : 8; })
      .attr("dy", 3)
      .attr("text-anchor", function(d) { return d.children ? "end" : "start"; })
      .text(function(d) { return d.name; });

  /*
  var r = 860 / 2;

  var tree = d3.layout.tree()
      .size([360, r - 120])
      .separation(function(a, b) { return (a.parent == b.parent ? 1 : 2) / a.depth; });

  var diagonal = d3.svg.diagonal.radial()
      .projection(function(d) { return [d.y, d.x / 180 * Math.PI]; });

  var vis = d3.select("#canvas").append("svg")
      .attr("width", r * 2)
      .attr("height", r * 2 - 150)
    .append("g")
      .attr("transform", "translate(" + r + "," + r + ")");

  var nodes = tree.nodes(graph_data);

  var link = vis.selectAll("path.link")
      .data(tree.links(nodes))
    .enter().append("path")
      .attr("class", "link")
      .attr("d", diagonal);

  var node = vis.selectAll("g.node")
      .data(nodes)
    .enter().append("g")
      .attr("class", "node")
      .attr("transform", function(d) { return "rotate(" + (d.x - 90) + ")translate(" + d.y + ")"; })

  node.append("circle")
      .attr("r", 4.5);

  node.append("text")
      .attr("dx", function(d) { return d.x < 180 ? 8 : -8; })
      .attr("dy", ".31em")
      .attr("text-anchor", function(d) { return d.x < 180 ? "start" : "end"; })
      .attr("transform", function(d) { return d.x < 180 ? null : "rotate(180)"; })
      .text(function(d) { return d.name; });
  //*/

};

var init_graph = function( graph_element )
{
  $.ajax
  (
    {
      url : core_basepath + '/zookeeper?wt=json&detail=true&path=%2Fclusterstate.json',
      dataType : 'json',
      context : graph_element,
      beforeSend : function( xhr, settings )
      {
        this
          .show();
      },
      success : function( response, text_status, xhr )
      {
        var state = null;
        eval( 'state = ' + response.znode.data + ';' );
        
        var collections = [];
        for( var c in state )
        {
          var shards = [];
          for( var s in state[c] )
          {
            var nodes = [];
            for( var n in state[c][s] )
            {
              var node = {
                id: state[c][s][n].node_name,
                name: state[c][s][n].base_url,
                data: {
                  type : 'node',
                  state : state[c][s][n].state,
                  leader : 'true' === state[c][s][n].leader
                }
              };
              nodes.push( node );
            }

            var shard = {
              id: s,
              name: s,
              data: {
                type : 'shard',
              },
              children: nodes
            };
            shards.push( shard );
          }

          var collection = {
            id: c,
            name: c,
            data: {
              type : 'collection',
            },
            children: shards
          };
          collections.push( collection );
        }

        var graph_data = collections.shift();
        generate_graph( graph_element, graph_data );
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

var init_tree = function( tree_element )
{
  $.ajax
  (
    {
      url : core_basepath + '/zookeeper?wt=json',
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

                    if( 0 !== parseInt( response.znode.prop.children_count ) )
                    {
                      data_element.hide();
                    }
                    else
                    {
                      var highlight = false;
                      var data = '<em>File "' + response.znode.path + '" has no Content</em>';

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
      error : function( xhr, text_status, error_thrown )
      {
        var message = 'Loading of <code>' + app.config.zookeeper_path + '</code> failed with "' + text_status + '" '
                    + '(<code>' + error_thrown.message + '</code>)';

        if( 200 !== xhr.status )
        {
          message = 'Loading of <code>' + app.config.zookeeper_path + '</code> failed with HTTP-Status ' + xhr.status + ' ';
        }

        this
          .html( '<div class="block" id="error">' + message + '</div>' );
      },
      complete : function( xhr, text_status )
      {
      }
    }
  );
};

// #/~cloud
sammy.get
(
  /^#\/~(cloud)$/,
  function( context )
  {
    core_basepath = $( 'li[data-basepath]', app.menu_element ).attr( 'data-basepath' );
    var content_element = $( '#content' );

    $.get
    (
      'tpl/cloud.html',
      function( template )
      {
        content_element
          .html( template );

        var cloud_element = $( '#cloud', content_element );
        var navigation_element = $( '#navigation', content_element );

        init_debug( cloud_element );

        $( '.tree', navigation_element )
          .die( 'activate' )
          .live
          (
            'activate',
            function( event )
            {
              $( this ).addClass( 'current' );
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
              $( this ).addClass( 'current' );
              init_graph( $( '#graph-content', cloud_element ) );
            }
          );

        $( 'a[href="' + context.path + '"]', navigation_element )
          .trigger( 'activate' );
        
      }
    );
  }
);