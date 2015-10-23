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

solrAdminApp.controller('CloudController',
    function($scope, $location, Zookeeper, Constants) {

        $scope.showDebug = false;

        $scope.$on("cloud-dump", function(event) {
            $scope.showDebug = true;
        });

        $scope.closeDebug = function() {
            $scope.showDebug = false;
        }

        var view = $location.search().view ? $location.search().view : "graph";
        if (view == "tree") {
            $scope.resetMenu("cloud-tree", Constants.IS_ROOT_PAGE);
            treeSubController($scope, Zookeeper);
        } else if (view == "rgraph") {
            $scope.resetMenu("cloud-rgraph", Constants.IS_ROOT_PAGE);
            graphSubController($scope, Zookeeper, true);
        } else if (view == "graph") {
            $scope.resetMenu("cloud-graph", Constants.IS_ROOT_PAGE);
            graphSubController($scope, Zookeeper, false);
        }
    }
);

var treeSubController = function($scope, Zookeeper) {
    $scope.showTree = true;
    $scope.showGraph = false;
    $scope.tree = {};
    $scope.showData = false;

    $scope.showTreeLink = function(link) {
        var path = decodeURIComponent(link.replace(/.*[\\?&]path=([^&#]*).*/, "$1"));
        Zookeeper.detail({path: path}, function(data) {
            $scope.znode = data.znode;
            var path = data.znode.path.split( '.' );
            if(path.length >1) {
              $scope.lang = path.pop();
            }
            $scope.showData = true;
        });
    };

    $scope.hideData = function() {
        $scope.showData = false;
    };

    $scope.initTree = function() {
      Zookeeper.simple(function(data) {
        $scope.tree = data.tree;
      });
    };

    $scope.initTree();
};

var graphSubController = function ($scope, Zookeeper, isRadial) {
    $scope.showTree = false;
    $scope.showGraph = true;

    $scope.filterType = "status";

    $scope.helperData = {
        protocol: [],
        host: [],
        hostname: [],
        port: [],
        pathname: []
    };

    $scope.next = function() {
        $scope.pos += $scope.rows;
        $scope.initGraph();
    }

    $scope.previous = function() {
        $scope.pos = Math.max(0, $scope.pos - $scope.rows);
        $scope.initGraph();
    }

    $scope.resetGraph = function() {
        $scope.pos = 0;
        $scope.initGraph();
    }

    $scope.initGraph = function() {
        Zookeeper.liveNodes(function (data) {
            var live_nodes = {};
            for (var c in data.tree[0].children) {
                live_nodes[data.tree[0].children[c].data.title] = true;
            }

            var params = {view: "graph"};
            if ($scope.rows) {
                params.start = $scope.pos;
                params.rows = $scope.rows;
            }

            var filter = ($scope.filterType=='status') ? $scope.pagingStatusFilter : $scope.pagingFilter;

            if (filter) {
                params.filterType = $scope.filterType;
                params.filter = filter;
            }

            Zookeeper.clusterState(params, function (data) {
                    eval("var state=" + data.znode.data); // @todo fix horrid means to parse JSON

                    var leaf_count = 0;
                    var graph_data = {
                        name: null,
                        children: []
                    };

                    for (var c in state) {
                        var shards = [];
                        for (var s in state[c].shards) {
                            var nodes = [];
                            for (var n in state[c].shards[s].replicas) {
                                leaf_count++;
                                var replica = state[c].shards[s].replicas[n]

                                var uri = replica.base_url;
                                var parts = uri.match(/^(\w+:)\/\/(([\w\d\.-]+)(:(\d+))?)(.+)$/);
                                var uri_parts = {
                                    protocol: parts[1],
                                    host: parts[2],
                                    hostname: parts[3],
                                    port: parseInt(parts[5] || 80, 10),
                                    pathname: parts[6]
                                };

                                $scope.helperData.protocol.push(uri_parts.protocol);
                                $scope.helperData.host.push(uri_parts.host);
                                $scope.helperData.hostname.push(uri_parts.hostname);
                                $scope.helperData.port.push(uri_parts.port);
                                $scope.helperData.pathname.push(uri_parts.pathname);

                                var status = replica.state;

                                if (!live_nodes[replica.node_name]) {
                                    status = 'gone';
                                }

                                var node = {
                                    name: uri,
                                    data: {
                                        type: 'node',
                                        state: status,
                                        leader: 'true' === replica.leader,
                                        uri: uri_parts
                                    }
                                };
                                nodes.push(node);
                            }

                            var shard = {
                                name: s,
                                data: {
                                    type: 'shard'
                                },
                                children: nodes
                            };
                            shards.push(shard);
                        }

                        var collection = {
                            name: c,
                            data: {
                                type: 'collection'
                            },
                            children: shards
                        };
                        graph_data.children.push(collection);
                    }

                    $scope.helperData.protocol = $.unique($scope.helperData.protocol);
                    $scope.helperData.host = $.unique($scope.helperData.host);
                    $scope.helperData.hostname = $.unique($scope.helperData.hostname);
                    $scope.helperData.port = $.unique($scope.helperData.port);
                    $scope.helperData.pathname = $.unique($scope.helperData.pathname);

                    if (!isRadial && data.znode && data.znode.paging) {
                        $scope.showPaging = true;

                        var parr = data.znode.paging.split('|');
                        if (parr.length < 3) {
                            $scope.showPaging = false;
                        } else {
                            $scope.start = Math.max(parseInt(parr[0]), 0);
                            $scope.prevEnabled = ($scope.start > 0);
                            $scope.rows = parseInt(parr[1]);
                            $scope.total = parseInt(parr[2]);

                            if ($scope.rows == -1) {
                                $scope.showPaging = false;
                            } else {
                                var filterType = parr.length > 3 ? parr[3] : '';

                                if (filterType == '' || filterType == 'none') filterType = 'status';

                                +$('#cloudGraphPagingFilterType').val(filterType);

                                var filter = parr.length > 4 ? parr[4] : '';
                                var page = Math.floor($scope.start / $scope.rows) + 1;
                                var pages = Math.ceil($scope.total / $scope.rows);
                                $scope.last = Math.min($scope.start + $scope.rows, $scope.total);
                                $scope.nextEnabled = ($scope.last < $scope.total);
                            }
                        }
                    }
                    else {
                        $scope.showPaging = false;
                    }
                    $scope.graphData = graph_data;
                    $scope.leafCount = leaf_count;
                    $scope.isRadial = isRadial;
                });
        });
    };

    $scope.initGraph();
};

solrAdminApp.directive('graph', function(Constants) {
    return {
        restrict: 'EA',
        scope: {
            data: "=",
            leafCount: "=",
            helperData: "=",
            isRadial: "="
        },
        link: function (scope, element, attrs) {
            var helper_path_class = function (p) {
                var classes = ['link'];
                classes.push('lvl-' + p.target.depth);

                if (p.target.data && p.target.data.leader) {
                    classes.push('leader');
                }

                if (p.target.data && p.target.data.state) {
                    classes.push(p.target.data.state);
                }

                return classes.join(' ');
            };

            var helper_node_class = function (d) {
                var classes = ['node'];
                classes.push('lvl-' + d.depth);

                if (d.data && d.data.leader) {
                    classes.push('leader');
                }

                if (d.data && d.data.state) {
                    classes.push(d.data.state);
                }

                return classes.join(' ');
            };

            var helper_node_text = function (d) {
                if (!d.data || !d.data.uri) {
                    return d.name;
                }

                var name = d.data.uri.hostname;

                if (1 !== scope.helperData.protocol.length) {
                    name = d.data.uri.protocol + '//' + name;
                }

                if (1 !== scope.helperData.port.length) {
                    name += ':' + d.data.uri.port;
                }

                if (1 !== scope.helperData.pathname.length) {
                    name += d.data.uri.pathname;
                }

                return name;
            };

            scope.$watch("data", function(newValue, oldValue) {
                if (newValue) {
                    if (scope.isRadial) {
                        radialGraph(element, scope.data, scope.leafCount);
                    } else {
                        flatGraph(element, scope.data, scope.leafCount);
                    }
                }
            });

            var flatGraph = function(element, graphData, leafCount) {
                var w = element.width(),
                    h = leafCount * 20;

                var tree = d3.layout.tree().size([h, w - 400]);

                var diagonal = d3.svg.diagonal().projection(function (d) {
                    return [d.y, d.x];
                });

                d3.select('#canvas', element).html('');
                var vis = d3.select('#canvas', element).append('svg')
                    .attr('width', w)
                    .attr('height', h)
                    .append('g')
                    .attr('transform', 'translate(100, 0)');

                var nodes = tree.nodes(graphData);

                var link = vis.selectAll('path.link')
                    .data(tree.links(nodes))
                    .enter().append('path')
                    .attr('class', helper_path_class)
                    .attr('d', diagonal);

                var node = vis.selectAll('g.node')
                    .data(nodes)
                    .enter().append('g')
                    .attr('class', helper_node_class)
                    .attr('transform', function (d) {
                        return 'translate(' + d.y + ',' + d.x + ')';
                    })

                node.append('circle')
                    .attr('r', 4.5);

                node.append('text')
                    .attr('dx', function (d) {
                        return 0 === d.depth ? -8 : 8;
                    })
                    .attr('dy', function (d) {
                        return 5;
                    })
                    .attr('text-anchor', function (d) {
                        return 0 === d.depth ? 'end' : 'start';
                    })
                    .attr('data-href', function (d) {
                        return d.name + Constants.ROOT_URL + "#/~cloud";
                    })
                    .text(helper_node_text)
                    .on('click', function(d,i) {
                        location.href = d.name+Constants.ROOT_URL+"#/~cloud";
                    });
            };

            var radialGraph = function(element, graphData, leafCount) {
                var max_val = Math.min(element.width(), $('body').height())
                var r = max_val / 2;

                var cluster = d3.layout.cluster()
                    .size([360, r - 160]);

                var diagonal = d3.svg.diagonal.radial()
                    .projection(function (d) {
                        return [d.y, d.x / 180 * Math.PI];
                    });

                d3.select('#canvas', element).html('');
                var vis = d3.select('#canvas').append('svg')
                    .attr('width', r * 2)
                    .attr('height', r * 2)
                    .append('g')
                    .attr('transform', 'translate(' + r + ',' + r + ')');

                var nodes = cluster.nodes(graphData);

                var link = vis.selectAll('path.link')
                    .data(cluster.links(nodes))
                    .enter().append('path')
                    .attr('class', helper_path_class)
                    .attr('d', diagonal);

                var node = vis.selectAll('g.node')
                    .data(nodes)
                    .enter().append('g')
                    .attr('class', helper_node_class)
                    .attr('transform', function (d) {
                        return 'rotate(' + (d.x - 90) + ')translate(' + d.y + ')';
                    })

                node.append('circle')
                    .attr('r', 4.5);

                node.append('text')
                    .attr('dx', function (d) {
                        return d.x < 180 ? 8 : -8;
                    })
                    .attr('dy', '.31em')
                    .attr('text-anchor', function (d) {
                        return d.x < 180 ? 'start' : 'end';
                    })
                    .attr('transform', function (d) {
                        return d.x < 180 ? null : 'rotate(180)';
                    })
                    .attr('data-href', function (d) {
                        return d.name;
                    })
                    .text(helper_node_text)
                    .on('click', function(d,i) {
                        location.href = d.name+Constants.ROOT_URL+"#/~cloud";
                    });
            }
        }
    };
})

/*

========================
var init_debug = function( cloud_element )
{
  var debug_element = $( '#debug', cloud_element );
  var debug_button = $( '#menu #cloud .dump a' );

  var clipboard_element = $( '.clipboard', debug_element );
  var clipboard_button = $( 'a', clipboard_element );

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

            url : app.config.solr_path + '/zookeeper?wt=json&dump=true',
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

*/
