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
    function($scope, $location, Zookeeper, Constants, Collections, SystemAll, MetricsAll) {

        $scope.showDebug = false;

        $scope.$on("cloud-dump", function(event) {
            $scope.showDebug = true;
        });

        $scope.closeDebug = function() {
            $scope.showDebug = false;
        };

        var view = $location.search().view ? $location.search().view : "nodes";
        if (view == "tree") {
            $scope.resetMenu("cloud-tree", Constants.IS_ROOT_PAGE);
            treeSubController($scope, Zookeeper);
        } else if (view == "rgraph") {
            $scope.resetMenu("cloud-rgraph", Constants.IS_ROOT_PAGE);
            graphSubController($scope, Zookeeper, true);
        } else if (view == "graph") {
            $scope.resetMenu("cloud-graph", Constants.IS_ROOT_PAGE);
            graphSubController($scope, Zookeeper, false);
        } else if (view == "nodes") {
            $scope.resetMenu("cloud-nodes", Constants.IS_ROOT_PAGE);
            nodesSubController($scope, Zookeeper, Collections, SystemAll, MetricsAll);
        }
    }
);

function getOrCreateObj(name, object) {
    if (name in object) {
        entry = object[name];
    } else {
        entry = {};
        object[name] = entry;
    }
    return entry;
}

function getOrCreateList(name, object) {
    if (name in object) {
        entry = object[name];
    } else {
        entry = [];
        object[name] = entry;
    }
    return entry;
}

function ensureInList(string, list) {
    if (list.indexOf(string) === -1) {
        list.push(string);
    }
}

var nodesSubController = function($scope, Zookeeper, Collections, SystemAll, MetricsAll) {
    $scope.showNodes = true;
    $scope.showTree = false;
    $scope.showGraph = false;
    $scope.showData = false;
    
    Collections.status(function (data) {
        var nodes = {};
        var hosts = {};
        var live_nodes = [];
        
        // Fetch cluster state from collections API and invert to a nodes structure
        for (var name in data.cluster.collections) {
            var collection = data.cluster.collections[name];
            collection.name = name;
            var shards = collection.shards;
            collection.shards = [];
            for (var shardName in shards) {
                var shard = shards[shardName];
                shard.name = shardName;
                shard.collection = collection.name;
                var replicas = shard.replicas;
                shard.replicas = [];
                for (var replicaName in replicas) {
                    var core = replicas[replicaName];
                    core.name = replicaName;
                    core.collection = collection.name;
                    core.shard = shard.name;
                    core.shard_state = shard.state;
                    
                    var node_name = core['node_name']; 
                    var node = getOrCreateObj(node_name, nodes);
                    var cores = getOrCreateList("cores", node);
                    cores.push(core);
                    node['base_url'] = core.base_url;
                    var collections = getOrCreateList("collections", node);
                    ensureInList(core.collection, collections);
                    var host_name = node_name.split(":")[0];
                    var host = getOrCreateObj(host_name, hosts);
                    var host_nodes = getOrCreateList("nodes", host);
                    ensureInList(node_name, host_nodes);
                }
            }
        }

        live_nodes = data.cluster.live_nodes;
        for (n in data.cluster.live_nodes) {
            if (!(data.cluster.live_nodes[n] in nodes)) {
                nodes[data.cluster.live_nodes[n]] = {};
            }
        }
        
        SystemAll.get(function(systemResponse) {
          for (var node in systemResponse) {
              console.log("Checking node " + node + " exist in " + nodes);
              if (node in nodes) {
                var s = systemResponse[node]; 
                nodes[node]['system'] = s;
                var memTotal = s.system.totalPhysicalMemorySize; 
                var memFree = s.system.freePhysicalMemorySize;
                var memPercentage = Math.floor((memTotal - memFree) / memTotal * 100); 
                nodes[node]['memUsedPct'] = memPercentage + "%";
                nodes[node]['memTotal'] = Math.floor(memTotal / 1024 / 1024 / 1024) + "Gb";
                nodes[node]['memFree'] = Math.floor(memFree / 1024 / 1024 / 1024) + "Gb";                 

                var heapTotal = s.jvm.memory.raw.total; 
                var heapFree = s.jvm.memory.raw.free;
                var heapPercentage = Math.floor((heapTotal - heapFree) / heapTotal * 100); 
                nodes[node]['heapUsedPct'] = heapPercentage + "%";
                nodes[node]['heapTotal'] = Math.floor(heapTotal / 1024 / 1024 / 1024) + "Gb";
                nodes[node]['heapFree'] = Math.floor(heapFree / 1024 / 1024 / 1024) + "Gb";
                
                var jvmUptime = s.jvm.jmx.upTimeMS / 1000; // Seconds
                nodes[node]['jvmUptime'] = secondsForHumans(jvmUptime);
                nodes[node]['jvmUptimeSec'] = jvmUptime;
                
                nodes[node]['loadAvg'] = Math.round(s.system.systemLoadAverage * 100) / 100;
                nodes[node]['cpuPct'] = Math.ceil(s.system.processCpuLoad) + "%";
              } else {
                  console.log("Skipping node " + node);
              }
          }
        });

        MetricsAll.get(function(metricsResponse) {
          for (var node in metricsResponse) {
            console.log("Checking node " + node + " exist in " + nodes);
            if (node in nodes) {
                var m = metricsResponse[node]; 
                nodes[node]['metrics'] = m;
                var diskTotal = m.metrics['solr.node']['CONTAINER.fs.totalSpace']; 
                var diskFree = m.metrics['solr.node']['CONTAINER.fs.usableSpace'];
                var diskPercentage = Math.floor((diskTotal - diskFree) / diskTotal * 100); 
                nodes[node]['diskUsedPct'] = diskPercentage + "%";
                nodes[node]['diskTotal'] = Math.floor(diskTotal / 1024 / 1024 / 1024) + "Gb";
                nodes[node]['diskFree'] = Math.floor(diskFree / 1024 / 1024 / 1024) + "Gb";
                
                var r = m.metrics['solr.jetty']['org.eclipse.jetty.server.handler.DefaultHandler.get-requests'];
                nodes[node]['req'] = r.count;
                nodes[node]['req1minRate'] = Math.floor(r['1minRate'] * 100) / 100;
                nodes[node]['req5minRate'] = Math.floor(r['5minRate'] * 100) / 100;
                nodes[node]['req15minRate'] = Math.floor(r['15minRate'] * 100) / 100;
                nodes[node]['reqp75_ms'] = Math.floor(r['p75_ms']);
                nodes[node]['reqp95_ms'] = Math.floor(r['p95_ms']);
                nodes[node]['reqp99_ms'] = Math.floor(r['p99_ms']);
                
                nodes[node]['gcMajorCount'] = m.metrics['solr.jvm']['gc.ConcurrentMarkSweep.count'];
                nodes[node]['gcMajorTime'] = m.metrics['solr.jvm']['gc.ConcurrentMarkSweep.time'];
                nodes[node]['gcMinorCount'] = m.metrics['solr.jvm']['gc.ParNew.count'];  
                nodes[node]['gcMinorTime'] = m.metrics['solr.jvm']['gc.ParNew.time'];
            } else {
              console.log("Skipping node " + node);
            }
          }
        });
        
        $scope.nodes = nodes;
        $scope.hosts = hosts;
        $scope.live_nodes = live_nodes;        

        $scope.Math = window.Math;
        
        console.log("Nodes=" + JSON.stringify($scope.nodes, undefined, 2));
        // console.log("Livenodes=" + JSON.stringify(live_nodes, undefined, 2));
    });
};                 

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
            } else {
              var lastPathElement = data.znode.path.split( '/' ).pop();
              if (lastPathElement == "managed-schema") {
                  $scope.lang = "xml";
              }
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

/**
 * Translates seconds into human readable format of seconds, minutes, hours, days, and years
 * 
 * @param  {number} seconds The number of seconds to be processed
 * @return {string}         The phrase describing the the amount of time
 */
function secondsForHumans ( seconds ) {
    var levels = [
        [Math.floor(seconds / 31536000), 'y'],
        [Math.floor((seconds % 31536000) / 86400), 'd'],
        [Math.floor(((seconds % 31536000) % 86400) / 3600), 'h'],
        [Math.floor((((seconds % 31536000) % 86400) % 3600) / 60), 'm']
    ];
    var returntext = '';

    for (var i = 0, max = levels.length; i < max; i++) {
        if ( levels[i][0] === 0 ) continue;
        returntext += ' ' + levels[i][0] + levels[i][1];
    }
    return returntext.trim() === '' ? '0m' : returntext.trim();
}

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
                            var shard_status = state[c].shards[s].state;
                            shard_status = shard_status == 'inactive' ? 'shard-inactive' : shard_status;
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

                                var replica_status = replica.state;

                                if (!live_nodes[replica.node_name]) {
                                    replica_status = 'gone';
                                } else if(shard_status=='shard-inactive') {
                                    replica_status += ' ' + shard_status;
                                }

                                var node = {
                                    name: uri,
                                    data: {
                                        type: 'node',
                                        state: replica_status,
                                        leader: 'true' === replica.leader,
                                        uri: uri_parts
                                    }
                                };
                                nodes.push(node);
                            }

                            var shard = {
                                name: shard_status == "shard-inactive" ? s + ' (inactive)' : s,
                                data: {
                                    type: 'shard',
                                    state: shard_status
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
    $scope.pos = 0;   
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
                    if(!(d.data.type=='shard' && d.data.state=='active')){
                        classes.push(d.data.state);
                    }
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


            function setNodeNavigationBehavior(node, view){
                node
                .attr('data-href', function (d) {
                    if (d.type == "node"){
                        return getNodeUrl(d, view);
                    }
                    else{
                        return "";
                    }
                })
                .on('click', function(d) {
                    if (d.data.type == "node"){
                        location.href = getNodeUrl(d, view);
                    }
                });
            }

            function getNodeUrl(d, view){
                var url = d.name + Constants.ROOT_URL + "#/~cloud";
                if (view != undefined){
                    url += "?view=" + view;
                }
                return url;
            }

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
                    .text(helper_node_text);

                setNodeNavigationBehavior(node);
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
                    .text(helper_node_text);

                setNodeNavigationBehavior(node, "rgraph");
            }
        }
    };
})
