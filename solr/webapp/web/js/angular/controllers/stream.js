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
solrAdminApp.controller('StreamController',
  function($scope, $routeParams, $location, Query, Constants) {

    $scope.resetMenu("stream", Constants.IS_COLLECTION_PAGE);

    $scope.stream = {
      wt: 'json',
      expr: $scope.expr,
      indent: 'on'
    };
    $scope.qt = "stream";
    $scope.doExplanation = false

    $scope.doStream = function() {

      var params = {};
      params.core = $routeParams.core;
      params.handler = $scope.qt;
      params.expr = [$scope.expr]
      if($scope.doExplanation){
        params.explain = [$scope.doExplanation]
      }

      $scope.lang = "json";
      $scope.response = null;
      $scope.url = "";

      var url = Query.url(params);

      Query.query(params, function(data) {

        var jsonData = JSON.parse(data.toJSON().data);
        if (undefined != jsonData["explanation"]) {
          $scope.showExplanation = true;

          streamGraphSubController($scope, jsonData["explanation"])
          delete jsonData["explanation"]
        } else {
          $scope.showExplanation = false;
        }

        data.data = JSON.stringify(jsonData,null,2);

        $scope.lang = "json";
        $scope.response = data;
        $scope.url = url;
        $scope.hostPortContext = $location.absUrl().substr(0,$location.absUrl().indexOf("#")); // For display only

      });
    };

    if ($location.search().expr) {
      $scope.expr = $location.search()["expr"];
      $scope.doStream();
    }

  }
);

var streamGraphSubController = function($scope, explanation) {
  $scope.showGraph = true;
  $scope.pos = 0;
  $scope.rows = 8;

  $scope.resetGraph = function() {
    $scope.pos = 0;
    $scope.initGraph();
  }

  $scope.initGraph = function(explanation) {

    data = explanation

    var leafCount = 0;
    var maxDepth = 0;
    var rootNode = {};

    leafCount = 0;

    let recurse = function(dataNode, depth) {

      if (depth > maxDepth) {
        maxDepth = depth;
      }

      let graphNode = {
        name: dataNode.expressionNodeId,
        implementingClass: 'unknown',
        data: {}
      };

      ["expressionNodeId", "expressionType", "functionName", "implementingClass", "expression", "note", "helpers"].forEach(function(key) {
        graphNode.data[key] = dataNode[key];
      });

      if (dataNode.children && dataNode.children.length > 0) {
        graphNode.children = [];
        dataNode.children.forEach(function(n) {
          graphNode.children.push(recurse(n, depth + 1));
        });
      } else {
        ++leafCount;
      }

      return graphNode;
    }

    $scope.showPaging = false;
    $scope.isRadial = false;
    $scope.explanationData = recurse(data, 1);

    $scope.depth = maxDepth + 1;
    $scope.leafCount = leafCount;
  };

  $scope.initGraph(explanation);
};

solrAdminApp.directive('explanationGraph', function(Constants) {
  return {
    restrict: 'EA',
    scope: {
      data: "=",
      leafCount: "=",
      depth: "="
    },
    link: function(scope, element, attrs) {
      
      var helper_path_class = function(p) {
        var classes = ['link'];

        return classes.join(' ');
      };

      var helper_node_class = function(d) {
        var classes = ['node'];

        if (d.data && d.data.expressionType) {
          classes.push(d.data.expressionType);
        }

        return classes.join(' ');
      };

      var helper_node_text = function(d) {
        if (d.data && d.data.functionName) {
          return d.data.functionName;
        }

        return d.name
      };

      var helper_tooltip = function(d) {

        return [
          "Function: " + d.data.functionName,
          "Type: " + d.data.expressionType,
          "Class: " + d.data.implementingClass.replace("org.apache.solr.client.solrj.io", "o.a.s.c.s.i"),
          "=============",
          d.data.expression
        ].join("\n");
      }

      scope.$watch("data", function(newValue, oldValue) {
        if (newValue) {
          flatGraph(element, scope.data, scope.depth, scope.leafCount);
        }
      });

      var flatGraph = function(element, graphData, depth, leafCount) {
        var w = 100 + (depth * 100),
          h = leafCount * 40;

        var tree = d3.layout.tree().size([h, w]);

        var diagonal = d3.svg.diagonal().projection(function(d) {
          return [d.y * .7, d.x];
        });

        d3.select('#canvas', element).html('');
        var vis = d3.select('#canvas', element).append('svg')
          .attr('width', w)
          .attr('height', h)
          .append('g')
          .attr('transform', 'translate(25, 0)');

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
          .attr('transform', function(d) {
            return 'translate(' + d.y * .7 + ',' + d.x + ')';
          })

        node.append('circle')
          .attr('r', 4.5);

        node.append('title')
          .text(helper_tooltip);

        node.append('text')
          .attr('dx', function(d) {
            return 8;
          })
          .attr('dy', function(d) {
            return 5;
          })
          .attr('text-anchor', function(d) {
            return 'start';
          })
          .text(helper_node_text)
      };
    }
  };
})
