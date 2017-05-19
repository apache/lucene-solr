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

// @todo test optimize (delete stuff, watch button appear, test button/form)
solrAdminApp.controller('CoreAdminController',
    function($scope, $routeParams, $location, $timeout, $route, Cores, Update, Constants){
      $scope.resetMenu("cores", Constants.IS_ROOT_PAGE);
      $scope.selectedCore = $routeParams.corename; // use 'corename' not 'core' to distinguish from /solr/:core/
      $scope.refresh = function() {
        Cores.get(function(data) {
          var coreCount = 0;
          var cores = data.status;
          for (_obj in cores) coreCount++;
          $scope.hasCores = coreCount >0;
          if (!$scope.selectedCore && coreCount==0) {
            $scope.showAddCore();
            return;
          } else if (!$scope.selectedCore) {
            for (firstCore in cores) break;
            $scope.selectedCore = firstCore;
            $location.path("/~cores/" + $scope.selectedCore).replace();
          }
          $scope.core = cores[$scope.selectedCore];
          $scope.corelist = [];
          $scope.swapCorelist = [];
          for (var core in cores) {
             $scope.corelist.push(cores[core]);
            if (cores[core] != $scope.core) {
              $scope.swapCorelist.push(cores[core]);
            }
          }
          if ($scope.swapCorelist.length>0) {
            $scope.swapOther = $scope.swapCorelist[0].name;
          }
        });
      };
      $scope.showAddCore = function() {
        $scope.hideAll();
        $scope.showAdd = true;
        $scope.newCore = {
          name: "new_core",
          dataDir: "data",
          instanceDir: "new_core",
          config: "solrconfig.xml",
          schema: "schema.xml",
          collection: "",
          shard: ""
        };
      };

      $scope.addCore = function() {
        if (!$scope.newCore.name) {
          $scope.addMessage = "Please provide a core name";
        } else if (false) { //@todo detect whether core exists
          $scope.AddMessage = "A core with that name already exists";
        } else {
          var params = {
            name: $scope.newCore.name,
            instanceDir: $scope.newCore.instanceDir,
            config: $scope.newCore.config,
            schema: $scope.newCore.schema,
            dataDir: $scope.newCore.dataDir
          };
          if ($scope.isCloud) {
            params.collection = $scope.newCore.collection;
            params.shard = $scope.newCore.shard;
          }
          Cores.add(params, function(data) {
            $location.path("/~cores/" + $scope.newCore.name);
            $scope.cancelAddCore();
          });
        }
      };

      $scope.cancelAddCore = function() {
        delete $scope.addMessage;
        $scope.showAdd = false
      };

      $scope.unloadCore = function() {
        var answer = confirm( 'Do you really want to unload Core "' + $scope.selectedCore + '"?' );
        if( !answer ) return;
        Cores.unload({core: $scope.selectedCore}, function(data) {
          $location.path("/~cores");
        });
      };

      $scope.showRenameCore = function() {
        $scope.hideAll();
        $scope.showRename = true;
      };

      $scope.renameCore = function() {
        if (!$scope.other) {
          $scope.renameMessage = "Please provide a new name for the " + $scope.selectedCore + " core";
        } else if ($scope.other == $scope.selectedCore) {
          $scope.renameMessage = "New name must be different from the current one";
        } else {
          Cores.rename({core:$scope.selectedCore, other: $scope.other}, function(data) {
            $location.path("/~cores/" + $scope.other);
            $scope.cancelRename();
          });
        }
      };

      $scope.cancelRenameCore = function() {
        $scope.showRename = false;
        delete $scope.renameMessage;
        $scope.other = "";
      };

      $scope.showSwapCores = function() {
        $scope.hideAll();
        $scope.showSwap = true;
      };

      $scope.swapCores = function() {
        if (!$scope.swapOther) {
          $scope.swapMessage = "Please select a core to swap with";
        } else if ($scope.swapOther == $scope.selectedCore) {
          $scope.swapMessage = "Cannot swap with the same core";
        } else {
          Cores.swap({core: $scope.selectedCore, other: $scope.swapOther}, function(data) {
            $location.path("/~cores/" + $scope.swapOther);
            delete $scope.swapOther;
            $scope.cancelSwapCores();
          });
        }
      };

      $scope.cancelSwapCores = function() {
        delete $scope.swapMessage;
        $scope.showSwap = false;
      }

      $scope.reloadCore = function() {
        if ($scope.initFailures[$scope.selectedCore]) {
          delete $scope.initFailures[$scope.selectedCore];
          $scope.showInitFailures = Object.keys(data.initFailures).length>0;
        }
        Cores.reload({core: $scope.selectedCore},
          function(data) {
            if (data.error) {
              $scope.reloadFailure = true;
              $timeout(function() {
                $scope.reloadFailure = false;
                $route.reload();
              }, 1000);
            } else {
              $scope.reloadSuccess = true;
              $timeout(function () {
                $scope.reloadSuccess = false;
                $route.reload();
              }, 1000);
            }
          });
      };

      $scope.hideAll = function() {
        $scope.showRename = false;
        $scope.showAdd = false;
        $scope.showSwap = false;
      };

      $scope.optimizeCore = function() {
        Update.optimize({core: $scope.selectedCore},
          function(successData) {
            $scope.optimizeSuccess = true;
            $timeout(function() {$scope.optimizeSuccess=false}, 1000);
            $scope.refresh();
          },
          function(failureData) {
            $scope.optimizeFailure = true;
            $timeout(function () {$scope.optimizeFailure=false}, 1000);
            $scope.refresh();
          });
      };

      $scope.refresh();
    }
);
