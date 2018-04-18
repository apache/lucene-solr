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

solrAdminApp.controller('JavaPropertiesController',
  function($scope, Properties, Constants){
    $scope.resetMenu("java-props", Constants.IS_ROOT_PAGE);
    $scope.refresh = function() {
      Properties.get(function(data) {
        var sysprops = data["system.properties"];
        var sep = sysprops["path.separator"]
        var props = [];
        for (var key in sysprops) {
          var value = sysprops[key];
          var key = key.replace(/\./g, '.&#8203;');
          if (key.indexOf(".path")!=-1 || key.indexOf(".dirs")) {
            var values = [];
            var parts = value.split(sep);
            for (var i in parts) {
              values.push({pos:i, value:parts[i]})
            }
            props.push({name: key, values: values});
          } else {
            props.push({name: key, values: [value]});
          }
        }
        $scope.props = props;
      });
    };

    $scope.refresh();
  });
