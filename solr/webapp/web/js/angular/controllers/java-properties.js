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
          var values = value.split(sep);
          if (value === sep) {
            values = [':'];
          }
          props.push({
            name: key.replace(/\./g, '.&#8203;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;'),
            values: values
          });
        }
        $scope.pathSeparator = sep;
        $scope.props = props;
      });
    };

    $scope.refresh();
  });
