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

solrAdminApp.controller('IndexController', function($scope, System, Cores, Constants) {
  $scope.resetMenu("index", Constants.IS_ROOT_PAGE);
  $scope.reload = function() {
    System.get(function(data) {
      $scope.system = data;

      // load average
      var load_average = ( data.system.uptime || '' ).match( /load averages?: (\d+[.,]\d\d),? (\d+[.,]\d\d),? (\d+[.,]\d\d)/ );
      if (load_average) {
        for (var i=0;i<2;i++) {
          load_average[i]=load_average[i].replace(",","."); // for European users
        }
        $scope.load_average = load_average.slice(1);
      }

      // physical memory
      var memoryMax = parse_memory_value(data.system.totalPhysicalMemorySize);
      $scope.memoryTotal = parse_memory_value(data.system.totalPhysicalMemorySize - data.system.freePhysicalMemorySize);
      $scope.memoryPercentage = ($scope.memoryTotal / memoryMax * 100).toFixed(1)+ "%";
      $scope.memoryMax = pretty_print_bytes(memoryMax);
      $scope.memoryTotalDisplay = pretty_print_bytes($scope.memoryTotal);

      // swap space
      var swapMax = parse_memory_value(data.system.totalSwapSpaceSize);
      $scope.swapTotal = parse_memory_value(data.system.totalSwapSpaceSize - data.system.freeSwapSpaceSize);
      $scope.swapPercentage = ($scope.swapTotal / swapMax * 100).toFixed(1)+ "%";
      $scope.swapMax = pretty_print_bytes(swapMax);
      $scope.swapTotalDisplay = pretty_print_bytes($scope.swapTotal);

      // file handles
      $scope.fileDescriptorPercentage = (data.system.openFileDescriptorCount / data.system.maxFileDescriptorCount *100).toFixed(1) + "%";

      // java memory
      var javaMemoryMax = parse_memory_value(data.jvm.memory.raw.max || data.jvm.memory.max);
      $scope.javaMemoryTotal = parse_memory_value(data.jvm.memory.raw.total || data.jvm.memory.total);
      $scope.javaMemoryUsed = parse_memory_value(data.jvm.memory.raw.used || data.jvm.memory.used);
      $scope.javaMemoryTotalPercentage = ($scope.javaMemoryTotal / javaMemoryMax *100).toFixed(1) + "%";
      $scope.javaMemoryUsedPercentage = ($scope.javaMemoryUsed / $scope.javaMemoryTotal *100).toFixed(1) + "%";
      $scope.javaMemoryPercentage = ($scope.javaMemoryUsed / javaMemoryMax * 100).toFixed(1) + "%";
      $scope.javaMemoryTotalDisplay = pretty_print_bytes($scope.javaMemoryTotal);
      $scope.javaMemoryUsedDisplay = pretty_print_bytes($scope.javaMemoryUsed);  // @todo These should really be an AngularJS Filter: {{ javaMemoryUsed | bytes }}
      $scope.javaMemoryMax = pretty_print_bytes(javaMemoryMax);

      // no info bar:
      $scope.noInfo = !(
        data.system.totalPhysicalMemorySize && data.system.freePhysicalMemorySize &&
        data.system.totalSwapSpaceSize && data.system.freeSwapSpaceSize &&
        data.system.openFileDescriptorCount && data.system.maxFileDescriptorCount);

      // command line args:
      $scope.commandLineArgs = data.jvm.jmx.commandLineArgs.sort();
    });
  };
  $scope.reload();
});

var parse_memory_value = function( value ) {
  if( value !== Number( value ) )
  {
    var units = 'BKMGTPEZY';
    var match = value.match( /^(\d+([,\.]\d+)?) (\w).*$/ );
    var value = parseFloat( match[1] ) * Math.pow( 1024, units.indexOf( match[3].toUpperCase() ) );
  }

  return value;
};

var pretty_print_bytes = function(byte_value) {
  var unit = null;

  byte_value /= 1024;
  byte_value /= 1024;
  unit = 'MB';

  if( 1024 <= byte_value ) {
    byte_value /= 1024;
    unit = 'GB';
  }
  return byte_value.toFixed( 2 ) + ' ' + unit;
};
