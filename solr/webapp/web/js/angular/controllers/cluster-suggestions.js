solrAdminApp.controller('ClusterSuggestionsController',
function($scope,$http) {
    $scope.data={}
    var dataArr =[];
    var dataJson = {};
    //function to display suggestion
    $http({
       method: 'GET',
       url: '/api/cluster/autoscaling/suggestions'
    }).then(function successCallback(response) {
         $scope.data = response.data;
         $scope.parsedData = $scope.data.suggestions;
      }, function errorCallback(response) {
      });
    //function to perform operation
    $scope.postdata = function (x) {
        x.loading = true;
        var path=x.operation.path;
        var command=x.operation.command;
        var fullPath='/api/'+path;
        console.log(fullPath);
        console.log(command);
        $http.post(fullPath, JSON.stringify(command)).then(function (response) {
            if (response.data)
            console.log(response.data);
            x.loading = false;
            x.done = true;
            x.run=true;
            $scope.msg = "Post Data Submitted Successfully!";
        }, function (response) {
            x.failed=true;
            $scope.msg = "Service not Exists";
            $scope.statusval = response.status;
            $scope.statustext = response.statusText;
            $scope.headers = response.headers();
        });
    };
    $scope.showPopover = function() {
      $scope.popup = true;
    };

    $scope.hidePopover = function () {
      $scope.popup = false;
    };
});
