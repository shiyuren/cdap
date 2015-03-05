angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsDetailRunStatusController', function($state, $scope, MyDataSource, amMoment, $filter, myHelpers) {
    var dataSrc = new MyDataSource($scope),
        filterFilter = $filter('filter'),
        basePath = '/apps/' + $state.params.appId + '/workflows/' + $state.params.programId;
    $scope.moment = amMoment;
    $scope.moment.changeLocale('en');
    $scope.status = null;
    $scope.duration = null;
    $scope.startTime = null;
    $scope.data = {};
    dataSrc.request({
      _cdapNsPath: basePath
    })
      .then(function(res) {
        var edges = [],
            nodes = [];

        myHelpers.convert(angular.copy(res.nodes), edges);
        myHelpers.expandForks(res.nodes, nodes);

        nodes = nodes.map(function(item) {
          return angular.extend({
            name: item.program.programName + item.nodeId,
            type: item.nodeType
          }, item);
        });

        $scope.data = {
          nodes: nodes,
          edges: edges,
          metrics: {}
        };

        var programs = [];
        angular.forEach(res.nodes, function(value, key) {
          programs.push(value.program);
        });
        $scope.actions = programs;
      });


    $scope.goToDetailActionView = function(programId, programType) {
      // As of 2.7 only a mapreduce job is scheduled in a workflow.
      if (programType === 'MAPREDUCE') {
        $state.go('mapreduce.detail', {
          programId: programId
        });
      }
    };

    dataSrc.poll({
      _cdapNsPath: basePath + '/runs'
    }, function(res) {
        var run, startMs;
        var runsThatWeCareAbout = filterFilter(res, { runid:$state.params.runId });
        if(runsThatWeCareAbout.length) {
          run = runsThatWeCareAbout[0];
          startMs = run.start * 1000;
          $scope.startTime = new Date(startMs);
          $scope.status = run.status;
          $scope.duration = (run.end ? (run.end * 1000) - startMs : 0);
        }


      });

  });
