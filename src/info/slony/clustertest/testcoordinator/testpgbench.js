//
//
// A test script (to be run in the test coordinator) that sends 
// a pgbench test client out and looks at the timestamp information returned
// it then will launch checkSlave tests clients to validate that the 
// data is visible to another test client.
//
//

//

coordinator.includeFile("util/compare_pgbench.js");
coordinator.includeFile("util/sloniksync.js");
coordinator.includeFile("util/prepare_db.js");
coordinator.includeFile("util/run_pgbench.js");


var results = new Packages.info.slony.clustertest.testcoordinator.TestResult(
		'pgbench');


/**
 * A task to try to shutdown the test after an elapsed period of time.
 */
shutdownTask = {
	onEvent : function(object, event) {			
			coordinator.stopProcessing();
			coordinator.clearListeners();			
	}
};



prepare_db(coordinator,results,['db1','db2']);

//Start the slons.
//These must be started before slonik runs or the subscribe won't happen
//thus slonik won't finish.
slon1 = coordinator.createSlonLauncher("db1");
slon2 = coordinator.createSlonLauncher("db2");
slon1.run();
slon2.run();

// First setup slony
coordinator.includeFile("info/slony/clustertest/testcoordinator/testslonik.js");
setupReplication(coordinator, results);

if(results.getFailureCount()==0) {
	pgbench = runPgBench(coordinator,results,'db1','db2');
	coordinator.processEvents();
}


syncNow(coordinator,"1","1",results);

compare_pgbench(coordinator,results,"db1","db2")

results.testComplete();
slon1.stop();
slon2.stop();
coordinator.shutdown();