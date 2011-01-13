var DB=null;

var maxAid = SCALING * 100000;
var maxBid = SCALING;
var maxTid = SCALING * 10;
var updAccounts;
var selAccounts;
var updBranches;
var updTellers;
var insHistory;

	// ----
	// The script must contain a connect() and a disconnect function.
	// As demonstrated below, it can do a bit more than just connect
	// to the database.
	// ----
	function connect(uri, user, pass) {
		DB = java.sql.DriverManager.getConnection(uri, user, pass);
		DB.setAutoCommit(false);

		updAccounts = DB.prepareStatement("UPDATE accounts SET abalance = abalance + ? WHERE aid = ?");
		selAccounts = DB.prepareStatement("SELECT abalance FROM accounts WHERE aid = ?");
		updTellers = DB.prepareStatement("UPDATE tellers SET tbalance = tbalance + ? WHERE tid = ?");
		updBranches = DB.prepareStatement("UPDATE branches SET bbalance = bbalance + ? WHERE bid = ?");
		insHistory = DB.prepareStatement("INSERT INTO history (tid, bid, aid, delta, mtime) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)");
	}

	function disconnect() {
		DB.close();
	}

	// ----
	// trans_tpcb actually implements the standard pgbench transaction.
	// ----
	function run() {
		var aid = util.random(1, maxAid);
		var bid = util.random(1, maxBid);
		var tid = util.random(1, maxTid);
		var delta = util.random(-5000, 5000);

		updAccounts.setInt(1, delta);
		updAccounts.setInt(2, aid);
		updAccounts.executeUpdate();

		selAccounts.setInt(1, aid);
		var res = selAccounts.executeQuery();
		res.next();
		var abalance = res.getInt(1);

		updTellers.setInt(1, delta);
		updTellers.setInt(2, tid);
		updTellers.executeUpdate();

		updBranches.setInt(1, delta);
		updBranches.setInt(2, bid);
		updBranches.executeUpdate();

		insHistory.setInt(1, tid);
		insHistory.setInt(2, bid);
		insHistory.setInt(3, aid);
		insHistory.setInt(4, delta);
		insHistory.executeUpdate();
		DB.commit();
	}
