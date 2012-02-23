require
(
	[
		'lib/order!lib/console',
		'lib/order!jquery',
		'lib/order!lib/jquery.form',
		'lib/order!lib/jquery.jstree',
		'lib/order!lib/jquery.sammy',
		'lib/order!lib/jquery.sparkline',
		'lib/order!lib/jquery.timeago',
		'lib/order!lib/highlight',
		'lib/order!scripts/app',

		'lib/order!scripts/analysis',
		'lib/order!scripts/cloud',
		'lib/order!scripts/cores',
		'lib/order!scripts/dataimport',
		'lib/order!scripts/file',
		'lib/order!scripts/index',
		'lib/order!scripts/java-properties',
		'lib/order!scripts/logging',
		'lib/order!scripts/ping',
		'lib/order!scripts/plugins',
		'lib/order!scripts/query',
		'lib/order!scripts/replication',
		'lib/order!scripts/schema-browser',
		'lib/order!scripts/threads',

		'lib/order!scripts/dashboard'
		
	],
	function( $ )
	{
		app.run();
	}
);