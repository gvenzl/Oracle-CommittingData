package com.gvenzl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import oracle.jdbc.pool.OracleDataSource;

/**
 * This class demonstrates the difference in throughput when
 * committing data differently to the Oracle Database.
 * @author gvenzl
 *
 */
public class CommittingData {

	private static String TESTTABLE = "COMMITDATA";
    private static int ITERATIONS = 100000;
    private static String ERROR_LOG_TABLE = "ERR_LOG_TABLE";
    private static String APPEND = "";
	
	private static String host = "";
	private static int port = 0;
	private static String serviceName = "";
	private static String userName = "";
	private static String password = "";
	private static boolean commitEveryRow = false;
	private static boolean commitAtEnd = false;
	private static int batchCommit = 0;
	private static boolean saveExceptions = false;

	private Connection myConnection;
	
	public CommittingData() throws SQLException {
		OracleDataSource ods = new OracleDataSource();
		ods.setDriverType("thin");
		ods.setServerName(host);
		ods.setPortNumber(port);
		ods.setServiceName(serviceName);
		ods.setUser(userName);
		ods.setPassword(password);

		myConnection = ods.getConnection();
		myConnection.setAutoCommit(false);
	}
	/**
	 * Main entry point.
	 * @param args Array with various options on how to execute
	 * and which database to connect to
	 */
	public static void main(final String[] args) throws Exception {

		if (args.length == 0) {
			printHelp();
		}
		else {

			for (int i=0;i<args.length;i++) {
				switch (args[i]) {
					case "-host":
						host = args[++i];
						break;
					case "-port":
						port = Integer.valueOf(args[++i]).intValue();
						break;
					case "-srvn":
						serviceName = args[++i];
						break;
					case "-user":
						userName = args[++i];
						break;
					case "-pass":
						password = args[++i];
						break;
					case "-commitEveryRow":
						commitEveryRow = true;
						break;
					case "-commitAtEnd":
						commitAtEnd = true;
						break;
					case "-batchCommit":
						batchCommit = Integer.valueOf(args[++i]).intValue();
						break;
					case "-saveExceptions":
						saveExceptions = true;
						break;
					case "-directPath":
						APPEND = " /*+ APPEND */ ";
						break;
					default: printHelp();
				}
			}

			CommittingData myApp = new CommittingData();
			myApp.setup();
			myApp.runTests();
			myApp.tearDown();
		}
	}

	/**
	 * This method prepares the Database for testing by creating the test table.
	 * @throws SQLException Any Database related error
	 */
	private void setup() throws SQLException {

		myConnection.prepareStatement(
				"CREATE TABLE " + TESTTABLE
					+ " (ID NUMBER, TXT VARCHAR2(255))").execute();
		if (saveExceptions) {
			myConnection.prepareStatement(
					"BEGIN DBMS_ERRLOG.CREATE_ERROR_LOG('" + TESTTABLE + "', '" + ERROR_LOG_TABLE + "'); END;").execute();
		}
	}
	
	private void tearDown() throws SQLException {
		myConnection.prepareStatement("DROP TABLE " + TESTTABLE).execute();
		if (saveExceptions) {
			myConnection.prepareStatement("DROP TABLE " + ERROR_LOG_TABLE).execute();
		}
		myConnection.prepareStatement("PURGE USER_RECYCLEBIN").execute();
	}
	
	/**
	 * Run the tests
	 * @throws SQLException Any Database error
	 */
	private void runTests() throws SQLException {
		
		if (commitEveryRow) {
			commitEveryRow();
		}
		else if (commitAtEnd) {
			commitAtEnd();
		}
		else if (batchCommit > 0 && saveExceptions) {
			batchCommitSaveExceptions(batchCommit);
		}
		else if (batchCommit > 0) {
			batchCommit(batchCommit);
		}
	}

	/**
	 * This method loads static data into the test table.
	 * It iterates over a loop as many times as is specified in the static ITERATIONS variable.
	 * The method commits after every newly inserted row.
	 * @throws SQLException Any Database error that might occur during the insert
	 */
	private void commitEveryRow() throws SQLException {
		
		System.out.println("Loading data with committing after every row - " + ITERATIONS + " iterations");
		
		PreparedStatement stmt = myConnection.prepareStatement("INSERT " + APPEND + " INTO " + TESTTABLE + " VALUES (?,?)");
		
		long startTime = System.currentTimeMillis();
		for(int i=0;i<ITERATIONS;i++) {
			stmt.setInt(1, i);
			stmt.setString(2, ";ajskfj[wig[ajdfkjaw[oeimakldjalksva;djfashdfjksahdf;lkjasdfoiwejaflkf;smvwlknvoaweijfasdfjasldf;kwlvma;dfjlaksjfowemowaivnoawn");
			stmt.execute();
			myConnection.commit();
		}
		long endTime = System.currentTimeMillis();
		
		System.out.println("Data loaded in: " + (endTime-startTime) + "ms");
	}
	
	/**
	 * This method loads static data into the test table.
	 * It iterates over a loop as many times as is specified in the static ITERATIONS variable.
	 * The method commits only once after all the data is fully loaded.
	 * @throws SQLException Any database error that may occurs during the insert
	 */
	private void commitAtEnd() throws SQLException {
		
		System.out.println("Loading data with committing after the entire set is loaded - " + ITERATIONS + " iterations");
		
		PreparedStatement stmt = myConnection.prepareStatement(
				"INSERT " + APPEND + " INTO " + TESTTABLE + " VALUES (?,?)");
		
		long startTime = System.currentTimeMillis();
		for(int i=0;i<ITERATIONS;i++) {
			stmt.setInt(1, i);
			stmt.setString(2, ";ajskfj[wig[ajdfkjaw[oeimakldjalksva;djfashdfjksahdf;lkjasdfoiwejaflkf;smvwlknvoaweijfasdfjasldf;kwlvma;dfjlaksjfowemowaivnoawn");
			stmt.execute();
		}
		myConnection.commit();
		long endTime = System.currentTimeMillis();
		
		System.out.println("Data loaded in: " + (endTime-startTime) + "ms");
		
	}
	
	/**
	 * This method loads static data into the test table.
	 * It used the JDBC batching functionality and iterates over a loop
	 * as many times as is specified in the static ITERATIONS variable.
	 * @param batchSize The size of the batch before executing it.
	 * @throws SQLException Any database error that may occurs during the insert
	 */
	private void batchCommit(final int batchSize) throws SQLException {
		
		System.out.println("Batch loading data with committing "
				+ "after the entire set is loaded - " + ITERATIONS + " iterations");
		
		PreparedStatement stmt = myConnection.prepareStatement(
				"INSERT " + APPEND + " INTO " + TESTTABLE + " VALUES (?,?)");
		
		long startTime = System.currentTimeMillis();
		for(int i=0;i<ITERATIONS;i++) {
			stmt.setInt(1, i);
			stmt.setString(2, ";ajskfj[wig[ajdfkjaw[oeimakldjalksva;djfashdfjksahdf;lkjasdfoiwejaflkf;smvwlknvoaweijfasdfjasldf;kwlvma;dfjlaksjfowemowaivnoawn");
			stmt.addBatch();
			// Execute batch if batch size is reached
			if ((i % batchSize) == 0) {
				stmt.executeBatch();
			}
		}
		stmt.executeBatch();
		myConnection.commit();
		long endTime = System.currentTimeMillis();
		
		System.out.println("Data loaded in: " + (endTime-startTime) + "ms");

	}

	/**
	 * This method loads static data into the test table.
	 * It used the JDBC batching functionality and iterates over a loop
	 * as many times as is specified in the static ITERATIONS variable.
	 * It also uses the LOG ERRORS clause that will prevent the
	 * execution from failing if any exception occurs within the batch.
	 * @param batchSize The size of the batch before executing it.
	 * @throws SQLException Any database error that may occurs during the insert
	 */
	private void batchCommitSaveExceptions(final int batchSize) throws SQLException {
		
		System.out.println("Batch loading data (save exceptions) with committing "
				+ "after the entire set is loaded - " + ITERATIONS + " iterations");
		
		PreparedStatement stmt = myConnection.prepareStatement(
				"INSERT " + APPEND + " INTO " + TESTTABLE + " VALUES (?,?) LOG ERRORS INTO " + ERROR_LOG_TABLE + " REJECT LIMIT UNLIMITED");
		
		long startTime = System.currentTimeMillis();
		for(int i=0;i<ITERATIONS;i++) {
			stmt.setInt(1, i);
			stmt.setString(2, ";ajskfj[wig[ajdfkjaw[oeimakldjalksva;djfashdfjksahdf;lkjasdfoiwejaflkf;smvwlknvoaweijfasdfjasldf;kwlvma;dfjlaksjfowemowaivnoawn");
			stmt.addBatch();
			// Execute batch if batch size is reached
			if ((i % batchSize) == 0) {
				stmt.executeBatch();
			}
		}
		stmt.executeBatch();
		myConnection.commit();
		long endTime = System.currentTimeMillis();
		
		System.out.println("Data loaded in: " + (endTime-startTime) + "ms");

	}
	/**
	 * Prints the help and exits the program.
	 */
	private static void printHelp() {
		System.out.println("Committing data to the Oracle Database - Usage:");
		System.out.println();
		System.out.println("java com.gvenzl.CommittingData -host [host] -port [port] -srvn [service name] -user [username] -pass [password] -commitEveryRow -commitAtEnd -batchCommit [batch size] -saveExceptions -directPath");
		System.out.println();
		System.out.println("host: 		The database host name");
		System.out.println("port: 		The database listener port");
		System.out.println("service name: 	The database service name");
		System.out.println("username: 	The database username");
		System.out.println("password: 	The database user password");
		System.out.println("commitEveryRow: Commit data after every row");
		System.out.println("commitAtEnd: 	Commit data only once at the end of a load");
		System.out.println("batch size: 	The size of the loading batch to execute at once");
		System.out.println("saveExceptions: Specify whether you would like to save exceptions during batch loading");
		System.out.println("directPath: 	Specify whether you would like to use DIRECT PATH loading");

		System.exit(0);
	}

}
