package com.grachro.derbysample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbutils.DbUtils;

public class CreateSampleDb {

	public static void main(String[] args) throws SQLException {

		String userHome = System.getProperty("user.home");
		String dbPath = userHome + "/dbviewer-sampleDb.derby";
		String jdbc = "jdbc:derby:" + dbPath + ";create=true";

		Connection conn = null;
		try {
			conn = DriverManager.getConnection(jdbc);
			createTable(conn);
			insert(conn);
			select(conn);
		} finally {
			DbUtils.closeQuietly(conn);
		}

	}

	private static void createTable(Connection conn) throws SQLException {
		Statement stmt = null;

		try {

			StringBuilder sql = new StringBuilder();
			sql.append("create table PROGRAMMING_LANGUAGES (");
			sql.append("  ID integer generated always as identity ");
			sql.append("     (start with 1, increment by 1),");
			sql.append("  NAME varchar(50),");
			sql.append("  constraint PK_PROGRAMMING_LANGUAGES primary key (ID) ");
			sql.append(")");

			stmt = conn.createStatement();
			stmt.execute(sql.toString());

		} finally {
			DbUtils.closeQuietly(stmt);
		}
	}

	private static void insert(Connection conn) throws SQLException {
		PreparedStatement stmt = null;

		String[] list = { "FORTRAN", "LISP", "COBOL", };

		try {

			StringBuilder sql = new StringBuilder();
			sql.append("insert into PROGRAMMING_LANGUAGES (NAME) ");
			sql.append("values (?)");

			stmt = conn.prepareStatement(sql.toString());

			for (String s : list) {
				stmt.setString(1, s);
				stmt.execute();
			}

		} finally {
			DbUtils.closeQuietly(stmt);
		}
	}

	private static void select(Connection conn) throws SQLException {
		Statement stmt = null;

		try {

			StringBuilder sql = new StringBuilder();
			sql.append("select * from PROGRAMMING_LANGUAGES order by NAME ");
			stmt = conn.createStatement();

			ResultSet result = stmt.executeQuery(sql.toString());

			while (result.next()) {
				int id = result.getInt(1);
				String name = result.getString(2);

				System.out.println("[" + id + "]" + name);
			}

		} finally {
			DbUtils.closeQuietly(stmt);
		}
	}
}
