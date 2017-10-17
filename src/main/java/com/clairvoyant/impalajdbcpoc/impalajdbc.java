package com.clairvoyant.impalajdbcpoc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class impalajdbc {

    private static String driverName = "com.cloudera.impala.jdbc3.Driver";

    private static final Logger logger = Logger.getLogger(impalajdbc.class.getName());

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection con = DriverManager.getConnection(
                "jdbc:impala://localhost:21050/default;AuthMech=0");
        Statement stmt = con.createStatement();

        boolean Successful;

        try {
            Successful = stmt.execute("create table if not exists test1 like test");

            Successful = stmt.execute("drop table test1");

            logger.info("Drop Table Result: True");

            Successful = stmt.execute("Invalidate metadata");

            Successful = true;

        } catch (SQLException e) {
            System.out.println("Exception");
            e.printStackTrace();
            Successful = false;
        }
        System.out.println("Successful:" + Successful);


        //Sending Exit Code
        if (Successful == false) {
            System.exit(1);
        }
        stmt.close();
        con.close();
    }
}
