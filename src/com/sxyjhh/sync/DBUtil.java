package com.sxyjhh.sync;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DBUtil
{
    public static DBUtil dbutil;

    public static synchronized DBUtil getInstance()
    {
        if (dbutil == null) {
            dbutil = new DBUtil();
        }
        return dbutil;
    }

    public Connection getOracleConnection(String sourceIp, String port, String sourceDBName, String userName, String userPwd)
    {
        Connection conn = null;
        try
        {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection("jdbc:oracle:thin:@" + sourceIp + ":" + port + ":" + sourceDBName, userName, userPwd);
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return conn;
    }

    public Connection getMysqlConnection(String ip, String dbName, String port, String userName, String userPwd)
    {
        Connection conn = null;
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://" + ip + ":" + port + "/" + dbName, userName, userPwd);
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
            return conn;
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            return conn;
        }
        return conn;
    }


    public boolean closeConn(Connection connection, Statement statement, ResultSet resultSet)
    {
        boolean resultsetClosed = false;
        boolean statementClosed = false;
        boolean connectionClosed = false;

        if(resultSet != null){
            try {
                resultSet.close();
                resultsetClosed = true;
            } catch (SQLException e) {
                resultSet=null;
                e.printStackTrace();
            }
        }
        if(statement != null){
            try {
                statement.close();
                statementClosed = true;
            } catch (SQLException e) {
                statement=null;
                e.printStackTrace();
            }
        }
        if(connection != null){
            try {
                connection.close();
                connectionClosed = true;
            } catch (SQLException e) {
                connection=null;
                e.printStackTrace();
            }
        }

        return resultsetClosed && statementClosed && connectionClosed;
    }

    public List<String> findAllColumns(String table, Connection conn)
    {
        List<String> columnList = new ArrayList();
        DatabaseMetaData meta = null;
        ResultSet rs = null;
        try
        {
            meta = conn.getMetaData();
            rs = meta.getColumns(null, null, table, "%");
            while (rs.next())
            {
                columnList.add(rs.getString("COLUMN_NAME"));
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            return columnList;
        }
        return columnList;
    }

    public List<String> findAllColumns(String table, Connection conn, String tableSpace)
    {
        List<String> columnList = new ArrayList();
        DatabaseMetaData meta = null;
        ResultSet rs = null;
        try
        {
            meta = conn.getMetaData();

            rs = meta.getColumns(null, tableSpace, table, "%");
            while (rs.next())
            {
                columnList.add(rs.getString("COLUMN_NAME"));
            }
            return columnList;
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            return columnList;
        }
        finally
        {
            try
            {
                rs.close();
            }
            catch (SQLException e)
            {
                rs = null;
                e.printStackTrace();
            }
        }
    }
}
