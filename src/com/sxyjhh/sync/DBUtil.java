package com.sxyjhh.sync;

import com.alibaba.fastjson.JSONObject;

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

    /**
     * 释放资源
     **/
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
    /**
     * 查询某表所有字段
     **/
 /*   public List<String> findAllColumns(String table,Connection conn)
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
    }*/
    /**
     * 查询某表所有字段
     **/
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

    /**
     * 查询某表所有字段
     **/
    public List<String> findTableFields(Connection con, String table) {
        PreparedStatement ptst = null;
        ResultSet rs;
        List<String> field = new ArrayList(); //字段
        String sql = "desc " + table;
        try {
            ptst = con.prepareStatement(sql);
            rs = ptst.executeQuery();
            while (rs.next()) {
                field.add(rs.getString("field"));
            }
            closeConn(null, ptst, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return field;
    }

    public List<JSONObject> findAllColumns(String table, Connection conn)
    {
        List<JSONObject> columnObjList = new ArrayList();
        DatabaseMetaData meta = null;
        ResultSet rs = null;
        try
        {
            meta = conn.getMetaData();
            rs = meta.getColumns(null, null, table, "%");
            while (rs.next())
            {
                JSONObject colJsonObj = new JSONObject();
                colJsonObj.put("code", rs.getString("COLUMN_NAME"));
                colJsonObj.put("name", rs.getString("REMARKS"));
                colJsonObj.put("type", rs.getString("TYPE_NAME"));
                colJsonObj.put("length", rs.getString("COLUMN_SIZE"));
                colJsonObj.put("decimalDigits", rs.getString("DECIMAL_DIGITS"));
                colJsonObj.put("nullable", rs.getString("NULLABLE"));
                columnObjList.add(colJsonObj);
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            return columnObjList;
        }
        return columnObjList;
    }
}
