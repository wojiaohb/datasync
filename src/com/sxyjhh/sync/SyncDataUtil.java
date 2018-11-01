package com.sxyjhh.sync;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import java.io.*;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * description：数据同步工具类
 * author：Shaok Lei
 * Date：2018-10-25
 */
public class SyncDataUtil {
    /**
     * 私有化默认的创建方法
     */
    private SyncDataUtil(){}

    /**
     * 私有内部类，用于创建单例
     */
    private static class SingletonHolder{
        private final static SyncDataUtil instance=new SyncDataUtil();
    }

    /**
     * 获取单例
     * @return
     */
    public static SyncDataUtil getInstance(){
        return SingletonHolder.instance;
    }
    /**
     * 同步数据的控制方法，用于从配置文件中获取需要同步的数据库、表的信息，
     * 调用表同步方法同步数据，
     * 将同步后的数据执行点（最后时间）写入到配置文件中供下次同步调用时使用
     * @return
     */
    protected boolean syncRunController(){
        boolean result = true;
        // 创建saxReader对象
        SAXReader reader = new SAXReader();
        // 通过read方法读取一个文件 转换成Document对象
        Document document = null;
        try {
            document = reader.read(new File("data_config.xml"));
        } catch (DocumentException e) {
            e.printStackTrace();
        }

        if(null != document){
            //数据库操作工具
            DBUtil dbUtil = DBUtil.getInstance();
            //读取来源库信息
            String sourceIp = document.getRootElement().element("sourceIp").getStringValue();
            String sourcePort = document.getRootElement().element("sourcePort").getStringValue();
            String sourceDBName = document.getRootElement().element("sourceDBName").getStringValue();
            String sourceUserName = document.getRootElement().element("sourceUserName").getStringValue();
            String sourceUserPwd = document.getRootElement().element("sourceUserPwd").getStringValue();
            //获取来源库链接
            Connection sourceConn = dbUtil.getOracleConnection(sourceIp,sourcePort,sourceDBName,sourceUserName,sourceUserPwd);

            //读取目标库信息
            String aimIp = document.getRootElement().element("aimIp").getStringValue();
            String aimPort = document.getRootElement().element("aimPort").getStringValue();
            String aimDBName = document.getRootElement().element("aimDBName").getStringValue();
            String aimUserName = document.getRootElement().element("aimUserName").getStringValue();
            String aimUserPwd = document.getRootElement().element("aimUserPwd").getStringValue();

            //获取目标库链接
            Connection aimConn = dbUtil.getOracleConnection(aimIp,aimPort,aimDBName,aimUserName,aimUserPwd);

            //读取所有要同步的表
            List<Element> tableList = document.getRootElement().element("tables").elements("table");
            //循环执行表数据同步
            for (Element tableElement : tableList) {
                //获取表名称
                String tableName = tableElement.attributeValue("table_name");
                //获取时间戳字段名称
                String tableDateCol = tableElement.attributeValue("table_date_col");
                //获取最大时间
                String maxDate = tableElement.attributeValue("max_date");

                //执行表数据同步
                maxDate = syncOneTable(tableName,tableDateCol,maxDate,sourceConn,aimConn);
                if(!"".equals(maxDate)){
                    tableElement.setAttributeValue("max_date",maxDate);
                    writer(document);
                }

            }

            //释放数据库连接
            dbUtil.closeConn(sourceConn,null,null);
            dbUtil.closeConn(aimConn,null,null);
        }else{
            result = false;
        }
        return result;
    }


    /**
     * 将修改的xml内容写入文件中
     */
    private void writer(Document document){
        // 紧凑的格式
        // OutputFormat format = OutputFormat.createCompactFormat();
        // 排版缩进的格式
        OutputFormat format = OutputFormat.createPrettyPrint();
        // 设置编码
        format.setEncoding("UTF-8");
        // 创建XMLWriter对象,指定了写出文件及编码格式
        // XMLWriter writer = new XMLWriter(new FileWriter(new
        // File("src//a.xml")),format);
        File file = new File("data_config.xml");
        OutputStream opStream = null;
        try {
            opStream = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        OutputStreamWriter opsWriter = null;
        if(null != opStream){
            try {
                opsWriter = new OutputStreamWriter(opStream,"UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        if(null != opsWriter){
            XMLWriter writer = new XMLWriter(opsWriter, format);
            try {
                // 立即写入
                writer.write(document);
                writer.flush();
                // 关闭操作
                writer.close();
                opsWriter.close();
                opStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }


        }
    }


    /**
     * 执行表数据同步
     * @param tableName 表名称
     * @param tableDateCol 时间戳字段
     * @param maxDate 最大时间（yyyy-MM-dd hh:mm:ss）
     * @param sourceConn 源数据库连接
     * @param aimConn 目标数据库连接
     * @return
     */
    private String syncOneTable(String tableName, String tableDateCol, String maxDate, Connection sourceConn, Connection aimConn) {
        DBUtil dbUtil = DBUtil.getInstance();
        Date date=null;
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //1.查询要同步的数据量
        int conut = 0;
        StringBuilder contSql = new StringBuilder("select count(*) from ");
        contSql.append(tableName);
        if(null != maxDate && !"".equals(maxDate)){
            contSql.append(" where ");
            contSql.append(tableDateCol);
            contSql.append(">?");
        }
        try {
            PreparedStatement countState = sourceConn.prepareStatement(contSql.toString());
            if(null != maxDate && !"".equals(maxDate)){
                try {
                    long dateLong = format.parse(maxDate).getTime();
                    countState.setDate(1,new Date(dateLong));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            ResultSet countResultSet = countState.executeQuery();
            if(countResultSet.next()){
                conut = countResultSet.getInt(1);
            }
            dbUtil.closeConn(null,countState,countResultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //2.如果有数据需要同步
        if(conut > 0){
            System.out.println(conut);
            //2.1查询要同步的数据的最大时间戳
            StringBuffer datesql=new StringBuffer("SELECT MAX( ");
            datesql.append(tableDateCol);
            datesql.append(") from ");
            datesql.append(tableName);
            try {
				PreparedStatement pstm=sourceConn.prepareStatement(datesql.toString());
				ResultSet executeQuery = pstm.executeQuery();
				if(executeQuery.next()){
					date = executeQuery.getDate(1);
				}
				dbUtil.closeConn(null,pstm,executeQuery);
			} catch (SQLException e) {
				e.printStackTrace();
			}
            //3.生成数据插入语句
            //3.1查询当前表的列名
            List<String> colList =  dbUtil.findAllColumns(tableName, sourceConn);
            //3.2生成数据插入语句 （增加的方式）
            StringBuilder insertSql = new StringBuilder("INSERT INTO ");
            insertSql.append("sync_table_1_test");
            insertSql.append(" ( ");
            StringBuilder paramSql = new StringBuilder(" values (");

            for(int i = 0 ; i < colList.size() ; i++){
                if(i<(colList.size()-1)){
                    if(colList.get(i).toUpperCase().equals("ACCEPTLIST")){
                        insertSql.append(colList.get(i));
                        insertSql.append(",");
                        paramSql.append("empty_clob()");
                        paramSql.append(",");
                    }else{
                        insertSql.append(colList.get(i));
                        insertSql.append(",");
                        paramSql.append("?");
                        paramSql.append(",");
                    }
                }else{
                    if(colList.get(i).toUpperCase().equals("ACCEPTLIST")){
                        insertSql.append(colList.get(i));
                        insertSql.append(")");
                        paramSql.append("empty_clob()");
                        paramSql.append(")");
                    }else{
                        insertSql.append(colList.get(i));
                        insertSql.append(")");
                        paramSql.append("?");
                        paramSql.append(")");
                    }
                }
            }
            //增加数据到表中
            String sqlForInsert = insertSql.toString() + paramSql.toString();
            System.out.println(sqlForInsert);
            //4.分页查询要同步的数据，执行数据插入操作
            //Oracle  12c的分页查询方式
            // SELECT * FROM top_test order by id fetch first 10 rows only;
            //select * from(select a.*,rownum rn from (select * from tableName) a
            //where rownum between (pageSize*pageNum-(pageSize-1)) and pageSize*pageNum)
            // SELECT * FROM top_test where updatetime order by updatetime fetch first 5 rows only;
            StringBuffer rn = new StringBuffer("select * from ");
            rn.append(" ( select a.* from  (select * from " );
            rn.append(tableName);
            rn.append(" ) a ");
            rn.append(" where ");
            rn.append(tableDateCol);
            rn.append("<=?");//date
            if(null != maxDate && !"".equals(maxDate)){
                rn.append(" and ");
                rn.append(tableDateCol);
                rn.append(">?");
            }
            rn.append(" and ");
            rn.append(" rownum  between ? and ? ) ");
            System.out.println(rn.toString());
            int page = 0;
            PreparedStatement pstm = null;
			try {
				pstm = sourceConn.prepareStatement(rn.toString());
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
            int insertNum =0;
            while (page*200 <= conut){
                page += 1;
            	try{
	            	//从数据源查询数据,每页按200条数据查询
	            	int offset = page * 200-( 200-1 );
	            	int endoff = page*200 ;
                    if(null != maxDate && !"".equals(maxDate)){
                        pstm.setDate(1, date);
                        try {
                            long dateLong = format.parse(maxDate).getTime();
                            pstm.setDate(2,new Date(dateLong));
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        pstm.setInt(3, offset);
                        pstm.setInt(4, endoff);
                    }else{
                        pstm.setDate(1, date);
                        pstm.setInt(2, offset);
                        pstm.setInt(3, endoff);
                    }


					ResultSet executeQuery = pstm.executeQuery();
                    //存入目标数据库
                    PreparedStatement insertpstm=aimConn.prepareStatement(sqlForInsert);

					while(executeQuery.next()){
						//将返回数据
                        int x=1;
						for(int j=0;j<colList.size();j++){
						    if(!colList.get(j).toUpperCase().equals("ACCEPTLIST")){
                                insertpstm.setObject(j+x, executeQuery.getObject(colList.get(j)));
                            }else{
						        x=0;
                            }
						}
						//增加到新表中的数据
                        boolean b = insertpstm.execute();

                        insertNum += b?1:0;
					}
					dbUtil.closeConn(null,pstm,executeQuery);
					dbUtil.closeConn(null,insertpstm,null);
					System.out.println("插入总计："+conut+"条！");
            	}catch(Exception e){
            		e.printStackTrace();
            	}
            }
        }
        //5.返回最大时间
        if(null == date){
            return "";
        }else{
            return format.format(date);
        }
    }
}
