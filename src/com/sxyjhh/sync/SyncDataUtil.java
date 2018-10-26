package com.sxyjhh.sync;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;

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
        //创建一个DocumentBuilderFactory的对象
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        //创建一个DocumentBuilder的对象
        //创建DocumentBuilder对象
        DocumentBuilder db = null;
        try {
            db = dbf.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            db = null;
            e.printStackTrace();
        }
        //通过DocumentBuilder对象的parser方法加载books.xml文件到当前项目下
        Document document = null;
        if(null != db){
            try {
                document = db.parse("data_config.xml");
            } catch (SAXException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if(null != document){
            //数据库操作工具
            DBUtil dbUtil = DBUtil.getInstance();
            //读取来源库信息
            String sourceIp = document.getElementsByTagName("sourceIp").item(0).getAttributes().getNamedItem("value").getTextContent();
            String sourcePort = document.getElementsByTagName("sourcePort").item(0).getAttributes().getNamedItem("value").getTextContent();
            String sourceDBName = document.getElementsByTagName("sourceDBName").item(0).getAttributes().getNamedItem("value").getTextContent();
            String sourceUserName = document.getElementsByTagName("sourceUserName").item(0).getAttributes().getNamedItem("value").getTextContent();
            String sourceUserPwd = document.getElementsByTagName("sourceUserPwd").item(0).getAttributes().getNamedItem("value").getTextContent();
            //获取来源库链接
            Connection sourceConn = dbUtil.getOracleConnection(sourceIp,sourcePort,sourceDBName,sourceUserName,sourceUserPwd);

            //读取目标库信息
            String aimIp = document.getElementsByTagName("aimIp").item(0).getAttributes().getNamedItem("value").getTextContent();
            String aimPort = document.getElementsByTagName("aimPort").item(0).getAttributes().getNamedItem("value").getTextContent();
            String aimDBName = document.getElementsByTagName("aimDBName").item(0).getAttributes().getNamedItem("value").getTextContent();
            String aimUserName = document.getElementsByTagName("aimUserName").item(0).getAttributes().getNamedItem("value").getTextContent();
            String aimUserPwd = document.getElementsByTagName("aimUserPwd").item(0).getAttributes().getNamedItem("value").getTextContent();
            //获取目标库链接
            Connection aimConn = dbUtil.getOracleConnection(aimIp,aimPort,aimDBName,aimUserName,aimUserPwd);

            //读取所有要同步的表
            NodeList tableList = document.getElementsByTagName("table");
            //循环执行表数据同步
            for (int i = 0; i < tableList.getLength(); i++) {
                Node tableNode = tableList.item(i);
                //获取表名称
                String tableName = tableNode.getAttributes().getNamedItem("table_name").getTextContent();
                //获取时间戳字段名称
                String tableDateCol = tableNode.getAttributes().getNamedItem("table_date_col").getTextContent();
                //获取最大时间
                String maxDate = tableNode.getAttributes().getNamedItem("max_date").getTextContent();

                //执行表数据同步
                maxDate = syncOneTable(tableName,tableDateCol,maxDate,sourceConn,aimConn);
                tableNode.getAttributes().getNamedItem("max_date").setTextContent(maxDate);
                writeXML(document,"data_config.xml");
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
     * @param doc
     * @param file
     */
    private static void writeXML(Document doc, String file) {
        try {
            Transformer t = TransformerFactory.newInstance().newTransformer();
            t.setOutputProperty("indent", "yes");
            t.transform(new DOMSource(doc), new StreamResult(new FileOutputStream(file)));
        } catch (TransformerException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
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
        return "2018-10-26 14:06:00";
    }
}
