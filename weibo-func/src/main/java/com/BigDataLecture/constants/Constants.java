package com.BigDataLecture.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class Constants {
//    HBase的配置信息
    public static Configuration CONFIGURATION = HBaseConfiguration.create();

//    命名空间
    public static String NAMESPACE = "weibo";
//    三张表
//    1. 微博内容表
    public static String CONTENT_TABLE = "weibo:content";
    public static String CONTENT_TABLE_CF = "info";
    public static int CONTENT_TABLE_VRESIONS = 1;

//    2. 用户关系表
    public static String RELATION_TABLE = "weibo:relation";
//    两个列族
    public static String RELATION_TABLE_CF1 = "attends";
    public static String RELATION_TABLE_CF2 = "fans";
    public static int RELATION_TABLE_VRESIONS = 1;

//    3. 初始化页面表
    public static String INBOX_TABLE = "weibo:inbox";
    public static String INBOX_TABLE_CF = "info";
    public static int INBOX_TABLE_VRESIONS = 3;

}
