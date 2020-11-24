package com.BigDataLecture;

//import javafx.scene.control.Tab;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
//import org.graalvm.compiler.hotspot.nodes.FastAcquireBiasedLockNode;

import java.io.IOException;

/*
* DDL:
* 1. 判断表是否存在
* 2. 创建表
* 3. 创建命名空间
* 4. 删除比较少
*
* DML：
* 5. 插入数据
* 6. 查数据get
* 7. 查数据scan
* 8. 删除数据
*
* DML和DDL是分开的，他们操作的对象（admin）也是分开的
* */

public class TestAPI {
    private  static Connection connection = null;
    private static Admin admin = null;
    static {
        try{
//      1.1 获取文件配置信息
//        HBaseConfiguration configuration = new HBaseConfiguration();
        HBaseConfiguration configuration = new HBaseConfiguration.create();
//      1.2 连接zookeeper， cat hbase-site.xml 里面的hbase.zookeeper.quorum参数
            configuration.set("hbase.zookeeper.quorum","hadoop101,hadoop103,hadoop104");
//      1.3 获取管理员对象
//        HbaseAdmin admin = new HbaseAdmin(configuration);
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
    public static  void close(){
        if(admin != null){
            try{
                admin.close();
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
        if(connection != null){
            try{
                connection.close();
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
    }
//    1. 判断表是否存在
    public static boolean isTableExist(String tableName)throws IOException{
//      1.4 判断表
//        boolean exists = admin.tableExists(tableName);
        boolean exists = admin.tableExists(TableName.valueOf(tableName));

//        admin.close();
        return exists;
    }
//    2. 创建表,cfs可变形参
    public  static void createTable(String tableName,String... cfs)throws IOException{
//        2.1 判断是否存在列族信息
        if(cfs.length <= 0){
            System.out.println("请设置列族信息");
            return;
        }
//        2.2 判断表是否存在
        if(isTableExist(tableName)){
            System.out.println("表已经存在");
            return;
        }
//        2.3 创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
//        2.4 循环添加列族信息
        for(String cf:cfs){
//            2.4.1 创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
//            2.4.2 添加具体列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
//         2.5 创建表
        admin.createTable(hTableDescriptor);
    }
//    3. 删除表
    public static void dropTable(String tableName) throws IOException{
        if(!isTableExist(tableName)){
            System.out.println("表不存在");
        }
//        使表下线
        admin.disableTable(TableName.valueOf(tableName));
//        删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

//    4. 创建命名空间
    public static void createNameSpace(String ns)throws IOException{
//      创建命名空间描述符
        NamespaceDescriptor namespaceDescriptor = new NamespaceDescriptor.create(ns).build;

//      2. 创建命名空间
        try{
            admin.createNamespace(namespaceDescriptor);
        }
        catch (NamespaceExistException e){
//            命名空间已经存在
            e.printStackTrace();
        }
        catch (IOException e){
            e.printStackTrace();
        }

    }

//    5. 向表插入数据
    public static void putData(String tableName,String rowKey,String cf,String cn,String value)throws IOException{
//        1. 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
//        2. 创建put对象
//        hbase底层存的都是字节数组，不分int，string什么的
        Put put = new Put(Bytes.toBytes(rowKey));
//        3. put对象赋值
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn), Bytes.toBytes(value));
//        4. 插入数据
        table.put(put);

        table.close();
    }
//      6. 获取数据get
    public static void getData(String tableName,String rowkey,String cf,String cn)throws IOException{
//        1. 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

//        2. 创建Get对象
        Get get = new Get(Bytes.toBytes(rowkey));
//        3. 获取数据
        Result result = table.get(get);
//        3.1 获取指定的列族，
        get.addFamily(Bytes.toBytes(cf));
//        3.2 获取指定的列，列不能和列族脱离，因此要获取列，一定要获取列族
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
//        3.3 设置获取数据的版本数，可选的
        get.setMaxVersions(5);
//        4. 解析result并打印
        for(Cell cell : result.rawCells()){
            System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    ", CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    ", Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
    }

//    7. 获取数据scan
    public static void scanTable(String tableName) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
//        2. 构建scan对象，空参时表示扫描全局
        Scan scan = new Scan();
//        也可以加上参数，表示rowkey1001-1003范围,是个过滤器，这个范围是左闭右开的
//        Scan scan1 = new Scan(Bytes.toBytes("1001"),Bytes.toBytes("1003"));
//        3. 扫描表
        ResultScanner resultScanner = table.getScanner(scan);

//        4. 解析resultScanner
        for(Result result:resultScanner){
//            解析result并打印
            for(Cell cell:result.rawCells()){
                System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ", CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ", Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }

//    8. 删除数据
    public static void deleteData(String tableName,String rowKey,String cf,String cn)throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
//        2. 构建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
//        2.1 设置删除的列,family代表列族，qualifier代表列
//        addColumn和addColumns的区别是，第一个可以删除指定版本，第二个删除所有版本
//       这个删除最近版本的要慎用，删除最新版本的，那么以前版本的版本的数据会出来诈尸
        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),1563502276050L);
//    2.2 还可以指定删除的列族，命令行里不可以只指定列族，这里可以
        delete.addFamily(Bytes.toBytes(cf));
//        3. 执行删除操作
        table.delete(delete);
        table.close();
    }

/*
* HBASE命令
* list ：查看表
* list_namespace:查看命名空间
* /bin/hbase shell:打开HBASE
* scan 'stu': 查看表
* put 'stu','1005','info2:sex','male'
* get 'stu','1001':
* describe 'stu2'
* delete 'stu','1001','info2:name' :删除数据
* delteall 'sti','1001':删除rowkey
*
* */
    public static void main(String[] args) throws IOException{
//    1. 测试表是否存在
        System.out.println(isTableExist("stu"));
        System.out.println(isTableExist("aaaaaaaaaa"));
//     2. 创建表测试
        createTable("sub5","info1","intfo2");
        System.out.println(isTableExist("stu5"));
//      3. 删除表测试
        dropTable("stu5");
//      4. 创建命名空间测试
//        可在终端list_namespace查看命名空间是否创建成功
        createNameSpace("0408");
//        这个表示在命名空间0408上创建一张表，不写的话是放在默认的命名空间下
//      createTable("s0408:ub5","info1","intfo2");

//      5. 创建数据测试
        putData("stu","1001","info2","name","zhangsan");

//      6. 获取单行数据
        getData("stu","1002","info2","asas");
//      7. 测试扫描数据
        scanTable("stu");
//        8. 测试删除
        deleteData("stu","1001","info","dsa");



//        关闭资源
        close();
    }
}
