package com.BigDataLecture.mr1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

// TableReducer 是帮助我们把数据写到HBASE表的数据
public class FruitReducer extends TableReducer<LongWritable, Text, NullWritable>{
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
//        1. 遍历values 1001 Apple Red
        for(Text value:values){
//            2. 获取每一行数据
            String[] fields = value.toString().split("\t");

//            构建put对象,参数是rowkey
            Put put = new Put(Bytes.toBytes(fields[0]));
//            给put对象赋值
//            put.addColumn(Bytes.toBytes("info"),)
        }
    }
}
