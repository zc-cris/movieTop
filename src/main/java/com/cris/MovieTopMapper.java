package com.cris;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 进行 ETL 流程的 Mapper，不需要走 Reducer
 *
 * @author zc-cris
 * @version 1.0
 **/
@SuppressWarnings("JavaDoc")
public class MovieTopMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text k = new Text();

    /** 通过工具类将每行数据进行校验和转换
     * @param key 文本偏移量
     * @param value 每行文本
     * @param context MapReduce 上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split("\t");

        String result = EtlStringUtil.handle(strings);
        if (result != null) {
            // 自定义数据清洗成功的计数器并自增
            context.getCounter("ETLCounter", "true").increment(1);
            k.set(result);
            context.write(k, NullWritable.get());
        } else {
            // 自定义数据清洗失败并过滤的计数器并自增
            context.getCounter("ETLCounter", "false").increment(1);
        }
    }
}
