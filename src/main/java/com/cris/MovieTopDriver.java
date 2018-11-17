package com.cris;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * ETL 驱动类，最标准的写法
 *
 * @author zc-cris
 * @version 1.0
 **/
@SuppressWarnings("JavaDoc")
public class MovieTopDriver implements Tool {

    /**
     * 抽象的配置类
     **/
    private Configuration configuration = new Configuration();
    /**
     * 抽象的人物类
     **/
    private Job job = null;

    public static void main(String[] args) {
        try {
            new MovieTopDriver().run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        job = Job.getInstance(configuration);
        job.setJarByClass(MovieTopDriver.class);
        job.setMapperClass(MovieTopMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        initFileInputPath(args[0]);
        initFileOutputPath(args[1]);

        boolean flag = job.waitForCompletion(true);

        return flag ? 0 : 1;
    }

    /**
     * 对 ETL 清理后的文件输出路径做校验
     *
     * @param outputPath ETL 输出文件路径
     * @throws IOException
     */
    private void initFileOutputPath(String outputPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(outputPath);
        boolean exists = fileSystem.exists(path);
        if (exists) {
            // 删除已经存在的 ETL 文件清洗输出目录
            fileSystem.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job, path);
    }

    /**
     * 对 ETL 待清理文件输入路径做出校验
     *
     * @param inputPath ETL 输入文件路径
     * @throws IOException
     */
    private void initFileInputPath(String inputPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path f = new Path(inputPath);
        boolean exists = fileSystem.exists(f);
        if (exists) {
            FileInputFormat.setInputPaths(job, f);
        } else {
            throw new RuntimeException("ETL 文件输入路径不存在！！！");
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }
}
