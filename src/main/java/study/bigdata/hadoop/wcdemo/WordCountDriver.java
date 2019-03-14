package study.bigdata.hadoop.wcdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    /**
     * 相当于一个yarn集群的客户端 需要在此封装我们的mr程序的相关运行参数，指定jar包 最后交给yarn
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

//      if (args == null || args.length == 0) {
//
//          args = new String[2];
//          args[0] = "hdfs://master:9000/WordCount/input/NOTICE.txt";
//          args[1] = "hdfs://master:9000/WordCount/output3";
//      }

        Configuration conf = getConf();
        Job job = Job.getInstance();

        // 指定本程序的jar包错在的本地路径
        job.setJarByClass(WordCountDriver.class);

        // 指定本业务job要使用的mapper业务类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 指定job的输入结果所在目录
        deleteDir(conf, args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        // job.submit();
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

    private static Configuration getConf() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://mycluster");
        conf.set("dfs.nameservices", "mycluster");
        conf.set("dfs.ha.namenodes.mycluster", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.mycluster.nn1", "hadoop1:8020");
        conf.set("dfs.namenode.rpc-address.mycluster.nn2", "hadoop5:8020");
        //conf.setBoolean(name, value);
        conf.set("dfs.client.failover.proxy.provider.mycluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        return conf;
    }

    private static void deleteDir(Configuration conf, String path) throws IOException {
        FileSystem fs = FileSystem.newInstance(conf);
        Path path1 = new Path(path);
        if (fs.exists(path1)) {
            fs.delete(path1, true);
        }
    }
}
