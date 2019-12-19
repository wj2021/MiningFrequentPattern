package ex5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.*;
import java.util.*;

public class MiningFrequentPatternHadoop {
    public static void main(String[] args) throws Exception {
//        // 读取transcation
//        BufferedReader bufferReader = new BufferedReader(new FileReader("input/chess.dat"));
//        String line = null;
//        List<Transaction<Integer>> transactionList = new ArrayList<>();
//        while((line = bufferReader.readLine()) != null) {
//            transactionList.add(new Transaction<Integer>(line, " "));
//            String[] tokens = line.split(" ");
//        }
//        bufferReader.close();
//
//        Apriori<Integer> apriori = new Apriori<>(transactionList, 0.8);
//        Map<Set<Integer>, Integer> result = apriori.getFrequentPattern();
//        System.out.println(result.size());

        BasicConfigurator.configure(); // 使用默认的日志配置，可以在idea运行时显示日志

        // 参数验证
        if(args.length != 3) {
           System.out.println("The num of input arguments must be <input> <output> <support>");
           System.exit(-2);
        }
        if(Double.parseDouble(args[2]) < 0 || Double.parseDouble(args[2]) > 1 ) {
            System.out.println("support must between 0 and 1");
            System.exit(-3);
        }

        Configuration conf = new Configuration();
        conf.set("minSupp", args[2]);

        // 判断输出路径是否存在，如果存在，则删除
        Path outPath = new Path(args[1]);
        FileSystem hdfs = outPath.getFileSystem(conf);
        if (hdfs.isDirectory(outPath)) {
            hdfs.delete(outPath, true);
        }

        Job job = Job.getInstance(conf, "MiningFrequentPatternHadoop");
        job.setJarByClass(MiningFrequentPatternHadoop.class);
        job.setMapperClass(MiningFrequentPatternMapper.class);
        job.setReducerClass(MiningFrequentPatternReducer.class);

        job.setMapOutputKeyClass(ItemSetString.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(ItemSetString.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println(job.waitForCompletion(true) ? 0 : 1);
    }
}

class MiningFrequentPatternMapper extends Mapper<Object, Text, ItemSetString, IntWritable> {
    private ItemSetString outKey = new ItemSetString();
    private IntWritable outValue = new IntWritable();
    private List<Transaction<Integer>> transactionList = new ArrayList<>();

    private Apriori apriori = new Apriori();

    private double minSupp = 1.0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        minSupp = Double.parseDouble(context.getConfiguration().get("minSupp"));
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Transaction<Integer> transaction = new Transaction<>(value.toString(), " ");
        transactionList.add(transaction);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 计算局部频繁项集
        apriori.setTransactionList(transactionList);
        apriori.setMinSupp(0.8);
        Map<Set<Integer>, Integer> result = new HashMap<>();
        try {
            result = apriori.getFrequentPattern();
        } catch (Exception e) {
            e.printStackTrace();
        }
        for(Map.Entry<Set<Integer>, Integer> item : result.entrySet()) {
            outKey.set(item.getKey().toString());
            outValue.set(item.getValue());
            context.write(outKey, outValue);
        }
    }
}

class MiningFrequentPatternReducer extends Reducer<ItemSetString, IntWritable, ItemSetString, NullWritable> {
    @Override
    protected void reduce(ItemSetString key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}

class ItemSetString extends Text {
    private String[] init() {
        return this.toString().substring(1, this.getLength()-1).split(", ");
    }

    @Override
    public int hashCode() {
        return WritableComparator.hashBytes(this.getBytes(), this.getLength());
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int compareTo(byte[] other, int off, int len) {
        return super.compareTo(other, off, len);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}

