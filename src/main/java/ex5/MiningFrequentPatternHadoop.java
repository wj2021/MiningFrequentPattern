package ex5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.log4j.BasicConfigurator;

import java.io.*;
import java.net.URI;
import java.util.*;

public class MiningFrequentPatternHadoop {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); // 使用默认的日志配置，可以在idea运行时显示日志

        // 参数验证
        if(args.length != 4) {
           System.out.println("The num of input arguments must be <input> <outputCandidate> <output> <support>");
           System.exit(-2);
        }
        if(Double.parseDouble(args[3]) < 0 || Double.parseDouble(args[3]) > 1 ) {
            System.out.println("support must between 0 and 1");
            System.exit(-3);
        }

        Configuration conf = new Configuration();
        conf.set("minSupp", args[3]);

        // 判断输出路径是否存在，如果存在，则删除
        FileSystem hdfs = FileSystem.get(conf);
        Path outPath = new Path(args[1]);
        if (hdfs.isDirectory(outPath)) {
            hdfs.delete(outPath, true);
        }
        outPath = new Path(args[2]);
        if (hdfs.isDirectory(outPath)) {
            hdfs.delete(outPath, true);
        }

        Job job = Job.getInstance(conf, "MiningFrequentPatternHadoop-step1");
        job.setJarByClass(MiningFrequentPatternHadoop.class);
        job.setMapperClass(MiningFrequentPatternMapper.class);
        job.setReducerClass(MiningFrequentPatternReducer.class);

        job.setMapOutputKeyClass(ItemSetText.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(ItemSetText.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Job job2 = Job.getInstance(conf, "MiningFrequentPatternHadoop-step2");
        job2.setJarByClass(MiningFrequentPatternHadoop.class);
        job2.setMapperClass(MiningFrequentPatternMapper2.class);
        job2.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(MiningFrequentPatternReducer2.class);

        job2.setMapOutputKeyClass(ItemSetText.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(ItemSetText.class);
        job2.setOutputKeyClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        if(job.waitForCompletion(true)) {
            //将job的结果存到cache中去
            FileStatus[] status = hdfs.listStatus(new Path(args[1]));
            for(FileStatus fs : status) {
                job2.addCacheFile(new URI(args[1] + "/" + fs.getPath().getName()));
            }
            System.out.println(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}

class MiningFrequentPatternMapper extends Mapper<LongWritable, Text, ItemSetText, IntWritable> {
    private ItemSetText outKey = new ItemSetText();
    private IntWritable outValue = new IntWritable();
    private List<Transaction<Integer>> transactionList = new ArrayList<>();

    private double minSupp = 1.0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        minSupp = Double.parseDouble(context.getConfiguration().get("minSupp"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Transaction<Integer> transaction = new Transaction<>(value.toString(), "\\s+");
        transactionList.add(transaction);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Apriori<Integer> apriori = new Apriori<>(transactionList, minSupp);
        // 计算局部频繁项集
        Map<Set<Integer>, Integer> result = new HashMap<>();
        try {
            result = apriori.getFrequentPattern();
        } catch (Exception e) {
            e.printStackTrace();
        }
        for(Map.Entry<Set<Integer>, Integer> item : result.entrySet()) {
            String key = item.getKey().toString();
            outKey.set(key);
            outValue.set(item.getValue());
            context.write(outKey, outValue);
        }
    }
}

class MiningFrequentPatternReducer extends Reducer<ItemSetText, IntWritable, ItemSetText, NullWritable> {
    @Override
    protected void reduce(ItemSetText key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}

class MiningFrequentPatternMapper2 extends Mapper<LongWritable, Text, ItemSetText, IntWritable> {
    private ItemSetText outKey = new ItemSetText();
    private IntWritable outValue = new IntWritable(1);

    private List<List<String>> candidateList = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 读取cache中的文件
        URI[] uris = context.getCacheFiles();
        FileSystem hdfs = FileSystem.get(new Configuration());
        for(URI uri : uris) {
            FSDataInputStream inputStream = hdfs.open(new Path(uri.toString()));
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while((line = reader.readLine()) != null && line.length() > 0) {
                List<String> items = new ArrayList<>(Arrays.asList(line.substring(1, line.length()-1).split(",\\s+")));
                candidateList.add(items);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");
        List<String> tokenList = new ArrayList<>(Arrays.asList(tokens));
        for(List<String> candidate : candidateList) {
            if(tokenList.containsAll(candidate)) {
                outKey.set(candidate.toString());
                context.write(outKey, outValue);
            }
        }
    }
}

class MiningFrequentPatternReducer2 extends Reducer<ItemSetText, IntWritable, ItemSetText, DoubleWritable> {
    private DoubleWritable outValue = new DoubleWritable();
    @Override
    protected void reduce(ItemSetText key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable v : values) {
            sum += v.get();
        }
        outValue.set(sum);
        context.write(key, outValue);
    }
}

/**
 * 自定义key
  */
class ItemSetText implements WritableComparable<ItemSetText> {
    private Text itemSet = new Text();

    public ItemSetText(){}

    public ItemSetText(String str) {
        itemSet.set(str);
    }

    public ItemSetText(Text text) {
        itemSet = text;
    }

    public Text getItemSet() {
        return itemSet;
    }

    public void setItemSet(Text itemSet) {
        this.itemSet = itemSet;
    }

    public void set(String str) {
        itemSet.set(str);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        itemSet.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        itemSet.write(dataOutput);
    }

    @Override
    public int hashCode() {
        String[] tokens = this.toString().substring(1, this.toString().length()-1).split(",\\s+");
        return Arrays.hashCode(tokens);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ItemSetText that = (ItemSetText) o;
        return this.compareTo(that) == 0;
    }

    @Override
    public int compareTo(ItemSetText o) {
        String key1 = this.getItemSet().toString();
        String key2 = o.getItemSet().toString();
        String[] tokens = key1.substring(1, key1.length()-1).split(",\\s+");
        String[] otherTokens = key2.substring(1,key2.length()-1).split(",\\s+");

        if(tokens.length != otherTokens.length) {
            return tokens.length - otherTokens.length > 0 ? 1 : -1;
        }

        // 排序
        List<String> a = new ArrayList<>(Arrays.asList(tokens));
        List<String> b = new ArrayList<>(Arrays.asList(otherTokens));
        a.sort(this::compareString);
        b.sort(this::compareString);

        for(int i = 0; i < a.size(); ++i) {
            int ans = compareString(a.get(i), b.get(i));
            if(ans != 0) {
                return ans;
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        return itemSet.toString();
    }

    // 比较两个字符串的大小
    // 长度长的字符串大，
    // 否则比较字符串中的每个字符，ASCII码大的字符串大
    private int compareString(String o1, String o2) {
        int len1 = o1.length();
        int len2 = o2.length();
        if(len1 != len2) {
            return len1 - len2 > 0 ? 1 : -1;
        } else {
            char[] o1Chs = o1.toCharArray();
            char[] o2Chs = o2.toCharArray();
            for(int i = 0; i < o1Chs.length; ++i) {
                if (o1Chs[i] != o2Chs[i]) {
                    return o1Chs[i] - o2Chs[i] > 0 ? 1 : -1;
                }
            }
            return 0;
        }
    }

}

