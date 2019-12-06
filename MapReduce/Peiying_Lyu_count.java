import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Peiying_Lyu_count {

    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
        //输入key类型、输入value类型、输出key类型、输出value类型
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 得到每一行的数据
            String line=value.toString();
            String [] words = line.split(",");
            // 通过逗号分隔
            // int max = value.get();
            // if(Integer.parseInt(words[2]) >= max){
            String bar_ = words[0];
                // int price_ = words[2];
            if(words[1].toLowerCase().startsWith("bud")){
                context.write(new Text(bar_),new LongWritable(Integer.parseInt(words[2])));
            }
            // }
        }
    }



    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,Context context)
                throws IOException, InterruptedException {
            int cnt = 0;
            long max_ = Integer.MIN_VALUE;
            for (LongWritable value : values){
                cnt ++;
                max_ = Math.max(max_ , value.get());
            }
            if (max_<=5){
                context.write(key, new LongWritable(cnt));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf0 = new Configuration();
        Job job0 = new Job(conf0, "Sells");
        job0.setJarByClass(Peiying_Lyu_count.class);
        job0.setMapperClass(Peiying_Lyu_count.Map.class);
        job0.setReducerClass(Peiying_Lyu_count.Reduce.class);

        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job0, new Path(args[0]));
        FileOutputFormat.setOutputPath(job0, new Path(args[1]));

        job0.waitForCompletion(true);

    }


}