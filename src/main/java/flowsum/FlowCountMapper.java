package flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    FlowBean v = new FlowBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();
        //2.切割

        String[] fields = line.split("\t");
        //3封装对象
        //1.取出手机号
        String phoneNum = fields[1];
        //2.取出上行流量和下行流量
        long upflow = Long.parseLong(fields[fields.length - 3]);
        long downflow = Long.parseLong(fields[fields.length - 2]);

        k.set(phoneNum);
        v.set(upflow,downflow);
        //4写出
        context.write(k,v);
    }
}
