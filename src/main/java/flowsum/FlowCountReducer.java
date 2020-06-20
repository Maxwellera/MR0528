package flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long sum_upflow = 0;
        long sum_downflow = 0;

        //1.遍历所有的bean,取出所有的upflow,downflow 分别求和
        for (FlowBean flowBean : values) {
            sum_upflow += flowBean.getUpFlow();
            sum_downflow += flowBean.getDownFlow();
        }

        v.set(sum_upflow,sum_downflow);

        //3写出
        context.write(key,v);
    }
}
