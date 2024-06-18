
/***   cODE bY ROHIT M    ***/
/***   CSE 6332 CLOUD N BIG D   ***/
/***   hOW tO eXECUTE - (One-step Map reduce)
 ***   hadoop jar multiply.jar inputA inputB output  ***/


import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Multiply {
    public static class MapforM
            extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int p = Integer.parseInt(conf.get("p"));
            String line = value.toString();
            // (i, j, Aij);
            String[] indicesAndValue = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();
            for (int k = 0; k < p; k++) {
                outputKey.set(indicesAndValue[0] + "," + k);
                // outputKey.set(i,k);
                outputValue.set("R" + "," + indicesAndValue[1]
                        + "," + indicesAndValue[2]);
                // outputValue.set(R,j,Rij);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class MapforN
            extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int m = Integer.parseInt(conf.get("m"));
            String line = value.toString();
            // (j, k, Bjk);
            String[] indicesAndValue = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();
                for (int i = 0; i < m; i++) {
                    outputKey.set(i + "," + indicesAndValue[1]);
                    outputValue.set("S," + indicesAndValue[0] + ","
                            + indicesAndValue[2]);
                    context.write(outputKey, outputValue);
                }

        }
    }


    public static class Reduce
            extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String[] value;
            //key=(i,k),
            HashMap<Integer, Float> hashR = new HashMap<Integer, Float>();
            HashMap<Integer, Float> hashS = new HashMap<Integer, Float>();
            for (Text val : values) {
                value = val.toString().split(",");
                if (value[0].equals("R")) {
                    hashR.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                } else {
                    hashS.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                }
            }
            int n = Integer.parseInt(context.getConfiguration().get("n"));
            float result = 0.0f;
            float m_ij;
            float n_jk;
            for (int j = 0; j < n; j++) {
                m_ij = hashR.containsKey(j) ? hashR.get(j) : 0.0f;
                n_jk = hashS.containsKey(j) ? hashS.get(j) : 0.0f;
                result += m_ij * n_jk;
            }
            if (result != 0.0f) {
                context.write(null,
                        new Text(key.toString() + "," + Float.toString(result)));
            }
            else{
                System.out.println("Error Occurred");
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // M is an m-by-n matrix; N is an n-by-p matrix.
        conf.set("m", "2");     // row of Matrix M
        conf.set("n", "2");     // column of Matrix M == row of Matrix N
        conf.set("p", "2");     // column of Matrix N
        Job job = new Job(conf, "MatrixMultiply");
        job.setJarByClass(Multiply.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MapforM.class);
        job.setMapperClass(MapforN.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MapforM.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,MapforN.class);


        //FileInputFormat.addInputPath(job, new Path(args[1])); //rohit
        //MultipleOutputs.addNamedOutput();
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.getConfiguration().set("mapreduce.output.basename", "solution-small");
        //FileOutputFormat.setOutputPath(job,new Path(simple.txt)); // output directory

        job.waitForCompletion(true);
    }
}