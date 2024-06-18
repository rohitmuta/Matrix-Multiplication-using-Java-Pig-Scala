
/*****  Code by Rohit Mahuli  *****/

/*****       1002091557       *****/

/***  How to run(in terminal)
 * :$: hadoop jar myproj8.jar testinputA testinputB interoutputdirectory reduceoutputdirectory  ***/



 import java.io.*;
import java.util.*;
//import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
//import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;
//import java.io.File;
//import java.util.ArrayList;
//import java.util.List;
//import org.apache.hadoop.util.*;
//import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.*;

class Element implements Writable
{
    public short tag;  // 1 for N, 0 for M;
    public int indec;
    public double valu;
    Element () {}

    Element ( short t, int in, double val ) {
        tag=t; indec = in; valu = val;
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
        indec = in.readInt();
        valu = in.readDouble();
    }
    public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        out.writeInt(indec);
        out.writeDouble(valu);
    }

}
class Pair implements WritableComparable<Pair>
{
    int i;
    int j;

    Pair () {}
/*
    Pair (int indexi , int indexj)
    {
        i=indexi;
        j=indexj;

    }


 */

    public void readFields ( DataInput in ) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }
    public void write ( DataOutput out ) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }

    public int compareTo(Pair pr)
    {
        int s = this.i - pr.i;
        if(s==0) {
            return (this.j - pr.j);
        }
        return s;
    }
}
public class Multiply {
    public static class MapperforM extends Mapper < Object,Text,IntWritable,Element > {

        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            int v = 0;
            short t = 0;
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            Element e = new Element();
            e.tag=t;
            e.indec=s.nextInt();
            v= s.nextInt();
            e.valu=s.nextDouble();
            context.write(new IntWritable(v),e);	//first map reduce job output : {v, (M, i, miv))
            s.close();
        }
    }

    public static class MapperforN extends Mapper<Object,Text,IntWritable,Element > {

        //@Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            int z=0;
            short t =1;
            Scanner g = new Scanner(value.toString()).useDelimiter(",");
            Element e = new Element();
            e.tag=t;
            z= g.nextInt();			//j
            e.indec=g.nextInt();	//k
            e.valu=g.nextDouble(); //value
            context.write(new IntWritable(z),e);
            g.close();
        }
    }
    public static class reduce extends Reducer<IntWritable,Element,Text,Text>
    {
        static Vector<Element> S = new Vector<Element>();
        static Vector<Element> R = new Vector<Element>();
        int i=0,j=0;

        //@Override
        public void reduce ( IntWritable key, Iterable<Element> values, Context context )
                throws IOException, InterruptedException {
            R.clear();
            S.clear();
            for (Element v: values)
            {
                if (v.tag == 0)
                {
                    Element el = new Element(v.tag,v.indec,v.valu);
                    R.add(el);
                }

                else
                {
                    Element d = new Element(v.tag,v.indec,v.valu);
                    S.add(d);
                }
            }

            for ( Element a: R )
            {
                for ( Element b: S )
                {
                    Text ky = new Text(a.indec+","+b.indec);
                    Pair pr = new Pair();
                    pr.i=a.indec;
                    pr.j=b.indec;
                    Text value= new Text(Double.toString(a.valu*b.valu));
                    context.write(ky,value);
                }
            }
        }
    }

    public static class IdentityMapper extends Mapper<Text,Text,Text,Text > {
        public void map ( Text key, Text value, Context context )
                throws IOException, InterruptedException
        {
            context.write(key,value);	//second map reduce job output(j, (N, k, njk))

        }
    }

    public static class ReducerforSum extends Reducer<Text,Text,Text,Text> {
        public void reduce ( Text key, Iterable<Text> values, Context context )
                throws IOException, InterruptedException {
            double summ = 0.0;
            //File myObj = new File("filename.txt");
            //FileWriter myWriter = new FileWriter("filename.txt");
            for (Text v: values)
            {
                summ +=  Double.valueOf(v.toString());
            }
            Text tzt = new Text(String.valueOf(summ));
            context.write(key,tzt);
           // myWriter.write(String.valueOf(t));
            //PrintWriter pw = new PrintWriter("solution-small.txt");
           // pw.println("abc"+ context.getValues());
           // pw.flush();
          //  pw.close();
        }


    }


    public static void main ( String[] args ) throws Exception
    {

        Configuration config = new Configuration();

        Job job1x = Job.getInstance(config,"Job 1"); //driver code contains configuration details
        job1x.setJobName("Matrix Multiplication");
        job1x.setJarByClass(Multiply.class);

        /**  mapper output format  **/
        job1x.setMapOutputKeyClass(IntWritable.class);
        job1x.setMapOutputValueClass(Element.class);

        /**   reducer output format  **/
        job1x.setOutputKeyClass(Text.class);
        job1x.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1x,new Path(args[0]),TextInputFormat.class,MapperforM.class);
        MultipleInputs.addInputPath(job1x,new Path(args[1]),TextInputFormat.class,MapperforN.class);
        job1x.setReducerClass(reduce.class);


        /** intermediate output  **/
        FileOutputFormat.setOutputPath(job1x,new Path(args[2])); /** directory where the output will be written onto **/
        job1x.waitForCompletion(true);

        //job2
        Job job2y = Job.getInstance(config,"Job 2");
        job2y.setJobName("Matrix Multiplication(Sum)");
        job2y.setJarByClass(Multiply.class);

        job2y.setMapOutputKeyClass(Text.class);
        job2y.setMapOutputValueClass(Text.class);

        job2y.setMapperClass(IdentityMapper.class);
        job2y.setReducerClass(ReducerforSum.class);

        job2y.setOutputKeyClass(Text.class);
        job2y.setOutputValueClass(Text.class);

        job2y.setInputFormatClass(KeyValueTextInputFormat.class);

        /** directory from where it will fetch input file **/
        FileInputFormat.setInputPaths(job2y,new Path(args[2]));  // input directory
        FileOutputFormat.setOutputPath(job2y,new Path(args[3])); // output directory
        //FileOutputFormat.setOutputPath(job2y,new Path(simple.txt)); // output directory
        job2y.getConfiguration().set("mapreduce.output.basename", "solution-small");

        job2y.waitForCompletion(true);
    }//main


}//multiply