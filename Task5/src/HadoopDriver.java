import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;

import task5.TextIntWritable;

public class HadoopDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            ToolRunner.printGenericCommandUsage(System.err);
            System.err.println("USAGE: hadoop jar ... <input-dir> <output-dir>");
            System.exit(1);
        }

        String tmp_path = "tmp" + args[1];
        Job job = Job.getInstance(getConf());
        job.setJarByClass(HadoopDriver.class);
        job.setJobName("WordCounter");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(tmp_path));
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(Summer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(job)));
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(job));

        job.waitForCompletion(true);


        Job job2 = Job.getInstance(getConf());
        job2.setJarByClass(HadoopDriver.class);
        job2.setJobName("Sorting");
        job2.setMapperClass(WordMapper2.class);
        job2.setReducerClass(Summer2.class);

        job2.setMapOutputKeyClass(TextIntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(tmp_path));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        return job2.waitForCompletion(true)?0:1;
    }

}
