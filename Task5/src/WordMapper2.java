import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.analysis.tokenattributes.*;

import java.io.*;
import java.util.*;

import task5.TextIntWritable;

public class WordMapper2
        extends Mapper<Object, Text, TextIntWritable, Text> {
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] input = value.toString().split("\t");
        int amount = Integer.parseInt(input[2]);
        context.write(new TextIntWritable(input[1], amount), new Text(input[0]));
    }


}