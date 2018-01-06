import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;

import task4.TextIntWritable;

public class Summer
extends Reducer<Text, Text, Text, TextIntWritable>
{
    public void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException
	{
        HashMap<Text, Integer>  map = new HashMap();
        Integer number;
        for(Text val : values)
        {
            number = map.get(val);
            if (number == null)
                map.put(val, new Integer(1));
            else
                map.put(val, number + 1);
        }

        Text result = new Text("");
        number = 0;
        for (Map.Entry<Text, Integer> entry : map.entrySet()){
            if ( entry.getValue() > number ) {
                number = entry.getValue();
                result = entry.getKey();
            }
        }
        TextIntWritable toWrite = new TextIntWritable(result.toString(), number);
        context.write(key, toWrite);
    }
}
