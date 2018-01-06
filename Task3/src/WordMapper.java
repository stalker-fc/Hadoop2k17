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
import java.text.BreakIterator;

public class WordMapper
        extends Mapper<IntWritable, Text, Text, Text> {
    public void map(IntWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Reader reader = new StringReader(value.toString());
        Analyzer analyzer = new StandardAnalyzer();

        TokenStream stream = analyzer.tokenStream(null, reader);

        stream.reset();
        String prev;
        String cur;
        if (stream.incrementToken()) {
            prev = stream
                    .getAttribute(CharTermAttribute.class)
                    .toString();
            while (stream.incrementToken()) {
                cur = stream
                        .getAttribute(CharTermAttribute.class)
                        .toString();
                context.write(new Text(prev), new Text(cur));
                prev = cur;
            }
        }
        stream.end();
        stream.close();
    }


    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        try {
            StringBuilder strings = new StringBuilder();
            while (context.nextKeyValue()) {
                Object tmp = context.getCurrentKey();
                Text value = context.getCurrentValue();
                strings.append(value.toString() + "\n");
            }
            String allValues = strings.toString();
            BreakIterator boundary = BreakIterator.getSentenceInstance();
            boundary.setText(allValues);
            int start = boundary.first();
            for (int end = boundary.next();
                 end != BreakIterator.DONE;
                 start = end, end = boundary.next()) {
                map(new IntWritable(0), new Text(allValues.substring(start, end)), context);
            }


        } finally {
            cleanup(context);
        }
    }
}
