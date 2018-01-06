import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;

public class SpectrumMapper
extends Mapper<Text, RawSpectrum, Text, RawSpectrum>
{

    public void map(Text key, RawSpectrum value, Context context)
	throws IOException, InterruptedException
	{
		// limit no. of spectra to save space
//		if (counter < 5) {
//			++counter;
//        	context.write(key, value);
//		}
		context.write(key, value);
    }

	private int counter = 0;

}
