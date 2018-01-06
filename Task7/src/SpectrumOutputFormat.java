import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class SpectrumOutputFormat
extends FileOutputFormat<Text,RawSpectrum>
{
	@Override
	public RecordWriter<Text, RawSpectrum>
	getRecordWriter(TaskAttemptContext context)
	throws IOException, InterruptedException
	{
		Path outdir = FileOutputFormat.getOutputPath(context);
		String glusterPath = outdir.toUri().getPath();
		return new SpectrumRecordWriter(glusterPath);
	}
}
