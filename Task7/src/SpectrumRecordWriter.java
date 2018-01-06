import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

import ucar.ma2.*;
import ucar.nc2.*;
import ucar.nc2.util.*;

public class SpectrumRecordWriter
extends RecordWriter<Text, RawSpectrum>
{
    private String path;
    private Map<String, CdfFileWriter> writersMap;
    private boolean start = true;


    public SpectrumRecordWriter(String path) throws IOException {
		this.path = path;
        writersMap = new HashMap<String, CdfFileWriter> ();
	}

	private CdfFileWriter getWriter(String nameOfFile) throws IOException {
        CdfFileWriter writer = writersMap.get(nameOfFile);
        if (writer != null) {
            return writer;
        } else {
            writer = new CdfFileWriter(nameOfFile);
            writersMap.put(nameOfFile, writer);
            return writer;
        }
    }

	@Override
	public void
	write(Text key, RawSpectrum value) throws IOException {
        String name = "/gfs" + path + "/" + value.getName() + ".nc";
        System.out.println("check: " + name);
        CdfFileWriter wr = getWriter(name);

        float[] i = value.getArray('i');
        float[] j = value.getArray('j');
        float[] k = value.getArray('k');
        float[] w = value.getArray('w');
        float[] d = value.getArray('d');


        for (int it = 0; it < i.length; it++) {
            wr.iArray.set(wr.iArray.getIndex().set(0, it), i[it]);
            wr.jArray.set(wr.jArray.getIndex().set(0, it), j[it]);
            wr.kArray.set(wr.kArray.getIndex().set(0, it), k[it]);
            wr.wArray.set(wr.wArray.getIndex().set(0, it), w[it]);
            wr.dArray.set(wr.dArray.getIndex().set(0, it), d[it]);
        }
        int[] origin = new int[]{0, 0};
        origin[0] = wr.timeIdx;

        try {
            wr.writer.write(wr.iVar, origin, wr.iArray);
            wr.writer.write(wr.jVar, origin, wr.jArray);
            wr.writer.write(wr.kVar, origin, wr.kArray);
            wr.writer.write(wr.wVar, origin, wr.wArray);
            wr.writer.write(wr.dVar, origin, wr.dArray);
        } catch (InvalidRangeException e){
            e.printStackTrace();
        }
        wr.timeIdx++;
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException{
        for (CdfFileWriter wr : writersMap.values()) {
            wr.writer.close();
        }
	}
}
