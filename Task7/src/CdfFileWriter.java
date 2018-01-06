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


public class CdfFileWriter {
    public NetcdfFileWriter writer;
    public int timeIdx = 0;

    public ArrayFloat.D2 iArray;
    public ArrayFloat.D2 jArray;
    public ArrayFloat.D2 kArray;
    public ArrayFloat.D2 wArray;
    public ArrayFloat.D2 dArray;

    public Variable iVar;
    public Variable jVar;
    public Variable kVar;
    public Variable wVar;
    public Variable dVar;

    public CdfFileWriter() { }

    public CdfFileWriter(String nameOfFile) throws IOException {
        writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, nameOfFile);
        Dimension time = writer.addUnlimitedDimension("time");
        Dimension frequency = writer.addDimension(null, "frequency", 47);

        dVar = writer.addVariable(null, "d", DataType.FLOAT, "time frequency");
        iVar = writer.addVariable(null, "i", DataType.FLOAT, "time frequency");
        jVar = writer.addVariable(null, "j", DataType.FLOAT, "time frequency");
        kVar = writer.addVariable(null, "k", DataType.FLOAT, "time frequency");
        wVar = writer.addVariable(null, "w", DataType.FLOAT, "time frequency");

        iArray = new ArrayFloat.D2(1, frequency.getLength());
        jArray = new ArrayFloat.D2(1, frequency.getLength());
        kArray = new ArrayFloat.D2(1, frequency.getLength());
        wArray = new ArrayFloat.D2(1, frequency.getLength());
        dArray = new ArrayFloat.D2(1, frequency.getLength());

        writer.create();
    }

    public NetcdfFileWriter getWriter() {
        return this.writer;
    }

    public ArrayFloat.D2 getArray(char field) {
        if (field == 'i') return iArray;
        if (field == 'j') return jArray;
        if (field == 'k') return kArray;
        if (field == 'w') return wArray;
        if (field == 'd') return dArray;
        return null;
    }
}