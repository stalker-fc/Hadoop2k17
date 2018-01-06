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

public class SpectrumRecordReader
        extends RecordReader<Text, RawSpectrum> {

    //возвращаемый результат для каждой записи - это одна сджойненная строка
    //ключ: дата + время измерения
    //значение: RawSpectrum - по сути, пять массивов float
    // line example: 2012 01 01 00 00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   ...
    private static final Pattern
            LINE_PATTERN = Pattern.compile("([0-9]{4} [0-9]{2} [0-9]{2} [0-9]{2} [0-9]{2})(.*)");


    private Text key;
    private RawSpectrum value;
    private Map<Text, RawSpectrum> values;
    private Iterator<Map.Entry<Text, RawSpectrum>> iterator;

    private int totalSize = 0;
    private int currentSize = 0;


    @Override
    public void close() throws IOException  {

    }


    @Override
    public Text getCurrentKey() {
        return key;
    }


    @Override
    public RawSpectrum getCurrentValue() {
        return value;
    }


    @Override
    public void
    initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException  {
        CombineFileSplit split = (CombineFileSplit) inputSplit;
        Path[] paths = split.getPaths();

        this.values = new HashMap<Text, RawSpectrum>();

        for (Path path : paths) {
            FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
            FSDataInputStream inputStream = fileSystem.open(path);
            GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
            LineReader reader = new LineReader(gzipInputStream, context.getConfiguration());

            Matcher fileNameMatcher = RawSpectrum.FILENAME_PATTERN.matcher(path.getName());
            char field = path.getName().charAt(5);
            if (fileNameMatcher.matches()) {
                Text line = new Text();
                Matcher lineMatcher;
                String variable = fileNameMatcher.group(2);

                while (reader.readLine(line) != 0) {
                    lineMatcher = LINE_PATTERN.matcher(line.toString());

                    if (lineMatcher.matches()) {
                        Text toAddKey = new Text(lineMatcher.group(1));
                        String floatsString = lineMatcher.group(2);
                        List<Float> floatsList = new ArrayList<Float>();
                        Scanner scanner = new Scanner(floatsString);
                        while (scanner.hasNextFloat()) {
                            floatsList.add(scanner.nextFloat());
                        }

                        float[] floats = new float[floatsList.size()];
                        for (int j = 0; j < floats.length; ++j) {
                            floats[j] = floatsList.get(j);
                        }
                        if (!values.containsKey(toAddKey)) {
                            RawSpectrum rawSpectrum = new RawSpectrum();
                            rawSpectrum.setField(path.getName().toString(), floats);
                            this.values.put(toAddKey, rawSpectrum);
                        } else {
                            this.values.get(toAddKey).setArray(field, floats);
                        }
                    }
                }
            }
            reader.close();
            gzipInputStream.close();
            fileSystem.close();
        }


        this.iterator = values.entrySet().iterator();
        this.totalSize = values.size();
    }


    @Override
    public boolean nextKeyValue() throws IOException {
        boolean next = false;
        while (!next && this.iterator.hasNext()) {
            this.currentSize += 1;
            Map.Entry entry = (Map.Entry) this.iterator.next();
            RawSpectrum rawSpectrum = (RawSpectrum) entry.getValue();
            float[] i = rawSpectrum.getArray('i');
            float[] j = rawSpectrum.getArray('j');
            float[] k = rawSpectrum.getArray('k');
            float[] w = rawSpectrum.getArray('w');
            float[] d = rawSpectrum.getArray('d');

            if (i != null && j != null && k != null && w != null && d != null) {
                next = true;
                key = (Text) entry.getKey();
                value = (RawSpectrum) entry.getValue();
            }
        }
        return next;
    }


    @Override
    public float getProgress() {
        if (totalSize == 0) {
            return 1f;
        } else {
            return (float) currentSize / (float) totalSize;
        }
    }

}
