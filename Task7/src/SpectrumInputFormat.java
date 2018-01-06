import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class SpectrumInputFormat
        extends InputFormat<Text, RawSpectrum> {

    //здесь нужно вернуть массив сплитов
    @Override
    public List<InputSplit>
    getSplits(JobContext context)
            throws IOException, InterruptedException {
        Map<String, List<FileStatus>> fileNames = new HashMap<>();
        for (Path path : FileInputFormat.getInputPaths(context)) {
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            for (FileStatus file : fs.listStatus(path)) {
                String name = file.getPath().getName().toString();
                String key = name.substring(0, 5) + name.substring(6, 10); //num of station + year
                if (!fileNames.containsKey(key)) {
                    List<FileStatus> files = new ArrayList<FileStatus>();
                    files.add(file);
                    fileNames.put(key, files);
                } else {
                    fileNames.get(key).add(file);
                }
            }
        }

        List<InputSplit> inputSplits = new ArrayList<InputSplit>();
        for (Map.Entry<String, List<FileStatus>> split : fileNames.entrySet()) {

            if (split.getValue().size() == 5) {
                Path[] paths = new Path[5];
                long[] fileSizes = new long[5];

                FileStatus[] fileStatuses = new FileStatus[5];
                fileStatuses = split.getValue().toArray(fileStatuses);
                for (int i = 0; i < 5; i++) {
                    paths[i] = fileStatuses[i].getPath();
                    fileSizes[i] = fileStatuses[i].getLen();
                }

                inputSplits.add(new CombineFileSplit(paths, fileSizes));
            }
        }
        return inputSplits;
    }

    @Override
    public RecordReader<Text, RawSpectrum>
    createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new SpectrumRecordReader();
    }

    private static long[]
    getFileLengths(List<Path> files, JobContext context)
            throws IOException {
        long[] lengths = new long[files.size()];
        for (int i = 0; i < files.size(); ++i) {
            FileSystem fs = files.get(i).getFileSystem(context.getConfiguration());
            lengths[i] = fs.getFileStatus(files.get(i)).getLen();
        }
        return lengths;
    }
}
