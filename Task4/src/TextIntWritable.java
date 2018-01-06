package task4;
import java.io.*;
import org.apache.hadoop.io.*;



public class TextIntWritable implements Writable {
    private String text;
    private Integer amount;

    public TextIntWritable(String text, Integer amount) {
        this.text = text;
        this.amount = amount;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(text);
        out.writeInt(amount);
    }

    public void readFields(DataInput in) throws IOException {
        text = in.readUTF();
        amount = in.readInt();
    }

    public String toString() {
        return text + " " + amount.toString();
    }
}
