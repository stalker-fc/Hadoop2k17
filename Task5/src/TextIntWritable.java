package task5;
import java.io.*;
import org.apache.hadoop.io.*;



public class TextIntWritable implements WritableComparable<TextIntWritable> {
    // Some data
    private String text;
    private Integer amount;

    public TextIntWritable() {
        this.text = "";
        this.amount = 0;
    }

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

    public int compareTo(TextIntWritable o) {
        Integer thisValue = this.amount;
        Integer thatValue = o.amount;
        return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }

    public int hashCode() {
        final int prime = 1039;
        int result = 1;
        result = prime * amount + prime * text.hashCode();
        return result;
    }

    public String toString() {
        return text + "\t" + amount.toString();
    }
}