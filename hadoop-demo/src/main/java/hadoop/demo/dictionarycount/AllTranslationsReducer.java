package hadoop.demo.dictionarycount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AllTranslationsReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String translations = "";

        for (Text val : values) {
            translations += "|" + val.toString();
        }

        result.set(translations);
        context.write(key, result);
    }
}