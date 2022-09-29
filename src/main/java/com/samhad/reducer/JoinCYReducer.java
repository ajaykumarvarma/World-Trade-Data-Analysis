package com.samhad.reducer;

import com.samhad.common.ReducerLogic;
import com.samhad.compositekey.CountryYearCompositeKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinCYReducer extends Reducer<CountryYearCompositeKey, Text, CountryYearCompositeKey, Text> {

    @Override
    protected void reduce(CountryYearCompositeKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        ReducerLogic reducerLogic = new ReducerLogic();
        reducerLogic.commonReducer(key, values, context);
    }
}
