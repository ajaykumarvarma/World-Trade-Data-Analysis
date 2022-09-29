package com.samhad.reducer;

import com.samhad.common.ReducerLogic;
import com.samhad.compositekey.CountryYearCommodityCompositeKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinCYCReducer extends Reducer<CountryYearCommodityCompositeKey, Text, CountryYearCommodityCompositeKey, Text> {

    @Override
    protected void reduce(CountryYearCommodityCompositeKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        ReducerLogic reducerLogic = new ReducerLogic();
        reducerLogic.commonReducer(key, values, context);
    }
}
