package com.samhad.mapper;

import com.samhad.common.GenericMapLogic;
import com.samhad.compositekey.CountryYearCommodityCompositeKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ExportCYCMapper extends Mapper<LongWritable, Text, CountryYearCommodityCompositeKey, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        GenericMapLogic genericImportMapLogic = new GenericMapLogic();
        genericImportMapLogic.map(value, context, true,false);
    }
}

