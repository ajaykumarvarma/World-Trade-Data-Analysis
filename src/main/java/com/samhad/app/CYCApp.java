package com.samhad.app;

import com.samhad.common.AppRunner;
import com.samhad.compositekey.CountryYearCommodityCompositeKey;
import com.samhad.mapper.ExportCYCMapper;
import com.samhad.mapper.ImportCYCMapper;
import com.samhad.reducer.JoinCYCReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CYCApp extends Configured implements Tool {

    public static void main(String[] args) {

        try {
            int res = ToolRunner.run(new Configuration(), new CYCApp(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        AppRunner appRunner = new AppRunner();

        return appRunner.jobRunner(
                this.getConf(), args, CountryYearCommodityCompositeKey.class,
                CYCApp.class, JoinCYCReducer.class,
                ImportCYCMapper.class, ExportCYCMapper.class
        );
    }


}
