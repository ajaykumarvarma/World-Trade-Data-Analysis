package com.samhad.app;

import com.samhad.common.AppRunner;
import com.samhad.compositekey.CountryYearCompositeKey;
import com.samhad.mapper.ExportCYMapper;
import com.samhad.mapper.ImportCYMapper;
import com.samhad.reducer.JoinCYReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CYApp extends Configured implements Tool {

    public static void main(String[] args) {

        try {
            int res = ToolRunner.run(new Configuration(), new CYApp(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        AppRunner appRunner = new AppRunner();

        return appRunner.jobRunner(
                this.getConf(), args, CountryYearCompositeKey.class,
                CYApp.class, JoinCYReducer.class,
                ImportCYMapper.class, ExportCYMapper.class
        );
    }
}
