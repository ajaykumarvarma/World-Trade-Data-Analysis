package com.samhad.common;

import com.samhad.compositekey.CountryYearCompositeKey;
import com.samhad.compositekey.CountryYearCommodityCompositeKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class GenericMapLogic {

    public void map(Text value, Mapper.Context context, boolean CYCLogic, boolean isImport) throws IOException, InterruptedException {

        Text valueText = new Text();
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");

        while (tokenizer.hasMoreElements()) {

            String token = tokenizer.nextToken();

            if (!token.equals("HSCode,Commodity,value,country,year")) {

                int foundPos = 0;
                boolean nullStat = true;
                String[] datum = token.split(",");
                StringBuilder commodityBuilder = new StringBuilder(datum[1].trim());

                for (int i = 2; i < datum.length; i++) {

                    if (datum[i].matches("\\d.\\d")) {
                        nullStat = false;
                        break;
                    } else {
                        foundPos++;
                        commodityBuilder.append(",").append(datum[i].trim());
                    }
                }

                if (!nullStat) {

                    String year = datum[4 + foundPos].trim();
                    String commodityValue = datum[2 + foundPos].trim();
                    String keyValue = datum[3 + foundPos].trim();

                    String val = isImport ? "import" : "export";
                    valueText.set(val + commodityValue);

                    if (CYCLogic) {
                        CountryYearCommodityCompositeKey compositeKey = new CountryYearCommodityCompositeKey(
                                new Text(keyValue), new Text(year), new Text(commodityBuilder.toString()));

                        context.write(compositeKey, valueText);
                    } else {
                        CountryYearCompositeKey compositeKey = new CountryYearCompositeKey(
                                new Text(keyValue), new Text(year));

                        context.write(compositeKey, valueText);
                    }
                }
            }
        }

    }
}
