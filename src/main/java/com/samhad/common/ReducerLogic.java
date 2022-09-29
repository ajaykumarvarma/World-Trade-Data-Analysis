package com.samhad.common;

import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ReducerLogic {

    public void commonReducer(WritableComparable key, Iterable<Text> values, Reducer.Context context)
            throws IOException, InterruptedException {

        Text contextText = new Text();
        Iterator<Text> iterator = values.iterator();
        double importValue = 0.0;
        double exportValue = 0.0;

        while (iterator.hasNext()) {

            String token = iterator.next().toString();

            String[] datumSplit = token.trim().split(" ");
            String mode = datumSplit[0].trim();
            double commodityValue = Double.parseDouble(datumSplit[1].trim());

            if (mode.equals("import")) {
                importValue += commodityValue;

            } else {
                exportValue += commodityValue;
            }
        }

        if (importValue != 0.0 || exportValue != 0.0) {

            String contextValue = " import " + Precision.round(importValue, 2) + "\texport " + Precision.round(exportValue, 2);
            contextText.set(contextValue);
            context.write(key, contextText);
        }
    }
}
