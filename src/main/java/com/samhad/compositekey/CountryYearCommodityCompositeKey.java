package com.samhad.compositekey;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountryYearCommodityCompositeKey implements WritableComparable<CountryYearCommodityCompositeKey> {

    private Text country = new Text();
    private Text year = new Text();
    private Text commodity = new Text();

    public CountryYearCommodityCompositeKey() {
    }

    public CountryYearCommodityCompositeKey(Text country, Text year, Text commodity) {
        this.country.set(country);
        this.year.set(year);
        this.commodity.set(commodity);
    }

    public Text getCountry() {
        return country;
    }

    public void setCountry(Text country) {
        this.country.set(country);
    }

    public Text getYear() {
        return year;
    }

    public void setYear(Text year) {
        this.year.set(year);
    }

    public Text getCommodity() {
        return commodity;
    }

    public void setCommodity(Text commodity) {
        this.commodity.set(commodity);
    }

    @Override
    public int compareTo(CountryYearCommodityCompositeKey countryYearCommodityCompositeKey) {
        return ComparisonChain.start()
                .compare(country, countryYearCommodityCompositeKey.country)
                .compare(year, countryYearCommodityCompositeKey.year)
                .compare(commodity, countryYearCommodityCompositeKey.year)
                .result();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        country.write(out);
        year.write(out);
        commodity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        country.readFields(in);
        year.readFields(in);
        commodity.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CountryYearCommodityCompositeKey that = (CountryYearCommodityCompositeKey) o;
        return Objects.equal(country, that.country) &&
                Objects.equal(year, that.year) &&
                Objects.equal(commodity, that.commodity);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(country, year, commodity);
    }

    @Override
    public String toString() {
        return "country=" + country.toString() +
                ", year=" + year.toString() +
                ", commodity=" + commodity.toString();
    }
}
