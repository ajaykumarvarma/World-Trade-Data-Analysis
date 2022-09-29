package com.samhad.compositekey;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountryYearCompositeKey implements WritableComparable<CountryYearCompositeKey> {

    private Text country = new Text();
    private Text year = new Text();

    public CountryYearCompositeKey() {
    }

    public CountryYearCompositeKey(Text country, Text year) {
        this.country.set(country);
        this.year.set(year);
    }

    public Text getCountry() {
        return country;
    }

    public void setCountry(Text country) {
        this.country = country;
    }

    public Text getYear() {
        return year;
    }

    public void setYear(Text year) {
        this.year = year;
    }

    @Override
    public int compareTo(CountryYearCompositeKey countryYearCompositeKey) {
        return ComparisonChain.start()
                .compare(country, countryYearCompositeKey.country)
                .compare(year, countryYearCompositeKey.year)
                .result();
    }

    @Override
    public void write(DataOutput out) throws IOException {

        country.write(out);
        year.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        country.readFields(in);
        year.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CountryYearCompositeKey that = (CountryYearCompositeKey) o;
        return Objects.equal(country, that.country) &&
                Objects.equal(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(country, year);
    }

    @Override
    public String toString() {
        return "country=" + country.toString() +
                ", year=" + year.toString();
    }
}
