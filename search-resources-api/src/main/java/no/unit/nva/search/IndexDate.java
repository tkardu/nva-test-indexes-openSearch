package no.unit.nva.search;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import nva.commons.utils.JacocoGenerated;

import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public class IndexDate {

    private String year;
    private String month;
    private String day;

    public IndexDate() {

    }

    private IndexDate(Builder builder) {
        setYear(builder.year);
        setMonth(builder.month);
        setDay(builder.day);
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    @JacocoGenerated
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexDate)) {
            return false;
        }
        IndexDate date = (IndexDate) o;
        return Objects.equals(getYear(), date.getYear())
                && Objects.equals(getMonth(), date.getMonth())
                && Objects.equals(getDay(), date.getDay());
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(getYear(), getMonth(), getDay());
    }

    public static final class Builder {
        private String year;
        private String month;
        private String day;

        public Builder() {
        }

        public Builder withYear(String year) {
            this.year = year;
            return this;
        }

        public Builder withMonth(String month) {
            this.month = month;
            return this;
        }

        public Builder withDay(String day) {
            this.day = day;
            return this;
        }

        public IndexDate build() {
            return new IndexDate(this);
        }
    }
}

