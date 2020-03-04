package dataStructures;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * A Serializable POJO class used by the CVOPT FLink algorithm.
 * This class represents data tuples, statistics of group bys, and polling messages.
 * This class also contains the resulting sample of a strata.
 */
public class DataTuple implements Serializable {

    public String groupBy_attributes; // The concatenated group by attributes ("column1,column2,column3, ...")
    public boolean isStats; // A flag indicating if this Class instance represents the statistics of a stratum
    public boolean isPoll; // A flag indicating if this Class instance represents a polling message for this stratum
    // Polling messages are used to forward the key by timers

    public double aggregate_attribute; // The aggregate column for a group

    public Double stratum_mean; // The mean of the stratum
    public Double stratum_std; // The standard deviation of the stratum
    public Integer count; // The real size of the stratum

    public Integer si; // The memory allocated by the CVOPT algorithm for the sample of this stratum
    public Double sample_mean; // The mean of the sample of the stratum
    public Double sample_std; // The standard deviation of the sample of the stratum
    public ArrayList<DataTuple> stratum_sample; // The stratum sample

    public DataTuple() {
    }

    public DataTuple(String groupBy_attributes) {
        this.groupBy_attributes = groupBy_attributes;
        this.isStats = false;
        this.isPoll = true;
    }

    public DataTuple(String groupBy_attributes, boolean isStats, Integer si) {
        this.groupBy_attributes = groupBy_attributes;
        this.isStats = isStats;
        this.isPoll = false;
        this.si = si;
    }

    public DataTuple(String groupBy_attributes, double aggregate_attribute) {
        this.groupBy_attributes = groupBy_attributes;
        this.aggregate_attribute = aggregate_attribute;
        this.isStats = false;
        this.isPoll = false;
    }

    public boolean isStats() {
        return isStats;
    }

    public void setStats(boolean stats) {
        this.isStats = stats;
    }

    public boolean isPoll() {
        return isPoll;
    }

    public void setPoll(boolean poll) {
        isPoll = poll;
    }

    public String getGroupBy_attributes() {
        return groupBy_attributes;
    }

    public void setGroupBy_attributes(String groupBy_attributes) {
        this.groupBy_attributes = groupBy_attributes;
    }

    public double getAggregate_attribute() {
        return aggregate_attribute;
    }

    public void setAggregate_attribute(double aggregate_attribute) {
        this.aggregate_attribute = aggregate_attribute;
    }

    public Double getStratum_mean() {
        return stratum_mean;
    }

    public void setStratum_mean(Double stratum_mean) {
        this.stratum_mean = stratum_mean;
    }

    public Double getStratum_std() {
        return stratum_std;
    }

    public void setStratum_std(Double stratum_std) {
        this.stratum_std = stratum_std;
    }

    public Integer getSi() {
        return si;
    }

    public void setSi(Integer si) {
        this.si = si;
    }

    public Double getSample_mean() {
        return sample_mean;
    }

    public void setSample_mean(Double sample_mean) {
        this.sample_mean = sample_mean;
    }

    public Double getSample_std() {
        return sample_std;
    }

    public void setSample_std(Double sample_std) {
        this.sample_std = sample_std;
    }

    public ArrayList<DataTuple> getStratum_sample() {
        return stratum_sample;
    }

    public void setStratum_sample(ArrayList<DataTuple> stratum_sample) {
        this.stratum_sample = stratum_sample;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    /**
     * This function returns the final results of each Flink Job
     *
     * @return The string format of the final result of each Job
     */
    @Override
    public String toString() {
        if (isStats)
            return groupBy_attributes + ", " +
                    stratum_mean + ", " +
                    stratum_std + ", " +
                    count + ", " +
                    si;
        else if (isPoll)
            return "Polling record for group by " + groupBy_attributes + "\n";
        else // This is the result
            return groupBy_attributes + ", " +
                    stratum_mean + ", " +
                    stratum_std + ", " +
                    count + ", " +
                    si + ", " +
                    stratum_sample.size() + ", " +
                    sample_mean + ", " +
                    sample_std + ", " +
                    "\n" + sampleToString() + "\n";
    }

    public String sampleToString() {
        if (stratum_sample != null) {
            String[] attributesArray = groupBy_attributes.split(",");
            StringBuilder attributes = new StringBuilder();
            for (String attribute : attributesArray) attributes.append(attribute).append(", ");
            StringBuilder print = new StringBuilder();
            for (DataTuple sample : stratum_sample) {
                assert (sample.groupBy_attributes.equals(groupBy_attributes));
                print.append(attributes.toString()).append(sample.getAggregate_attribute()).append("\n");
            }
            return print.toString();
        } else return "not a sample data structure";
    }
}