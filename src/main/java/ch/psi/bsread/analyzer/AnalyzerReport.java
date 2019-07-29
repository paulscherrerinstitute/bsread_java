package ch.psi.bsread.analyzer;

public class AnalyzerReport {

    private final int SIZE_HISTOGRAM_PULSE_ID_INCREMENTS = 100;
    private final int SIZE_HISTOGRAM_DELAYS = 10000; // 10 seconds

    private int numberOfMessages = 0;
    // number of correct messages will always be one less than number of messages because the first message cannot be fully checked for validity
    private int numberOfCorrectMessages = 0;

    private int zeroPulseIds = 0;
    private int duplicatedPulseIds = 0;
    private int pulseIdsBeforeLastValid = 0;

    private int globalTimestampOutOfValidTimeRange = 0;
    private int duplicatedGlobalTimestamp = 0;
    private int globalTimestampBeforeLastValid = 0;

    // histogram of pulse-id increments - // last index holds all others
    private int[] histogramPulseIdIncrements = new int[SIZE_HISTOGRAM_PULSE_ID_INCREMENTS+1];

    // histogram of delays in ms - last index holds all other delays, index 0  holds delays equal and lower to 0
    // Note: Maybe save full delay list to spot periodicity of delays?
    private int[] histogramDelays = new int[SIZE_HISTOGRAM_DELAYS+1]; //


    public void incrementNumberOfMessages(){
        numberOfMessages++;
    }
    public void incrementNumberOfCorrectMessages(){
        numberOfCorrectMessages++;
    }
    public void incrementZeroPulseIds(){
        zeroPulseIds++;
    }
    public void incrementDuplicatedPulseIds(){
        duplicatedPulseIds++;
    }
    public void incrementPulseIdsBeforeLastValid(){
        pulseIdsBeforeLastValid++;
    }
    public void incrementGlobalTimestampOutOfValidTimeRange(){ globalTimestampOutOfValidTimeRange++; }
    public void incrementGlobalTimestampBeforeLastValid(){
        globalTimestampBeforeLastValid++;
    }
    public void incrementDuplicatedGlobalTimestamp(){
        duplicatedGlobalTimestamp++;
    }

    public void updateHistogramPulseIdIncrements(int increment){
        if(increment<SIZE_HISTOGRAM_PULSE_ID_INCREMENTS) {
            histogramPulseIdIncrements[increment] += 1;
        }
        else{
            // last index holds count for all other increments
            histogramPulseIdIncrements[SIZE_HISTOGRAM_PULSE_ID_INCREMENTS] += 1;
        }
    }

    public void updateHistogramDelays(int delay){
        if(delay <=0){
            histogramDelays[0] += 1;
        }
        else if(delay<SIZE_HISTOGRAM_DELAYS) {
            histogramDelays[delay] += 1;
        }
        else{
            // last index holds count for all larger delays
            histogramDelays[SIZE_HISTOGRAM_DELAYS] += 1;
        }
    }

    public int getNumberOfMessages() {
        return numberOfMessages;
    }

    public int getNumberOfCorrectMessages() {
        return numberOfCorrectMessages;
    }

    public int getZeroPulseIds() {
        return zeroPulseIds;
    }

    public int getDuplicatedPulseIds() {
        return duplicatedPulseIds;
    }

    public int getPulseIdsBeforeLastValid() {
        return pulseIdsBeforeLastValid;
    }

    public int getGlobalTimestampOutOfValidTimeRange() {
        return globalTimestampOutOfValidTimeRange;
    }

    public int getDuplicatedGlobalTimestamp() {
        return duplicatedGlobalTimestamp;
    }

    public int getGlobalTimestampBeforeLastValid() {
        return globalTimestampBeforeLastValid;
    }

    public int[] getHistogramPulseIdIncrements() {
        return histogramPulseIdIncrements;
    }

    public int[] getHistogramDelays() {
        return histogramDelays;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Number of messages: %d\n", numberOfMessages));
        sb.append(String.format("Number of correct messages: %d\n", numberOfCorrectMessages));

        sb.append(String.format("Zero pulse-ids: %d\n", zeroPulseIds));
        sb.append(String.format("Duplicated pulse-ids: %d\n", duplicatedPulseIds));
        sb.append(String.format("Pulse-ids before last valid: %d\n", pulseIdsBeforeLastValid));

        sb.append(String.format("Global-timestamp out of valid timerange: %d\n", globalTimestampOutOfValidTimeRange));
        sb.append(String.format("Duplicated global-timestamp: %d\n", duplicatedGlobalTimestamp));
        sb.append(String.format("Global-timestamp before last valid: %d\n", globalTimestampBeforeLastValid));

        return sb.toString();
    }
}
