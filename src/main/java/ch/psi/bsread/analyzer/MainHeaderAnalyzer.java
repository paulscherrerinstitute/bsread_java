package ch.psi.bsread.analyzer;

import ch.psi.bsread.message.MainHeader;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * MainHeader validator - checks if the header information (pulse-id and global timestamp)
 * are within certain constraints
 *
 * The basic assumption for this validator to work correctly is that the first message analyzed need to be
 * correct - to have rough idea of correctness we check if pulse-id is not 0 and the global time is within
 * a reasonable timerange around the current time.
 */
public class MainHeaderAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(MainHeaderAnalyzer.class);

    private MainHeader lastValid = null;
    private final String streamName;

    private boolean createHistograms = false;
    private boolean checkPulseIdTime = true;
    

    private long validTimeDelta = TimeUnit.MINUTES.toMillis(10);
    private long validPulseIdDelta = TimeUnit.DAYS.toSeconds(1) *100;

    private AnalyzerReport report;

    /**
     * Create validator
     * @param streamName Name of the stream - used for logging purposes only
     */
    public MainHeaderAnalyzer(String streamName) {
        this.streamName = streamName;
        this.report = new AnalyzerReport();
    }

    /**
     * Validate pulse-id and global-timestamp based on the state of previous messages
     *
     * @param header Message header to analyze
     * @return Returns true if header is valid based on the validators state (messages validated before)
     */
    public boolean analyze(MainHeader header) {

        final long currentTime = System.currentTimeMillis();

        final long headerTimestamp = header.getGlobalTimestamp().getAsMillis();
        final long headerPulseId = header.getPulseId();

        report.incrementNumberOfMessages();

        // Check for 0 pulse-id
        if (headerPulseId == 0) {
            report.incrementZeroPulseIds();
            logger.warn("stream: {} - pulse-id: {} at timestamp: {} - 0 pulse-id",
                    streamName,
                    headerPulseId,
                    header.getGlobalTimestamp());

            return false;
        }

        // Check if global timestamp send from the IOC largely differs from current time
        // Note: this check might lead to problems if the receiving nodes local time largely differs
        // from the actual time of the other systems
        // Only logging old messages, but accept them. refusing messages in the future.
        if (headerTimestamp < (currentTime - validTimeDelta) ) {
            //report.incrementGlobalTimestampOutOfValidTimeRange();
            logger.warn("stream: {} - pulse-id: {} at timestamp: {} - too old {} +/- {} ms",
                    streamName,
                    headerPulseId,
                    header.getGlobalTimestamp(),
                    currentTime,
                    validTimeDelta);

            //return false;
        } else if ( headerTimestamp > (currentTime + validTimeDelta)) {
            report.incrementGlobalTimestampOutOfValidTimeRange();
            logger.warn("stream: {} - pulse-id: {} at timestamp: {} - out of valid time range {} +/- {} ms",
                    streamName,
                    headerPulseId,
                    header.getGlobalTimestamp(),
                    currentTime,
                    validTimeDelta);

            return false;
        }      
        
        if (checkPulseIdTime){
            
            long timestampNanos =  header.getGlobalTimestamp().getAsLongArray()[1] % 1000000;
            if (!checkPulseId(headerPulseId,timestampNanos)){
                logger.warn("stream: {} - pulse-id: {} at timestamp: {} - pulse-id does not match timestamp nanos {}",
                    streamName,
                    headerPulseId,
                    header.getGlobalTimestamp(),
                    timestampNanos);
                return false;                
            }
            
            
            if ((headerPulseId - getSimulatedPulseId()) > validPulseIdDelta) {
                logger.warn("stream: {} - pulse-id: {} at timestamp: {} - out of valid pulse-id time range +{} ms",
                    streamName,
                    headerPulseId,
                    header.getGlobalTimestamp(),                    
                    validPulseIdDelta);
                return false;
            }        
        }

        // For the following checks a last valid header is necessary
        // Note: Assumption first message we get is correct - if this is not true all following checks might fail
        if(lastValid != null) {

            final long validPulseId = lastValid.getPulseId();
            final long validTimestamp = lastValid.getGlobalTimestamp().getAsMillis();

            // Check for duplicated pulse-id
            if (validPulseId == headerPulseId) {
                report.incrementDuplicatedPulseIds();
                logger.warn("stream: {} - pulse-id: {} at timestamp: {} - duplicate pulse-id {}",
                        streamName,
                        headerPulseId,
                        header.getGlobalTimestamp(),
                        validPulseId);

                return false;
            }


            // Check for equal or smaller pulse-id
            if (validPulseId > headerPulseId) {
                report.incrementPulseIdsBeforeLastValid();
                logger.warn("stream: {} - pulse-id: {} at timestamp: {} - pulse-id before last valid pulse-id {}",
                        streamName,
                        headerPulseId,
                        header.getGlobalTimestamp(),
                        validPulseId);

                return false;
            }

            // Check if timestamp is after last valid message
            // We ignore the nanoseconds part as it is invalid anyway i.e. it is used to hold parts of the pulse-id
            if (validTimestamp == headerTimestamp ) {
                report.incrementDuplicatedGlobalTimestamp();
                logger.warn("stream: {} - pulse-id: {} at timestamp: {} - duplicate global-timestamp {}",
                        streamName,
                        headerPulseId,
                        header.getGlobalTimestamp(),
                        lastValid.getGlobalTimestamp());

                return false;
            }

            if (validTimestamp > headerTimestamp ) {
                report.incrementGlobalTimestampBeforeLastValid();
                logger.warn("stream: {} - pulse-id: {} at timestamp: {} - global-timestamp before last valid timestamp {}",
                        streamName,
                        headerPulseId,
                        header.getGlobalTimestamp(),
                        lastValid.getGlobalTimestamp());

                return false;
            }

            if(createHistograms) {
                // Update pulse-id increment histogram
                report.updateHistogramPulseIdIncrements((int) (headerPulseId - validPulseId)); // cast to int needed
                // Update delay histogram
                report.updateHistogramDelays((int) (currentTime - headerTimestamp));
            }


            report.incrementNumberOfCorrectMessages();
        }

        lastValid = header;

        return true;
    }

    /**
     * Reset the validator state
     * @return true if a state was reset, false if no reset was needed
     */
    public boolean reset(){
        // Always reset the report
        report = new AnalyzerReport();

        if(lastValid == null){
            return false;
        }

        lastValid = null;
        return true;
    }

    public long getValidTimeDelta() {
        return validTimeDelta;
    }

    public void setValidTimeDelta(long validTimeDelta) {
        this.validTimeDelta = validTimeDelta;
    }
    

    public boolean isCreateHistograms() {
        return createHistograms;
    }

    public void setCreateHistograms(boolean createHistograms) {
        this.createHistograms = createHistograms;
    }
    
    public boolean isCheckPulseIdTime() {
        return checkPulseIdTime;
    }

    public void setCheckPulseIdTime(boolean checkPulseIdTime) {
        this.checkPulseIdTime = checkPulseIdTime;
    }
    
    public long getValidPulseIdDelta() {
        return validPulseIdDelta;
    }

    public void setValidPidDelta(long validPulseIdDelta) {
        this.validPulseIdDelta = validPulseIdDelta;
    }
    
    

    public AnalyzerReport getReport() {
        return report;
    }
    
    public static final LocalDateTime startPulseId = LocalDateTime.of(2017, 9, 4, 11, 11, 18);     
    
    public static long getSimulatedPulseId() {
        LocalDateTime now = LocalDateTime.now();
        long  millis = ChronoUnit.MILLIS.between(startPulseId, now); 
        long pid = millis/10;
        return pid;
    }
        
    public static boolean checkPulseId(long pulseId, long timestampNanos) {
        return ((pulseId % 1000000) == (timestampNanos % 1000000));
    }       
    
}