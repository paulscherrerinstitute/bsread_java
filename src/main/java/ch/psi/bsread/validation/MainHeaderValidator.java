package ch.psi.bsread.validation;

import ch.psi.bsread.message.MainHeader;
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
public class MainHeaderValidator {

    private static final Logger logger = LoggerFactory.getLogger(MainHeaderValidator.class);

    private MainHeader lastValid = null;
    private final String streamName;

    private long validTimeDelta = TimeUnit.SECONDS.toMillis(10);

    /**
     * Create validator
     * @param streamName Name of the stream - used for logging purposes only
     */
    public MainHeaderValidator(String streamName) {
        this.streamName = streamName;
    }

    /**
     * Validate pulse-id and global-timestamp based on the state of previous messages
     *
     * @param header Message header to analyze
     * @return Returns true if header is valid based on the validators state (messages validated before)
     */
    public boolean validate(MainHeader header) {
        return validateReason(header) > 0 ? false : true;
    }

    /**
     * Validate pulse-id and global-timestamp. If header is not valid returns a reason for not
     * @param header    Header to be analyzed
     * @return  Integer indicating the reason why the header is not valid. Returns
     *          0 - valid header
     *          1 - 0 pulse-id
     *          2 - global-time of message out of valid time range
     *          3 - pulse-id before last valid pulse-id
     *          4 - global-time before last valid global-time
     */
    public int validateReason(MainHeader header){

        final long currentTime = System.currentTimeMillis();

        final long headerTimestamp = header.getGlobalTimestamp().getAsMillis();
        final long headerPulseId = header.getPulseId();


        // Check for 0 pulse-id
        if (headerPulseId == 0) {
            logger.warn("stream: {} - pulse-id: {} at timestamp: {} - 0 pulse-id",
                    streamName,
                    headerPulseId,
                    header.getGlobalTimestamp());

            return 1;
        }

        // Check if global timestamp send from the IOC largely differs from current time
        // Note: this check might lead to problems if the receiving nodes local time largely differs
        // from the actual time of the other systems
        if (headerTimestamp < (currentTime - validTimeDelta) || headerTimestamp > (currentTime + validTimeDelta)) {
            logger.warn("stream: {} - pulse-id: {} at timestamp: {} - out of valid time range {} +/- {} ms",
                    streamName,
                    headerPulseId,
                    header.getGlobalTimestamp(),
                    currentTime,
                    validTimeDelta);

            return 2;
        }

        // For the following checks a last valid header is necessary
        // Note: Assumption first message we get is correct - if this is not true all following checks might fail
        if(lastValid != null) {

            final long validPulseId = lastValid.getPulseId();
            final long validTimestamp = lastValid.getGlobalTimestamp().getAsMillis();

            // Check for equal or smaller pulse-id
            if (validPulseId >= headerPulseId) {

                logger.warn("stream: {} - pulse-id: {} at timestamp: {} - pulse-id before or equal last valid pulse-id {}",
                        streamName,
                        headerPulseId,
                        header.getGlobalTimestamp(),
                        validPulseId);

                return 3;
            }

            // Check if timestamp is after last valid message
            // We ignore the nanoseconds part as it is invalid anyway i.e. it is used to hold parts of the pulse-id
            if (validTimestamp >= headerTimestamp ) {

                logger.warn("stream: {} - pulse-id: {} at timestamp: {} - global-timestamp before or equal last valid timestamp {}",
                        streamName,
                        headerPulseId,
                        header.getGlobalTimestamp(),
                        lastValid.getGlobalTimestamp());

                return 4;
            }
        }

        lastValid = header;

        return 0;
    }

    /**
     * Reset the validator state
     * @return true if a state was reset, false if no reset was needed
     */
    public boolean reset(){
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
}
