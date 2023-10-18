package ch.psi.bsread.analyzer;

import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Timestamp;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class MainHeaderAnalyzerTest {

    @Test
    public void analyze() {

        MainHeaderAnalyzer validator = new MainHeaderAnalyzer("tcp://teststream");
        validator.setCheckPulseIdTime(false);
        MainHeader header;

        // Check 0 pulse-id detection
        validator.reset();

        header = new MainHeader();
        header.setPulseId(0);
        header.setGlobalTimestamp(Timestamp.ofMillis(System.currentTimeMillis()));

        assertFalse(validator.analyze(header));
        assertFalse(validator.reset());  // validator must not have a state as failing the check must not modify the state of the validator


        // Check valid start with global-timestamp around current time
        validator.reset();

        header = new MainHeader();
        header.setPulseId(10);
        header.setGlobalTimestamp(Timestamp.ofMillis(System.currentTimeMillis()));

        assertTrue(validator.analyze(header));
        assertTrue(validator.reset());  // validator have a state

        validator.reset();

        header = new MainHeader();
        header.setPulseId(10);
        header.setGlobalTimestamp(Timestamp.ofMillis(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(8)));

        assertTrue(validator.analyze(header));
        assertTrue(validator.reset());  // validator have a state

        validator.reset();

        header = new MainHeader();
        header.setPulseId(10);
        header.setGlobalTimestamp(Timestamp.ofMillis(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(8)));

        assertTrue(validator.analyze(header));
        assertTrue(validator.reset());  // validator have a state


        // Check invalid start with global-timestamp / invalid global-timestamp range
        validator.reset();

        //header = new MainHeader();
        //header.setPulseId(10);
        //header.setGlobalTimestamp(Timestamp.ofMillis(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(12)));

        //assertFalse(validator.analyze(header));
        //assertEquals(1, validator.getReport().getGlobalTimestampOutOfValidTimeRange());
        //assertFalse(validator.reset());  // validator must not have a state as failing the check must not modify the state of the validator

        //validator.reset();

        header = new MainHeader();
        header.setPulseId(10);
        header.setGlobalTimestamp(Timestamp.ofMillis(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(12)));

        assertFalse(validator.analyze(header));
        assertEquals(1, validator.getReport().getGlobalTimestampOutOfValidTimeRange());
        assertFalse(validator.reset());  // validator must not have a state as failing the check must not modify the state of the validator


        // Check invalid message due to global-timestamp being before last valid
        validator.reset();

        long timestamp = System.currentTimeMillis();
        header = new MainHeader();
        header.setPulseId(2);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp));
        assertTrue(validator.analyze(header));
        header = new MainHeader();
        header.setPulseId(3);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp - 1)); // one millisecond before
        assertFalse(validator.analyze(header));
        assertEquals(1, validator.getReport().getGlobalTimestampBeforeLastValid());

        // Check for correct increase
        validator.reset();

        timestamp = System.currentTimeMillis();
        header = new MainHeader();
        header.setPulseId(1);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp));
        assertTrue(validator.analyze(header));
        header = new MainHeader();
        header.setPulseId(2);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp+1)); // one millisecond after
        assertTrue(validator.analyze(header));

        // Check invalid due to same or previous pulse-id in header
        validator.reset();

        timestamp = System.currentTimeMillis();
        header = new MainHeader();
        header.setPulseId(1);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp));
        assertTrue(validator.analyze(header));

        header = new MainHeader();
        header.setPulseId(2);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp+1));
        assertTrue(validator.analyze(header));

        header = new MainHeader();
        header.setPulseId(1);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp+3));
        assertFalse(validator.analyze(header));
        assertEquals(1, validator.getReport().getPulseIdsBeforeLastValid());

        header = new MainHeader();
        header.setPulseId(2);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp+4));
        assertFalse(validator.analyze(header)); // last valid pulse-id was already 2
        assertEquals(1, validator.getReport().getDuplicatedPulseIds());

        //continue normally again
        header = new MainHeader();
        header.setPulseId(3);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp+5));
        assertTrue(validator.analyze(header));

        assertTrue(validator.reset());
    }
}