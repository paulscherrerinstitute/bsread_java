package ch.psi.bsread.validation;

import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Timestamp;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class MainHeaderValidatorTest {

    @Test
    public void validate() {

        MainHeaderValidator validator = new MainHeaderValidator("tcp://teststream");
        MainHeader header;

        // Check 0 pulse-id detection
        validator.reset();

        header = new MainHeader();
        header.setPulseId(0);
        header.setGlobalTimestamp(Timestamp.ofMillis(System.currentTimeMillis()));

        assertFalse(validator.validate(header));
        assertFalse(validator.reset());  // validator must not have a state as failing the check must not modify the state of the validator


        // Check valid start with global-timestamp around current time
        validator.reset();

        header = new MainHeader();
        header.setPulseId(10);
        header.setGlobalTimestamp(Timestamp.ofMillis(System.currentTimeMillis()));

        assertTrue(validator.validate(header));
        assertTrue(validator.reset());  // validator have a state

        validator.reset();

        header = new MainHeader();
        header.setPulseId(10);
        header.setGlobalTimestamp(Timestamp.ofMillis(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(8)));

        assertTrue(validator.validate(header));
        assertTrue(validator.reset());  // validator have a state

        validator.reset();

        header = new MainHeader();
        header.setPulseId(10);
        header.setGlobalTimestamp(Timestamp.ofMillis(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(8)));

        assertTrue(validator.validate(header));
        assertTrue(validator.reset());  // validator have a state


        // Check invalid start with global-timestamp / invalid global-timestamp range
        validator.reset();

        header = new MainHeader();
        header.setPulseId(10);
        header.setGlobalTimestamp(Timestamp.ofMillis(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(12)));

        assertFalse(validator.validate(header));
        assertFalse(validator.reset());  // validator must not have a state as failing the check must not modify the state of the validator

        validator.reset();

        header = new MainHeader();
        header.setPulseId(10);
        header.setGlobalTimestamp(Timestamp.ofMillis(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(12)));

        assertFalse(validator.validate(header));
        assertFalse(validator.reset());  // validator must not have a state as failing the check must not modify the state of the validator


        // Check invalid message due to global-timestamp being before last valid
        validator.reset();

        long timestamp = System.currentTimeMillis();
        header = new MainHeader();
        header.setPulseId(2);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp));
        assertTrue(validator.validate(header));
        header = new MainHeader();
        header.setPulseId(3);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp - 1)); // one millisecond before
        assertFalse(validator.validate(header));

        // Check for correct increase
        validator.reset();

        timestamp = System.currentTimeMillis();
        header = new MainHeader();
        header.setPulseId(1);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp));
        assertTrue(validator.validate(header));
        header = new MainHeader();
        header.setPulseId(2);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp+1)); // one millisecond after
        assertTrue(validator.validate(header));

        // Check invalid due to same or previous pulse-id in header
        validator.reset();

        timestamp = System.currentTimeMillis();
        header = new MainHeader();
        header.setPulseId(1);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp));
        assertTrue(validator.validate(header));

        header = new MainHeader();
        header.setPulseId(2);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp+1));
        assertTrue(validator.validate(header));

        header = new MainHeader();
        header.setPulseId(1);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp+3));
        assertFalse(validator.validate(header));

        header = new MainHeader();
        header.setPulseId(2);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp+4));
        assertFalse(validator.validate(header)); // last valid pulse-id was already 2

        //continue normally again
        header = new MainHeader();
        header.setPulseId(3);
        header.setGlobalTimestamp(Timestamp.ofMillis(timestamp+5));
        assertTrue(validator.validate(header));

        assertTrue(validator.reset());
    }
}