package ch.psi.bsread.sync;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import ch.psi.bsread.configuration.Channel;
import ch.psi.bsread.message.Timestamp;

public class MessageSynchronizerTest {

	/**
	 * Test default setup for MessageBuilder with 100Hz
	 */
	@Test
	public void testMessageSynchronizer_100Hz() {
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(5);
		MessageSynchronizer<TestEvent> mBuffer = new MessageSynchronizer<>(completeQueue, 3, false, false, Arrays.asList(new Channel("A", 1), new Channel(
				"B", 1)),
				(event) -> event.getChannel(), (event) -> event.getPulseId());

		// Test pattern
		// A(1) A(2) B(2) B(1)
		Timestamp globalTime0 = new Timestamp();
		Timestamp globalTime1 = new Timestamp();
		Timestamp globalTime2 = new Timestamp();
		mBuffer.addMessage(newMessage(1, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(2, globalTime2, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(2, globalTime2, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(1, globalTime1, "B"));

		// Expecting 2 messages in correct order
		assertEquals(2, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());
		AssembledMessage message = new AssembledMessage(completeQueue.poll());
		assertEquals(1, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(2, message.getPulseId());
		assertEquals(globalTime2, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test pattern
		// A(0) B(0)
		mBuffer.addMessage(newMessage(0, globalTime0, "A"));

		// Expecting that there will be no messages as the pulse-id 0 is
		// below the pulse-id already delivered
		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		mBuffer.addMessage(newMessage(0, globalTime0, "B"));

		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		Timestamp globalTime3 = new Timestamp();
		Timestamp globalTime4 = new Timestamp();
		Timestamp globalTime5 = new Timestamp();
		Timestamp globalTime6 = new Timestamp();
		// Test Pattern
		// A(3), A(4), A(5), A(6), B(4), B(3)
		// 3 should be dropped because exceeding buffer size
		// 4 should be delivered
		mBuffer.addMessage(newMessage(3, globalTime3, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(4, globalTime4, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(5, globalTime5, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(6, globalTime6, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(4, globalTime4, "B"));
		assertEquals(1, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(4, message.getPulseId());
		assertEquals(globalTime4, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(3, globalTime3, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		assertEquals(0, completeQueue.size());

		// State: A(5), A(6) still in buffer
		Timestamp globalTime7 = new Timestamp();
		Timestamp globalTime8 = new Timestamp();
		Timestamp globalTime9 = new Timestamp();
		mBuffer.addMessage(newMessage(7, globalTime7, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(8, globalTime8, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(9, globalTime9, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());

		mBuffer.addMessage(newMessage(7, globalTime7, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(7, message.getPulseId());
		assertEquals(globalTime7, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(8, globalTime8, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(8, message.getPulseId());
		assertEquals(globalTime8, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(9, globalTime9, "B"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(9, message.getPulseId());
		assertEquals(globalTime9, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test Pattern
		// A(11), B(11), B(10), A(10)
		Timestamp globalTime10 = new Timestamp();
		Timestamp globalTime11 = new Timestamp();
		mBuffer.addMessage(newMessage(11, globalTime11, "A"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(11, globalTime11, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(10, globalTime10, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(10, globalTime10, "A"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(2, completeQueue.size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(10, message.getPulseId());
		assertEquals(globalTime10, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(11, message.getPulseId());
		assertEquals(globalTime11, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
	}

	/**
	 * Test default setup for MessageBuilder with 100Hz
	 */
	@Test
	public void testMessageSynchronizer_100Hz_Time() {
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(5);
		MessageSynchronizer<TestEvent> mBuffer = new MessageSynchronizer<>(completeQueue, 3, false, false, Arrays.asList(new Channel("A", 1), new Channel(
				"B", 1)),
				(event) -> event.getChannel(), (event) -> event.getPulseId(), (pulseId) -> pulseId);

		// Test pattern
		// A(1) A(2) B(2) B(1)
		Timestamp globalTime0 = new Timestamp();
		Timestamp globalTime1 = new Timestamp();
		Timestamp globalTime2 = new Timestamp();
		mBuffer.addMessage(newMessage(1, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(2, globalTime2, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(2, globalTime2, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(1, globalTime1, "B"));

		// Expecting 2 messages in correct order
		assertEquals(2, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());
		AssembledMessage message = new AssembledMessage(completeQueue.poll());
		assertEquals(1, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(2, message.getPulseId());
		assertEquals(globalTime2, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test pattern
		// A(0) B(0)
		mBuffer.addMessage(newMessage(0, globalTime0, "A"));

		// Expecting that there will be no messages as the pulse-id 0 is
		// below the pulse-id already delivered
		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		mBuffer.addMessage(newMessage(0, globalTime0, "B"));

		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		Timestamp globalTime3 = new Timestamp();
		Timestamp globalTime4 = new Timestamp();
		Timestamp globalTime5 = new Timestamp();
		Timestamp globalTime6 = new Timestamp();
		// Test Pattern
		// A(3), A(4), A(5), A(6), B(4), B(3)
		// 3 should be dropped because exceeding buffer size
		// 4 should be delivered
		mBuffer.addMessage(newMessage(3, globalTime3, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(4, globalTime4, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(5, globalTime5, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(6, globalTime6, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(4, globalTime4, "B"));
		assertEquals(1, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(4, message.getPulseId());
		assertEquals(globalTime4, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(3, globalTime3, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		assertEquals(0, completeQueue.size());

		// State: A(5), A(6) still in buffer
		Timestamp globalTime7 = new Timestamp();
		Timestamp globalTime8 = new Timestamp();
		Timestamp globalTime9 = new Timestamp();
		mBuffer.addMessage(newMessage(7, globalTime7, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(8, globalTime8, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(9, globalTime9, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());

		mBuffer.addMessage(newMessage(7, globalTime7, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(7, message.getPulseId());
		assertEquals(globalTime7, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(8, globalTime8, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(8, message.getPulseId());
		assertEquals(globalTime8, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(9, globalTime9, "B"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(9, message.getPulseId());
		assertEquals(globalTime9, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test Pattern
		// A(11), B(11), B(10), A(10)
		Timestamp globalTime10 = new Timestamp();
		Timestamp globalTime11 = new Timestamp();
		mBuffer.addMessage(newMessage(11, globalTime11, "A"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(11, globalTime11, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(10, globalTime10, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(10, globalTime10, "A"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(2, completeQueue.size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(10, message.getPulseId());
		assertEquals(globalTime10, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(11, message.getPulseId());
		assertEquals(globalTime11, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
	}

	/**
	 * Test default setup for MessageBuilder with 100Hz
	 */
	@Test
	public void testMessageSynchronizer_100Hz_SendFirstComplete() {
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(5);
		MessageSynchronizer<TestEvent> mBuffer = new MessageSynchronizer<>(completeQueue, 3, false, true, Arrays.asList(new Channel("A", 1), new Channel(
				"B", 1)),
				(event) -> event.getChannel(), (event) -> event.getPulseId());

		// Test pattern
		// A(1) A(2) B(2) B(1)
		Timestamp globalTime0 = new Timestamp();
		Timestamp globalTime1 = new Timestamp();
		Timestamp globalTime2 = new Timestamp();
		mBuffer.addMessage(newMessage(1, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(2, globalTime2, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(2, globalTime2, "B"));

		// Expecting 1 message
		assertEquals(1, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());
		AssembledMessage message = new AssembledMessage(completeQueue.poll());
		assertEquals(2, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(1, globalTime1, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(0, mBuffer.getBufferSize());

		// Test pattern
		// A(0) B(0)
		mBuffer.addMessage(newMessage(0, globalTime0, "A"));

		// Expecting that there will be no messages as the pulse-id 0 is
		// below the pulse-id already delivered
		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		mBuffer.addMessage(newMessage(0, globalTime0, "B"));

		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		Timestamp globalTime3 = new Timestamp();
		Timestamp globalTime4 = new Timestamp();
		Timestamp globalTime5 = new Timestamp();
		Timestamp globalTime6 = new Timestamp();
		// Test Pattern
		// A(3), A(4), A(5), A(6), B(4), B(3)
		// 3 should be dropped because exceeding buffer size
		// 4 should be delivered
		mBuffer.addMessage(newMessage(3, globalTime3, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(4, globalTime4, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(5, globalTime5, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(6, globalTime6, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(4, globalTime4, "B"));
		assertEquals(1, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(4, message.getPulseId());
		assertEquals(globalTime4, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(3, globalTime3, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		assertEquals(0, completeQueue.size());

		// State: A(5), A(6) still in buffer
		Timestamp globalTime7 = new Timestamp();
		Timestamp globalTime8 = new Timestamp();
		Timestamp globalTime9 = new Timestamp();
		mBuffer.addMessage(newMessage(7, globalTime7, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(8, globalTime8, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(9, globalTime9, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());

		mBuffer.addMessage(newMessage(7, globalTime7, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(7, message.getPulseId());
		assertEquals(globalTime7, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(8, globalTime8, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(8, message.getPulseId());
		assertEquals(globalTime8, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(9, globalTime9, "B"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(9, message.getPulseId());
		assertEquals(globalTime9, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test Pattern
		// A(11), B(11), B(10), A(10)
		Timestamp globalTime10 = new Timestamp();
		Timestamp globalTime11 = new Timestamp();
		mBuffer.addMessage(newMessage(11, globalTime11, "A"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(11, globalTime11, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(10, globalTime10, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(10, globalTime10, "A"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(2, completeQueue.size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(10, message.getPulseId());
		assertEquals(globalTime10, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(11, message.getPulseId());
		assertEquals(globalTime11, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
	}

	/**
	 * Test default setup for MessageBuilder with 10Hz
	 */
	@Test
	public void testMessageSynchronizer_10Hz() {
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(5);
		MessageSynchronizer<TestEvent> mBuffer = new MessageSynchronizer<>(completeQueue, 3, false, false, Arrays.asList(new Channel("A", 10), new Channel(
				"B", 10)),
				(event) -> event.getChannel(), (event) -> event.getPulseId());

		// Test pattern
		// A(1) A(2) B(2) B(1)
		Timestamp globalTime0 = new Timestamp();
		Timestamp globalTime1 = new Timestamp();
		Timestamp globalTime2 = new Timestamp();
		mBuffer.addMessage(newMessage(10, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(20, globalTime2, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(20, globalTime2, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(10, globalTime1, "B"));

		// Expecting 2 messages in correct order
		assertEquals(2, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());
		AssembledMessage message = new AssembledMessage(completeQueue.poll());
		assertEquals(10, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(20, message.getPulseId());
		assertEquals(globalTime2, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test pattern
		// A(0) B(0)
		mBuffer.addMessage(newMessage(0, globalTime0, "A"));

		// Expecting that there will be no messages as the pulse-id 0 is
		// below the pulse-id already delivered
		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		mBuffer.addMessage(newMessage(0, globalTime0, "B"));

		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		Timestamp globalTime3 = new Timestamp();
		Timestamp globalTime4 = new Timestamp();
		Timestamp globalTime5 = new Timestamp();
		Timestamp globalTime6 = new Timestamp();
		// Test Pattern
		// A(3), A(4), A(5), A(6), B(4), B(3)
		// 3 should be dropped because exceeding buffer size
		// 4 should be delivered
		mBuffer.addMessage(newMessage(30, globalTime3, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(40, globalTime4, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(50, globalTime5, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(60, globalTime6, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(40, globalTime4, "B"));
		assertEquals(1, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(40, message.getPulseId());
		assertEquals(globalTime4, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(30, globalTime3, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		assertEquals(0, completeQueue.size());

		// State: A(5), A(6) still in buffer
		Timestamp globalTime7 = new Timestamp();
		Timestamp globalTime8 = new Timestamp();
		Timestamp globalTime9 = new Timestamp();
		mBuffer.addMessage(newMessage(70, globalTime7, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(80, globalTime8, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(90, globalTime9, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());

		mBuffer.addMessage(newMessage(70, globalTime7, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(70, message.getPulseId());
		assertEquals(globalTime7, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(80, globalTime8, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(80, message.getPulseId());
		assertEquals(globalTime8, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(90, globalTime9, "B"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(90, message.getPulseId());
		assertEquals(globalTime9, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test Pattern
		// A(11), B(11), B(10), A(10)
		Timestamp globalTime10 = new Timestamp();
		Timestamp globalTime11 = new Timestamp();
		mBuffer.addMessage(newMessage(110, globalTime11, "A"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(110, globalTime11, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(100, globalTime10, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(100, globalTime10, "A"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(2, completeQueue.size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(100, message.getPulseId());
		assertEquals(globalTime10, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(110, message.getPulseId());
		assertEquals(globalTime11, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test wrong frequency
		Timestamp globalTime12 = new Timestamp();
		mBuffer.addMessage(newMessage(115, globalTime12, "A"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
	}

	/**
	 * Test default setup for MessageBuilder with 10Hz
	 */
	@Test
	public void testMessageSynchronizer_10Hz_SendFirstComplete() {
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(5);
		MessageSynchronizer<TestEvent> mBuffer = new MessageSynchronizer<>(completeQueue, 3, false, true, Arrays.asList(new Channel("A", 10), new Channel(
				"B", 10)),
				(event) -> event.getChannel(), (event) -> event.getPulseId());

		// Test pattern
		// A(1) A(2) B(2) B(1)
		Timestamp globalTime0 = new Timestamp();
		Timestamp globalTime1 = new Timestamp();
		Timestamp globalTime2 = new Timestamp();
		mBuffer.addMessage(newMessage(10, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(20, globalTime2, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(20, globalTime2, "B"));

		assertEquals(1, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());
		AssembledMessage message = new AssembledMessage(completeQueue.poll());
		assertEquals(20, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(10, globalTime1, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(0, mBuffer.getBufferSize());

		// Test pattern
		// A(0) B(0)
		mBuffer.addMessage(newMessage(0, globalTime0, "A"));

		// Expecting that there will be no messages as the pulse-id 0 is
		// below the pulse-id already delivered
		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		mBuffer.addMessage(newMessage(0, globalTime0, "B"));

		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		Timestamp globalTime3 = new Timestamp();
		Timestamp globalTime4 = new Timestamp();
		Timestamp globalTime5 = new Timestamp();
		Timestamp globalTime6 = new Timestamp();
		// Test Pattern
		// A(3), A(4), A(5), A(6), B(4), B(3)
		// 3 should be dropped because exceeding buffer size
		// 4 should be delivered
		mBuffer.addMessage(newMessage(30, globalTime3, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(40, globalTime4, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(50, globalTime5, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(60, globalTime6, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(40, globalTime4, "B"));
		assertEquals(1, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(40, message.getPulseId());
		assertEquals(globalTime4, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(30, globalTime3, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		assertEquals(0, completeQueue.size());

		// State: A(5), A(6) still in buffer
		Timestamp globalTime7 = new Timestamp();
		Timestamp globalTime8 = new Timestamp();
		Timestamp globalTime9 = new Timestamp();
		mBuffer.addMessage(newMessage(70, globalTime7, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(80, globalTime8, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(90, globalTime9, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());

		mBuffer.addMessage(newMessage(70, globalTime7, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(70, message.getPulseId());
		assertEquals(globalTime7, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(80, globalTime8, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(80, message.getPulseId());
		assertEquals(globalTime8, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(90, globalTime9, "B"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(90, message.getPulseId());
		assertEquals(globalTime9, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test Pattern
		// A(11), B(11), B(10), A(10)
		Timestamp globalTime10 = new Timestamp();
		Timestamp globalTime11 = new Timestamp();
		mBuffer.addMessage(newMessage(110, globalTime11, "A"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(110, globalTime11, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(100, globalTime10, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(100, globalTime10, "A"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(2, completeQueue.size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(100, message.getPulseId());
		assertEquals(globalTime10, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(110, message.getPulseId());
		assertEquals(globalTime11, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test wrong frequency
		Timestamp globalTime12 = new Timestamp();
		mBuffer.addMessage(newMessage(115, globalTime12, "A"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
	}

	/**
	 * Test default setup for MessageBuilder with 10 and 100Hz
	 */
	@Test
	public void testMessageSynchronizer_100_10Hz() {
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(5);
		MessageSynchronizer<TestEvent> mBuffer = new MessageSynchronizer<>(completeQueue, 4, false, false, Arrays.asList(new Channel("A", 1), new Channel(
				"B", 10)),
				(event) -> event.getChannel(), (event) -> event.getPulseId());

		// Test pattern
		// A(1) A(2) B(2) B(1)
		Timestamp globalTime1 = new Timestamp();
		mBuffer.addMessage(newMessage(0, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(1, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(10, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(20, globalTime1, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(5, globalTime1, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(0, globalTime1, "B"));

		// Expecting 2 messages in correct order
		assertEquals(2, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		AssembledMessage message = new AssembledMessage(completeQueue.poll());
		assertEquals(0, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(1, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		// Test pattern
		// A(0) B(0)
		mBuffer.addMessage(newMessage(-1, globalTime1, "A"));

		// Expecting that there will be no messages as the pulse-id 0 is
		// below the pulse-id already delivered
		assertEquals(2, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		mBuffer.addMessage(newMessage(2, globalTime1, "C"));

		// Expecting that there will be no messages since the channel is not
		// part of the mBuffer
		assertEquals(2, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		for (int i = 2; i <= 9; ++i) {
			mBuffer.addMessage(newMessage(i, globalTime1, "A"));

			assertEquals(1, completeQueue.size());
			assertEquals(2, mBuffer.getBufferSize());

			message = new AssembledMessage(completeQueue.poll());
			assertEquals(i, message.getPulseId());
			assertEquals(globalTime1, message.getGlobalTimestamp());
			assertEquals(1, message.getValues().size());
		}

		mBuffer.addMessage(newMessage(11, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(10, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(10, globalTime1, "B"));
		assertEquals(2, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(10, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(11, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		for (int i = 12; i <= 19; ++i) {
			mBuffer.addMessage(newMessage(i, globalTime1, "A"));

			assertEquals(1, completeQueue.size());
			assertEquals(1, mBuffer.getBufferSize());

			message = new AssembledMessage(completeQueue.poll());
			assertEquals(i, message.getPulseId());
			assertEquals(globalTime1, message.getGlobalTimestamp());
			assertEquals(1, message.getValues().size());
		}

		mBuffer.addMessage(newMessage(20, globalTime1, "A"));
		assertEquals(1, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(20, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(30, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(40, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(22, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(23, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(24, globalTime1, "A"));
		assertEquals(3, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(22, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(23, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(24, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		for (int i = 25; i <= 29; ++i) {
			mBuffer.addMessage(newMessage(i, globalTime1, "A"));

			assertEquals(1, completeQueue.size());
			assertEquals(2, mBuffer.getBufferSize());

			message = new AssembledMessage(completeQueue.poll());
			assertEquals(i, message.getPulseId());
			assertEquals(globalTime1, message.getGlobalTimestamp());
			assertEquals(1, message.getValues().size());
		}

		mBuffer.addMessage(newMessage(31, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(32, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(33, globalTime1, "A"));
		assertEquals(3, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(31, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(32, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(33, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		mBuffer.addMessage(newMessage(30, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(35, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(50, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(60, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(70, globalTime1, "B"));
		assertEquals(1, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(35, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		mBuffer.addMessage(newMessage(80, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(34, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(37, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(36, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(40, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(41, globalTime1, "A"));
		assertEquals(1, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(41, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
	}

	/**
	 * Test default setup for MessageBuilder with 10 and 100Hz
	 */
	@Test
	public void testMessageSynchronizer_100_10Hz_SendFirstComplete() {
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(5);
		MessageSynchronizer<TestEvent> mBuffer = new MessageSynchronizer<>(completeQueue, 4, false, true, Arrays.asList(new Channel("A", 1), new Channel(
				"B", 10)),
				(event) -> event.getChannel(), (event) -> event.getPulseId());

		// Test pattern
		// A(1) A(2) B(2) B(1)
		Timestamp globalTime1 = new Timestamp();
		mBuffer.addMessage(newMessage(0, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(1, globalTime1, "A"));

		assertFalse(completeQueue.isEmpty());
		assertEquals(0, mBuffer.getBufferSize());

		AssembledMessage message = new AssembledMessage(completeQueue.poll());
		assertEquals(1, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		mBuffer.addMessage(newMessage(10, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(20, globalTime1, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(5, globalTime1, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(0, globalTime1, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		// Test pattern
		// A(0) B(0)
		mBuffer.addMessage(newMessage(-1, globalTime1, "A"));

		// Expecting that there will be no messages as the pulse-id 0 is
		// below the pulse-id already delivered
		assertEquals(2, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		mBuffer.addMessage(newMessage(2, globalTime1, "C"));

		// Expecting that there will be no messages since the channel is not
		// part of the mBuffer
		assertEquals(2, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		for (int i = 2; i <= 9; ++i) {
			mBuffer.addMessage(newMessage(i, globalTime1, "A"));

			assertEquals(1, completeQueue.size());
			assertEquals(2, mBuffer.getBufferSize());

			message = new AssembledMessage(completeQueue.poll());
			assertEquals(i, message.getPulseId());
			assertEquals(globalTime1, message.getGlobalTimestamp());
			assertEquals(1, message.getValues().size());
		}

		mBuffer.addMessage(newMessage(11, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(10, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(10, globalTime1, "B"));
		assertEquals(2, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(10, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(11, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		for (int i = 12; i <= 19; ++i) {
			mBuffer.addMessage(newMessage(i, globalTime1, "A"));

			assertEquals(1, completeQueue.size());
			assertEquals(1, mBuffer.getBufferSize());

			message = new AssembledMessage(completeQueue.poll());
			assertEquals(i, message.getPulseId());
			assertEquals(globalTime1, message.getGlobalTimestamp());
			assertEquals(1, message.getValues().size());
		}

		mBuffer.addMessage(newMessage(20, globalTime1, "A"));
		assertEquals(1, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(20, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(30, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(40, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(22, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(23, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(24, globalTime1, "A"));
		assertEquals(3, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(22, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(23, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(24, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		for (int i = 25; i <= 29; ++i) {
			mBuffer.addMessage(newMessage(i, globalTime1, "A"));

			assertEquals(1, completeQueue.size());
			assertEquals(2, mBuffer.getBufferSize());

			message = new AssembledMessage(completeQueue.poll());
			assertEquals(i, message.getPulseId());
			assertEquals(globalTime1, message.getGlobalTimestamp());
			assertEquals(1, message.getValues().size());
		}

		mBuffer.addMessage(newMessage(31, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(32, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(33, globalTime1, "A"));
		assertEquals(3, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(31, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(32, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(33, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		mBuffer.addMessage(newMessage(30, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(35, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(50, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(60, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(70, globalTime1, "B"));
		assertEquals(1, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(35, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		mBuffer.addMessage(newMessage(80, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(34, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(37, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(36, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(40, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(41, globalTime1, "A"));
		assertEquals(1, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(41, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
	}

	/**
	 * Test default setup for MessageBuilder
	 */
	@Test
	public void testMessageSynchronizer_PulseIdStart() {
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(5);
		MessageSynchronizer<TestEvent> mBuffer = new MessageSynchronizer<>(completeQueue, 3, false, false, Arrays.asList(new Channel("A", 1), new Channel(
				"B", 1)),
				(event) -> event.getChannel(), (event) -> event.getPulseId());

		// Test pattern
		// A(2) A(1) B(2) B(1)
		Timestamp globalTime1 = new Timestamp();
		Timestamp globalTime2 = new Timestamp();
		mBuffer.addMessage(newMessage(2, globalTime2, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(1, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(2, globalTime2, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(1, globalTime1, "B"));

		// Expecting 2 messages in correct order
		assertEquals(2, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());
		AssembledMessage message = new AssembledMessage(completeQueue.poll());
		assertEquals(1, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(2, message.getPulseId());
		assertEquals(globalTime2, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
	}

	/**
	 * Test default setup for MessageBuilder
	 */
	@Test
	public void testMessageSynchronizer_PulseIdStart_SendFirstComplete() {
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(5);
		MessageSynchronizer<TestEvent> mBuffer = new MessageSynchronizer<>(completeQueue, 3, false, true, Arrays.asList(new Channel("A", 1), new Channel(
				"B", 1)),
				(event) -> event.getChannel(), (event) -> event.getPulseId());

		// Test pattern
		// A(2) A(1) B(2) B(1)
		Timestamp globalTime1 = new Timestamp();
		Timestamp globalTime2 = new Timestamp();
		mBuffer.addMessage(newMessage(2, globalTime2, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(1, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(2, globalTime2, "B"));

		// Expecting 1 message
		assertEquals(1, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());
		AssembledMessage message = new AssembledMessage(completeQueue.poll());
		assertEquals(2, message.getPulseId());
		assertEquals(globalTime2, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(1, globalTime1, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(0, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(3, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(3, globalTime1, "B"));

		// Expecting 1 message
		assertEquals(1, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(3, message.getPulseId());
		assertEquals(globalTime2, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
	}

	/**
	 * Testing MessageSynchronizer if sendIncompleteMessage flag is set to true
	 */
	@Test
	public void testMessageSynchronizer_100Hz_Incomplete() {
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(5);
		MessageSynchronizer<TestEvent> mBuffer = new MessageSynchronizer<>(completeQueue, 3, true, false, Arrays.asList(new Channel("A", 1), new Channel(
				"B", 1)),
				(event) -> event.getChannel(), (event) -> event.getPulseId());

		// Test pattern
		// A(1) A(2) B(2) B(1)
		Timestamp globalTime0 = new Timestamp();
		Timestamp globalTime1 = new Timestamp();
		Timestamp globalTime2 = new Timestamp();
		mBuffer.addMessage(newMessage(1, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(2, globalTime2, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(2, globalTime2, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(1, globalTime1, "B"));

		// Expecting 2 messages in correct order
		assertEquals(2, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());
		AssembledMessage message = new AssembledMessage(completeQueue.poll());
		assertEquals(1, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(2, message.getPulseId());
		assertEquals(globalTime2, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test pattern
		// A(0) B(0)
		mBuffer.addMessage(newMessage(0, globalTime0, "A"));

		// Expecting that there will be no messages as the pulse-id 0 is
		// below the pulse-id already delivered
		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		mBuffer.addMessage(newMessage(0, globalTime0, "B"));

		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		Timestamp globalTime3 = new Timestamp();
		Timestamp globalTime4 = new Timestamp();
		Timestamp globalTime5 = new Timestamp();
		Timestamp globalTime6 = new Timestamp();
		// Test Pattern
		// A(3), A(4), A(5), A(6), B(4), B(3)
		// 3 should be send incomplete because exceeding buffer size
		// 4 should be delivered
		mBuffer.addMessage(newMessage(3, globalTime3, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(4, globalTime4, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(5, globalTime5, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(6, globalTime6, "A"));
		assertEquals(1, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(3, message.getPulseId());
		assertEquals(globalTime3, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		assertTrue(message.getValues().containsKey("A"));

		mBuffer.addMessage(newMessage(4, globalTime4, "B"));
		assertEquals(1, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(4, message.getPulseId());
		assertEquals(globalTime4, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(3, globalTime3, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		assertEquals(0, completeQueue.size());

		// State: A(5), A(6) still in buffer
		Timestamp globalTime7 = new Timestamp();
		Timestamp globalTime8 = new Timestamp();
		Timestamp globalTime9 = new Timestamp();
		mBuffer.addMessage(newMessage(7, globalTime7, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(8, globalTime8, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		assertTrue(message.getValues().containsKey("A"));
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(5, message.getPulseId());
		assertEquals(globalTime5, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		assertTrue(message.getValues().containsKey("A"));
		mBuffer.addMessage(newMessage(9, globalTime9, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(6, message.getPulseId());
		assertEquals(globalTime6, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		assertTrue(message.getValues().containsKey("A"));

		mBuffer.addMessage(newMessage(7, globalTime7, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(7, message.getPulseId());
		assertEquals(globalTime7, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(8, globalTime8, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(8, message.getPulseId());
		assertEquals(globalTime8, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(9, globalTime9, "B"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(9, message.getPulseId());
		assertEquals(globalTime9, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test Pattern
		// A(11), B(11), B(10), A(10)
		Timestamp globalTime10 = new Timestamp();
		Timestamp globalTime11 = new Timestamp();
		mBuffer.addMessage(newMessage(11, globalTime11, "A"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(11, globalTime11, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(10, globalTime10, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(10, globalTime10, "A"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(2, completeQueue.size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(10, message.getPulseId());
		assertEquals(globalTime10, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(11, message.getPulseId());
		assertEquals(globalTime11, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
	}

	/**
	 * Testing MessageSynchronizer if sendIncompleteMessage flag is set to true
	 */
	@Test
	public void testMessageSynchronizer_100Hz_Incomplete_Time() {
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(5);
		MessageSynchronizer<TestEvent> mBuffer = new MessageSynchronizer<>(completeQueue, 3, true, false, Arrays.asList(new Channel("A", 1), new Channel(
				"B", 1)),
				(event) -> event.getChannel(), (event) -> event.getPulseId(), (pulseId) -> pulseId);

		// Test pattern
		// A(1) A(2) B(2) B(1)
		Timestamp globalTime0 = new Timestamp();
		Timestamp globalTime1 = new Timestamp();
		Timestamp globalTime2 = new Timestamp();
		mBuffer.addMessage(newMessage(1, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(2, globalTime2, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(2, globalTime2, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(1, globalTime1, "B"));

		// Expecting 2 messages in correct order
		assertEquals(2, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());
		AssembledMessage message = new AssembledMessage(completeQueue.poll());
		assertEquals(1, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(2, message.getPulseId());
		assertEquals(globalTime2, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test pattern
		// A(0) B(0)
		mBuffer.addMessage(newMessage(0, globalTime0, "A"));

		// Expecting that there will be no messages as the pulse-id 0 is
		// below the pulse-id already delivered
		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		mBuffer.addMessage(newMessage(0, globalTime0, "B"));

		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		Timestamp globalTime3 = new Timestamp();
		Timestamp globalTime4 = new Timestamp();
		Timestamp globalTime5 = new Timestamp();
		Timestamp globalTime6 = new Timestamp();
		// Test Pattern
		// A(3), A(4), A(5), A(6), B(4), B(3)
		// 3 should be send incomplete because exceeding buffer size
		// 4 should be delivered
		mBuffer.addMessage(newMessage(3, globalTime3, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(4, globalTime4, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(5, globalTime5, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(6, globalTime6, "A"));
		assertEquals(1, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(3, message.getPulseId());
		assertEquals(globalTime3, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		assertTrue(message.getValues().containsKey("A"));

		mBuffer.addMessage(newMessage(4, globalTime4, "B"));
		assertEquals(1, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(4, message.getPulseId());
		assertEquals(globalTime4, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(3, globalTime3, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		assertEquals(0, completeQueue.size());

		// State: A(5), A(6) still in buffer
		Timestamp globalTime7 = new Timestamp();
		Timestamp globalTime8 = new Timestamp();
		Timestamp globalTime9 = new Timestamp();
		mBuffer.addMessage(newMessage(7, globalTime7, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(8, globalTime8, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		assertTrue(message.getValues().containsKey("A"));
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(5, message.getPulseId());
		assertEquals(globalTime5, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		assertTrue(message.getValues().containsKey("A"));
		mBuffer.addMessage(newMessage(9, globalTime9, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(6, message.getPulseId());
		assertEquals(globalTime6, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		assertTrue(message.getValues().containsKey("A"));

		mBuffer.addMessage(newMessage(7, globalTime7, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(7, message.getPulseId());
		assertEquals(globalTime7, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(8, globalTime8, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(8, message.getPulseId());
		assertEquals(globalTime8, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(9, globalTime9, "B"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(9, message.getPulseId());
		assertEquals(globalTime9, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test Pattern
		// A(11), B(11), B(10), A(10)
		Timestamp globalTime10 = new Timestamp();
		Timestamp globalTime11 = new Timestamp();
		mBuffer.addMessage(newMessage(11, globalTime11, "A"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(11, globalTime11, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(10, globalTime10, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(10, globalTime10, "A"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(2, completeQueue.size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(10, message.getPulseId());
		assertEquals(globalTime10, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(11, message.getPulseId());
		assertEquals(globalTime11, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
	}

	/**
	 * Testing MessageSynchronizer if sendIncompleteMessage flag is set to true
	 */
	@Test
	public void testMessageSynchronizer_100Hz_Incomplete_SendFirstComplete() {
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(5);
		MessageSynchronizer<TestEvent> mBuffer = new MessageSynchronizer<>(completeQueue, 3, true, true, Arrays.asList(new Channel("A", 1), new Channel("B",
				1)),
				(event) -> event.getChannel(), (event) -> event.getPulseId());

		// Test pattern
		// A(1) A(2) B(2) B(1)
		Timestamp globalTime0 = new Timestamp();
		Timestamp globalTime1 = new Timestamp();
		Timestamp globalTime2 = new Timestamp();
		mBuffer.addMessage(newMessage(1, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(2, globalTime2, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(2, globalTime2, "B"));

		// Expecting 1 message
		assertEquals(1, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());
		AssembledMessage message = new AssembledMessage(completeQueue.poll());
		assertEquals(2, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(1, globalTime1, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(0, mBuffer.getBufferSize());

		// Test pattern
		// A(0) B(0)
		mBuffer.addMessage(newMessage(0, globalTime0, "A"));

		// Expecting that there will be no messages as the pulse-id 0 is
		// below the pulse-id already delivered
		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		mBuffer.addMessage(newMessage(0, globalTime0, "B"));

		assertEquals(0, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		Timestamp globalTime3 = new Timestamp();
		Timestamp globalTime4 = new Timestamp();
		Timestamp globalTime5 = new Timestamp();
		Timestamp globalTime6 = new Timestamp();
		// Test Pattern
		// A(3), A(4), A(5), A(6), B(4), B(3)
		// 3 should be send incomplete because exceeding buffer size
		// 4 should be delivered
		mBuffer.addMessage(newMessage(3, globalTime3, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(4, globalTime4, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(5, globalTime5, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());
		mBuffer.addMessage(newMessage(6, globalTime6, "A"));
		assertEquals(1, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(3, message.getPulseId());
		assertEquals(globalTime3, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		assertTrue(message.getValues().containsKey("A"));

		mBuffer.addMessage(newMessage(4, globalTime4, "B"));
		assertEquals(1, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(4, message.getPulseId());
		assertEquals(globalTime4, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(3, globalTime3, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		assertEquals(0, completeQueue.size());

		// State: A(5), A(6) still in buffer
		Timestamp globalTime7 = new Timestamp();
		Timestamp globalTime8 = new Timestamp();
		Timestamp globalTime9 = new Timestamp();
		mBuffer.addMessage(newMessage(7, globalTime7, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(8, globalTime8, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		assertTrue(message.getValues().containsKey("A"));
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(5, message.getPulseId());
		assertEquals(globalTime5, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		assertTrue(message.getValues().containsKey("A"));
		mBuffer.addMessage(newMessage(9, globalTime9, "A"));
		assertEquals(3, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(6, message.getPulseId());
		assertEquals(globalTime6, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		assertTrue(message.getValues().containsKey("A"));

		mBuffer.addMessage(newMessage(7, globalTime7, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(7, message.getPulseId());
		assertEquals(globalTime7, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(8, globalTime8, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(8, message.getPulseId());
		assertEquals(globalTime8, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(9, globalTime9, "B"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(1, completeQueue.size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(9, message.getPulseId());
		assertEquals(globalTime9, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		// Test Pattern
		// A(11), B(11), B(10), A(10)
		Timestamp globalTime10 = new Timestamp();
		Timestamp globalTime11 = new Timestamp();
		mBuffer.addMessage(newMessage(11, globalTime11, "A"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(11, globalTime11, "B"));
		assertEquals(1, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(10, globalTime10, "B"));
		assertEquals(2, mBuffer.getBufferSize());
		assertEquals(0, completeQueue.size());
		mBuffer.addMessage(newMessage(10, globalTime10, "A"));
		assertEquals(0, mBuffer.getBufferSize());
		assertEquals(2, completeQueue.size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(10, message.getPulseId());
		assertEquals(globalTime10, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(11, message.getPulseId());
		assertEquals(globalTime11, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
	}

	/**
	 * Test default setup for MessageBuilder with 10 and 100Hz
	 */
	@Test
	public void testMessageSynchronizer_100_10Hz_Incomplete() {
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(5);
		MessageSynchronizer<TestEvent> mBuffer = new MessageSynchronizer<>(completeQueue, 4, true, false, Arrays.asList(new Channel("A", 1), new Channel(
				"B", 10)),
				(event) -> event.getChannel(), (event) -> event.getPulseId());

		// Test pattern
		// A(1) A(2) B(2) B(1)
		Timestamp globalTime1 = new Timestamp();
		mBuffer.addMessage(newMessage(0, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(1, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(10, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(20, globalTime1, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(5, globalTime1, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(0, globalTime1, "B"));

		// Expecting 2 messages in correct order
		assertEquals(2, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());
		AssembledMessage message = new AssembledMessage(completeQueue.poll());
		assertEquals(0, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(1, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		// Test pattern
		// A(0) B(0)
		mBuffer.addMessage(newMessage(-1, globalTime1, "A"));

		// Expecting that there will be no messages as the pulse-id 0 is
		// below the pulse-id already delivered
		assertEquals(2, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		mBuffer.addMessage(newMessage(2, globalTime1, "C"));

		// Expecting that there will be no messages since the channel is not
		// part of the mBuffer
		assertEquals(2, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		for (int i = 2; i <= 9; ++i) {
			mBuffer.addMessage(newMessage(i, globalTime1, "A"));

			assertEquals(1, completeQueue.size());
			assertEquals(2, mBuffer.getBufferSize());

			message = new AssembledMessage(completeQueue.poll());
			assertEquals(i, message.getPulseId());
			assertEquals(globalTime1, message.getGlobalTimestamp());
			assertEquals(1, message.getValues().size());
		}

		mBuffer.addMessage(newMessage(11, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(10, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(10, globalTime1, "B"));
		assertEquals(2, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(10, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(11, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		for (int i = 12; i <= 19; ++i) {
			mBuffer.addMessage(newMessage(i, globalTime1, "A"));

			assertEquals(1, completeQueue.size());
			assertEquals(1, mBuffer.getBufferSize());

			message = new AssembledMessage(completeQueue.poll());
			assertEquals(i, message.getPulseId());
			assertEquals(globalTime1, message.getGlobalTimestamp());
			assertEquals(1, message.getValues().size());
		}

		mBuffer.addMessage(newMessage(20, globalTime1, "A"));
		assertEquals(1, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(20, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(30, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(40, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(22, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(23, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(24, globalTime1, "A"));
		assertEquals(3, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(22, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(23, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(24, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		for (int i = 25; i <= 29; ++i) {
			mBuffer.addMessage(newMessage(i, globalTime1, "A"));

			assertEquals(1, completeQueue.size());
			assertEquals(2, mBuffer.getBufferSize());

			message = new AssembledMessage(completeQueue.poll());
			assertEquals(i, message.getPulseId());
			assertEquals(globalTime1, message.getGlobalTimestamp());
			assertEquals(1, message.getValues().size());
		}

		mBuffer.addMessage(newMessage(31, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(32, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(33, globalTime1, "A"));
		assertEquals(4, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(30, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		assertTrue(message.getValues().containsKey("B"));
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(31, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(32, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(33, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		mBuffer.addMessage(newMessage(30, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(35, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(50, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(60, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(70, globalTime1, "B"));
		assertEquals(1, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(35, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		mBuffer.addMessage(newMessage(80, globalTime1, "B"));
		assertEquals(1, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(40, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		assertTrue(message.getValues().containsKey("B"));

		mBuffer.addMessage(newMessage(34, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(37, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(36, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(40, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(41, globalTime1, "A"));
		assertEquals(1, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(41, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
	}

	/**
	 * Test default setup for MessageBuilder with 10 and 100Hz
	 */
	@Test
	public void testMessageSynchronizer_100_10Hz_Incomplete_SendFirstComplete() {
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(5);
		MessageSynchronizer<TestEvent> mBuffer = new MessageSynchronizer<>(completeQueue, 4, true, true, Arrays.asList(new Channel("A", 1), new Channel("B",
				10)),
				(event) -> event.getChannel(), (event) -> event.getPulseId());

		// Test pattern
		// A(1) A(2) B(2) B(1)
		Timestamp globalTime1 = new Timestamp();
		mBuffer.addMessage(newMessage(0, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(1, globalTime1, "A"));

		assertEquals(1, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());

		AssembledMessage message = new AssembledMessage(completeQueue.poll());
		assertEquals(1, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		mBuffer.addMessage(newMessage(10, globalTime1, "A"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(20, globalTime1, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(5, globalTime1, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(0, globalTime1, "B"));

		assertTrue(completeQueue.isEmpty());
		assertEquals(2, mBuffer.getBufferSize());

		// Test pattern
		// A(0) B(0)
		mBuffer.addMessage(newMessage(-1, globalTime1, "A"));

		// Expecting that there will be no messages as the pulse-id 0 is
		// below the pulse-id already delivered
		assertEquals(2, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		mBuffer.addMessage(newMessage(2, globalTime1, "C"));

		// Expecting that there will be no messages since the channel is not
		// part of the mBuffer
		assertEquals(2, mBuffer.getBufferSize());
		assertTrue(completeQueue.isEmpty());

		for (int i = 2; i <= 9; ++i) {
			mBuffer.addMessage(newMessage(i, globalTime1, "A"));

			assertEquals(1, completeQueue.size());
			assertEquals(2, mBuffer.getBufferSize());

			message = new AssembledMessage(completeQueue.poll());
			assertEquals(i, message.getPulseId());
			assertEquals(globalTime1, message.getGlobalTimestamp());
			assertEquals(1, message.getValues().size());
		}

		mBuffer.addMessage(newMessage(11, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(10, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(10, globalTime1, "B"));
		assertEquals(2, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(10, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(11, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		for (int i = 12; i <= 19; ++i) {
			mBuffer.addMessage(newMessage(i, globalTime1, "A"));

			assertEquals(1, completeQueue.size());
			assertEquals(1, mBuffer.getBufferSize());

			message = new AssembledMessage(completeQueue.poll());
			assertEquals(i, message.getPulseId());
			assertEquals(globalTime1, message.getGlobalTimestamp());
			assertEquals(1, message.getValues().size());
		}

		mBuffer.addMessage(newMessage(20, globalTime1, "A"));
		assertEquals(1, completeQueue.size());
		assertEquals(0, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(20, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(2, message.getValues().size());

		mBuffer.addMessage(newMessage(30, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(40, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(22, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(23, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(24, globalTime1, "A"));
		assertEquals(3, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(22, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(23, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(24, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		for (int i = 25; i <= 29; ++i) {
			mBuffer.addMessage(newMessage(i, globalTime1, "A"));

			assertEquals(1, completeQueue.size());
			assertEquals(2, mBuffer.getBufferSize());

			message = new AssembledMessage(completeQueue.poll());
			assertEquals(i, message.getPulseId());
			assertEquals(globalTime1, message.getGlobalTimestamp());
			assertEquals(1, message.getValues().size());
		}

		mBuffer.addMessage(newMessage(31, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(32, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(33, globalTime1, "A"));
		assertEquals(4, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		message = new AssembledMessage(completeQueue.poll());
		assertEquals(30, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		assertTrue(message.getValues().containsKey("B"));
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(31, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(32, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(33, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		mBuffer.addMessage(newMessage(30, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(1, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(35, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(2, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(50, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(3, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(60, globalTime1, "B"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(70, globalTime1, "B"));
		assertEquals(1, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(35, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());

		mBuffer.addMessage(newMessage(80, globalTime1, "B"));
		assertEquals(1, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(40, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
		assertTrue(message.getValues().containsKey("B"));

		mBuffer.addMessage(newMessage(34, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(37, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(36, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(40, globalTime1, "A"));
		assertEquals(0, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());

		mBuffer.addMessage(newMessage(41, globalTime1, "A"));
		assertEquals(1, completeQueue.size());
		assertEquals(4, mBuffer.getBufferSize());
		message = new AssembledMessage(completeQueue.poll());
		assertEquals(41, message.getPulseId());
		assertEquals(globalTime1, message.getGlobalTimestamp());
		assertEquals(1, message.getValues().size());
	}

	@Test
	public void testMessageSynchronizer_LoadTest_1_100Hz() {
		// in case order is incorrect (not only first element) one might have to
		// change
		// consumer-producer behavior (only one thread can decide when complete
		// -> similar to
		// SortedAsyncStream)
		testMessageSynchronizer_LoadTest(1, 1, 0);
	}

	@Test
	public void testMessageSynchronizer_LoadTest_2_100Hz() {
		testMessageSynchronizer_LoadTest(2, 1, 0);
	}

	@Test
	public void testMessageSynchronizer_LoadTest_3_100Hz() {
		testMessageSynchronizer_LoadTest(3, 1, 0);
	}

	@Test
	public void testMessageSynchronizer_LoadTest_3_100Hz_Forget() {
		testMessageSynchronizer_LoadTest(3, 1, 5);
	}

	@Test
	public void testMessageSynchronizer_LoadTest_1_10Hz() {
		// in case order is incorrect (not only first element) one might have to
		// change
		// consumer-producer behavior (only one thread can decide when complete
		// -> similar to
		// SortedAsyncStream)
		testMessageSynchronizer_LoadTest(1, 10, 0);
	}

	@Test
	public void testMessageSynchronizer_LoadTest_2_10Hz() {
		testMessageSynchronizer_LoadTest(2, 10, 0);
	}

	@Test
	public void testMessageSynchronizer_LoadTest_3_10Hz() {
		testMessageSynchronizer_LoadTest(3, 10, 0);
	}

	public void testMessageSynchronizer_LoadTest(int nrOfChannels, int modulo, int nrToForget) {
		int nrOfEvents = 10000;
		Timestamp globalTime = new Timestamp();
		String channelBase = "Channel_";
		int bufferSize = nrOfEvents + 1;

		Set<Long> forget = new HashSet<>(nrToForget);
		if (nrToForget > 0) {
			bufferSize = nrOfEvents / 2;
			for (int i = 0; i < nrToForget; ++i) {
				forget.add(Long.valueOf(5 + i * 5));
			}
		}

		List<Channel> channels = new ArrayList<>();
		for (int i = 0; i < nrOfChannels; ++i) {
			channels.add(new Channel(channelBase + i, modulo));
		}
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(nrOfEvents + 1);
		MessageSynchronizer<TestEvent> buffer = new MessageSynchronizer<>(completeQueue, bufferSize, false, false, channels,
				(event) -> event.getChannel(), (event) -> event.getPulseId());
		CountDownLatch startSync = new CountDownLatch(1);
		ExecutorService executor = Executors.newFixedThreadPool(nrOfChannels + 1);

		// make sure it knows about first pulse (first and second happen to be
		// out of order since
		// second is received and complete before first arrives)
		for (int i = 0; i < nrOfChannels; ++i) {
			// add first pulse
			buffer.addMessage(newMessage(0, globalTime, channelBase + i));
		}

		List<Future<Void>> futures = new ArrayList<>(nrOfChannels);
		for (int i = 0; i < nrOfChannels; ++i) {
			futures.add(executor.submit(new LoadCallable(channelBase + i, globalTime, modulo, nrOfEvents - 1, modulo,
					startSync,
					buffer,
					forget)));
		}

		// start together
		startSync.countDown();

		// wait until all completed
		for (Future<Void> future : futures) {
			try {
				future.get(nrOfEvents * 10, TimeUnit.MILLISECONDS);
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				e.printStackTrace();
				assertTrue(false);
			}
		}

		// test if everything ok.
		assertEquals(0, buffer.getBufferSize());
		assertEquals(nrOfEvents - nrToForget, completeQueue.size());

		// System.out.println("Elements: "+ completeQueue.stream().map(map -> ""
		// +map.values().iterator().next().getPulseId()).collect(Collectors.joining(",
		// ")));

		for (int i = 0; i < modulo * nrOfEvents; i += modulo) {
			if (!forget.contains(Long.valueOf(i))) {
				final AssembledMessage message = new AssembledMessage(completeQueue.poll());

				assertEquals(i, message.getPulseId());
				assertEquals(globalTime, message.getGlobalTimestamp());
				assertEquals(nrOfChannels, message.getValues().size());

				int j = 0;
				for (Map.Entry<String, TestEvent> entry : message.getValues().entrySet()) {
					assertEquals(channelBase + j, entry.getKey());
					++j;
				}
			}
		}
	}

	@Test
	public void testMessageSynchronizer_LoadTestTime_1_100Hz() {
		// in case order is incorrect (not only first element) one might have to
		// change
		// consumer-producer behavior (only one thread can decide when complete
		// -> similar to
		// SortedAsyncStream)
		testMessageSynchronizer_LoadTestTime(1, 1, 0);
	}

	@Test
	public void testMessageSynchronizer_LoadTestTime_2_100Hz() {
		testMessageSynchronizer_LoadTestTime(2, 1, 0);
	}

	@Test
	public void testMessageSynchronizer_LoadTestTime_3_100Hz() {
		testMessageSynchronizer_LoadTestTime(3, 1, 0);
	}

	@Test
	public void testMessageSynchronizer_LoadTestTime_3_100Hz_Forget() {
		testMessageSynchronizer_LoadTestTime(3, 1, 5);
	}

	@Test
	public void testMessageSynchronizer_LoadTestTime_1_10Hz() {
		// in case order is incorrect (not only first element) one might have to
		// change
		// consumer-producer behavior (only one thread can decide when complete
		// -> similar to
		// SortedAsyncStream)
		testMessageSynchronizer_LoadTestTime(1, 10, 0);
	}

	@Test
	public void testMessageSynchronizer_LoadTestTime_2_10Hz() {
		testMessageSynchronizer_LoadTest(2, 10, 0);
	}

	@Test
	public void testMessageSynchronizer_LoadTestTime_3_10Hz() {
		testMessageSynchronizer_LoadTest(3, 10, 0);
	}

	public void testMessageSynchronizer_LoadTestTime(int nrOfChannels, int modulo, int nrToForget) {
		int nrOfEvents = 10000;
		Timestamp globalTime = new Timestamp();
		String channelBase = "Channel_";
		long sendMessageTimeout = nrOfEvents + 1;

		Set<Long> forget = new HashSet<>(nrToForget);
		if (nrToForget > 0) {
			sendMessageTimeout = nrOfEvents / 2;
			for (int i = 0; i < nrToForget; ++i) {
				forget.add(Long.valueOf(5 + i * 5));
			}
		}

		List<Channel> channels = new ArrayList<>();
		for (int i = 0; i < nrOfChannels; ++i) {
			channels.add(new Channel(channelBase + i, modulo));
		}
		BlockingQueue<Map<String, TestEvent>> completeQueue = new ArrayBlockingQueue<>(nrOfEvents + 1);
		MessageSynchronizer<TestEvent> buffer = new MessageSynchronizer<>(completeQueue, sendMessageTimeout, false, false, channels,
				(event) -> event.getChannel(), (event) -> event.getPulseId(), (pulseId) -> pulseId);
		CountDownLatch startSync = new CountDownLatch(1);
		ExecutorService executor = Executors.newFixedThreadPool(nrOfChannels + 1);

		// make sure it knows about first pulse (first and second happen to be
		// out of order since
		// second is received and complete before first arrives)
		for (int i = 0; i < nrOfChannels; ++i) {
			// add first pulse
			buffer.addMessage(newMessage(0, globalTime, channelBase + i));
		}

		List<Future<Void>> futures = new ArrayList<>(nrOfChannels);
		for (int i = 0; i < nrOfChannels; ++i) {
			futures.add(executor.submit(new LoadCallable(channelBase + i, globalTime, modulo, nrOfEvents - 1, modulo,
					startSync,
					buffer,
					forget)));
		}

		// start together
		startSync.countDown();

		// wait until all completed
		for (Future<Void> future : futures) {
			try {
				future.get(nrOfEvents * 10, TimeUnit.MILLISECONDS);
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				e.printStackTrace();
				assertTrue(false);
			}
		}

		// test if everything ok.
		assertEquals(0, buffer.getBufferSize());
		assertEquals(nrOfEvents - nrToForget, completeQueue.size());

		// System.out.println("Elements: " + completeQueue.stream().map(map ->
		// "" +
		// map.values().iterator().next().getPulseId()).collect(Collectors.joining(",
		// ")));

		for (int i = 0; i < modulo * nrOfEvents; i += modulo) {
			if (!forget.contains(Long.valueOf(i))) {
				final AssembledMessage message = new AssembledMessage(completeQueue.poll());

				assertEquals(i, message.getPulseId());
				assertEquals(globalTime, message.getGlobalTimestamp());
				assertEquals(nrOfChannels, message.getValues().size());

				int j = 0;
				for (Map.Entry<String, TestEvent> entry : message.getValues().entrySet()) {
					assertEquals(channelBase + j, entry.getKey());
					++j;
				}
			}
		}
	}

	@Test
	public void testIsPulseIdMissing_01() {
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 0, Arrays.asList(Pair.of((long) 1, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 1, Arrays.asList(Pair.of((long) 1, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 1, Arrays.asList(Pair.of((long) 1, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 2, Arrays.asList(Pair.of((long) 1, (long) 0))));

		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 0, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 1, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 2, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 3, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 4, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 5, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 1, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 2, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 3, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 4, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 5, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 6, Arrays.asList(Pair.of((long) 2, (long) 0))));

		assertFalse(MessageSynchronizer.isPulseIdMissing(6, 6, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 6, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 6, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 6, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 6, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 6, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(7, 7, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(6, 7, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(5, 7, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 7, Arrays.asList(Pair.of((long) 2, (long) 0))));

		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 0, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 1, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 2, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 3, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 4, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 5, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 1, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 2, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 3, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 4, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 5, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 6, Arrays.asList(Pair.of((long) 3, (long) 0))));

		assertFalse(MessageSynchronizer.isPulseIdMissing(6, 6, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 6, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 6, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(3, 6, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 6, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 6, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(7, 7, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(6, 7, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(5, 7, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 7, Arrays.asList(Pair.of((long) 3, (long) 0))));

		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 1, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 4, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 5, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 6, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 7, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 8, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 9, (long) 0))));

		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 1, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 4, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 5, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 6, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 7, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 8, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 9, (long) 0))));

		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 1, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 4, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 5, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 6, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 7, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 8, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 9, (long) 0))));

		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 1, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 2, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 3, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 4, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 5, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 6, (long) 0))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 7, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 8, (long) 0))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 9, (long) 0))));
	}

	@Test
	public void testIsPulseIdMissing_02() {
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 0, Arrays.asList(Pair.of((long) 1, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 1, Arrays.asList(Pair.of((long) 1, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 1, Arrays.asList(Pair.of((long) 1, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 2, Arrays.asList(Pair.of((long) 1, (long) 1))));

		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 0, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 1, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 2, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 4, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 3, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 4, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 5, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 1, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 2, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 3, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 4, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 5, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 6, Arrays.asList(Pair.of((long) 2, (long) 1))));

		assertFalse(MessageSynchronizer.isPulseIdMissing(6, 6, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 6, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 6, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 6, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 6, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 6, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(7, 7, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(6, 7, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 7, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 7, Arrays.asList(Pair.of((long) 2, (long) 1))));

		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 0, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 1, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 2, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 3, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 4, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 5, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 6, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 1, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 2, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 3, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 4, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 5, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 6, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 7, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 8, Arrays.asList(Pair.of((long) 3, (long) 1))));

		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 0, Arrays.asList(Pair.of((long) 3, (long) 2))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 1, Arrays.asList(Pair.of((long) 3, (long) 2))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 2, Arrays.asList(Pair.of((long) 3, (long) 2))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 3, Arrays.asList(Pair.of((long) 3, (long) 2))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 4, Arrays.asList(Pair.of((long) 3, (long) 2))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 5, Arrays.asList(Pair.of((long) 3, (long) 2))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 6, Arrays.asList(Pair.of((long) 3, (long) 2))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 1, Arrays.asList(Pair.of((long) 3, (long) 2))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(1, 2, Arrays.asList(Pair.of((long) 3, (long) 2))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 3, Arrays.asList(Pair.of((long) 3, (long) 2))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 4, Arrays.asList(Pair.of((long) 3, (long) 2))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 5, Arrays.asList(Pair.of((long) 3, (long) 2))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 6, Arrays.asList(Pair.of((long) 3, (long) 2))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 7, Arrays.asList(Pair.of((long) 3, (long) 2))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 8, Arrays.asList(Pair.of((long) 3, (long) 2))));

		assertFalse(MessageSynchronizer.isPulseIdMissing(6, 6, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 6, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 6, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 6, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 6, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 6, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 6, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(7, 7, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(6, 7, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 7, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 7, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 7, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(8, 8, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(7, 8, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(6, 8, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(5, 8, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 8, Arrays.asList(Pair.of((long) 3, (long) 1))));

		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 1, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 4, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 5, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 6, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 7, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 8, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 8, Arrays.asList(Pair.of((long) 9, (long) 1))));

		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 1, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 4, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 5, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 6, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 7, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 8, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(3, 7, Arrays.asList(Pair.of((long) 9, (long) 1))));

		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 1, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 4, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 5, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 6, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 7, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 8, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 7, Arrays.asList(Pair.of((long) 9, (long) 1))));

		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 1, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 2, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 3, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 4, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 5, (long) 1))));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 6, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 7, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 8, (long) 1))));
		assertFalse(MessageSynchronizer.isPulseIdMissing(3, 8, Arrays.asList(Pair.of((long) 9, (long) 1))));
	}

	@Test
	public void testIsPulseIdMissing_03() {
		Collection<Pair<Long, Long>> config = Arrays.asList(Pair.of((long) 1, (long) 0), Pair.of((long) 1, (long) 0));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 0, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 1, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 2, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 5, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 6, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(5, 7, config));

		config = Arrays.asList(Pair.of((long) 1, (long) 0), Pair.of((long) 2, (long) 0));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 0, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 1, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 2, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 3, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 4, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 5, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 5, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 6, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(5, 7, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(5, 8, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(5, 9, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(6, 6, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 6, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(4, 6, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 6, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 6, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 5, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 5, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 5, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 5, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 5, config));

		config = Arrays.asList(Pair.of((long) 2, (long) 0), Pair.of((long) 4, (long) 0));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 0, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 1, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 2, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 3, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 4, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 5, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 5, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 6, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(5, 7, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(5, 8, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(5, 9, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(6, 6, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 6, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 6, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 6, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 6, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 5, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 5, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 5, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 5, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 5, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(2, 4, config));

		config = Arrays.asList(Pair.of((long) 2, (long) 0), Pair.of((long) 3, (long) 0));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 0, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 1, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(0, 2, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 3, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 4, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(0, 5, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 5, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 6, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(5, 7, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(5, 8, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(5, 9, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(6, 6, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 6, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 6, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 6, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 6, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(5, 5, config));
		assertFalse(MessageSynchronizer.isPulseIdMissing(4, 5, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(3, 5, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 5, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(1, 5, config));
		assertTrue(MessageSynchronizer.isPulseIdMissing(2, 4, config));
	}

	private TestEvent newMessage(long pulseId, Timestamp globalTime, String channel) {
		return new TestEvent(channel, pulseId, globalTime.getSec(), globalTime.getNs());
	}

	private class LoadCallable implements Callable<Void> {
		private final String channel;
		private final Timestamp globalTime;
		private final int modulo;
		private final int startPulseId;
		private final int endPulseId;
		private final CountDownLatch waitForStart;
		private final MessageSynchronizer<TestEvent> buffer;
		private final Set<Long> forget;

		public LoadCallable(String channel, Timestamp globalTime, int startPulseId, int nrOfEvents, int interval,
				CountDownLatch waitForStart, MessageSynchronizer<TestEvent> buffer, Set<Long> forget) {
			this.channel = channel;
			this.globalTime = globalTime;
			this.modulo = interval;
			this.startPulseId = startPulseId;
			this.endPulseId = startPulseId + nrOfEvents * interval;
			this.waitForStart = waitForStart;
			this.buffer = buffer;
			this.forget = forget;
		}

		@Override
		public Void call() {
			try {
				waitForStart.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			for (int i = startPulseId; i < endPulseId; i += modulo) {
				if (!forget.contains(Long.valueOf(i))) {
					buffer.addMessage(newMessage(i, globalTime, channel));
				}
			}

			return null;
		}
	}
}
