
# Overview
This package can be use to receive data from the beam synchronous data acquisition within Matlab.

The latest stable package can be downloaded [here](http://slsyoke4.psi.ch:8081/artifactory/releases/bsread_java_matlab-1.4.0.jar).

The *prerequisites* for this package is *Matlab2015a* or later.

To be able to use the package, include the full qualified path of the jar in the *javaclasspath.txt* within the Matlab home folder. For example:

```
/Users/ebner/Documents/MATLAB/bsread_java_matlab-1.4.0.jar
```

After altering the file you need to restart Matlab. Now you are able to use the package to receive data.

_Note:_ On information regarding how to build the Matlab jar please consult [Readme.md](Readme.md).

# Usage

After including the jar in the Matlab classpath (see above) you are able to receive data as follows: 

Receiving one message and printing the pulse id:

```
import ch.psi.bsread.basic.*
receiver = BasicReceiver()
receiver.connect()
message = receiver.receive()
message.getMainHeader().getPulseId()
receiver.close()
```

Simple receiver loop printing the pulse id of the message:

```
import ch.psi.bsread.basic.*
receiver = BasicReceiver()
receiver.connect()
while 1
	disp(receiver.receive().getMainHeader().getPulseId())
end
receiver.close()
```

After creating the BasicReceiver object and connecting to a stream (in the examples above a stream that comes from *localhost* port *9999*) you are able to receive messages via the *receive()* function. Consecutive calls to *receive()* will provide the messages for the consecutive pulse_id. 

A message received contains a MainHeader containing the pulse id and global timestamp, a DataHeader describing the data channels within the stream and the actual Values of the channels for the pulse.

Following some code snippets to show how to access these values:

 * Get Pulse Id

```
message.getMainHeader().getPulseId()
```

 * Get Global Timestamp (Unix-Time)

```
timestamp = message.getMainHeader().getGlobalTimestamp()
% Get milliseconds epoch
timestamp.getEpoch()
% Get nanoseconds offset
timestamp.getNs()
```

 * Get names of channels included in stream

```
message.getValues.keySet()
```

 * Get value of a specific channel

```
message.getValues().get('CHANNEL_NAME').getValue()
```

* Get IOC timestamp of a value

```
message.getValues().get('CHANNEL_NAME').getTimestamp()
```

* Iterate over the values

```
entryset = message.getValues.entrySet()
iterator = entryset.iterator()
while iterator.hasNext()
	entry = iterator.next()
	entry.getKey() % Get channel name
	entry.getValue().getValue() % get value
end
```
