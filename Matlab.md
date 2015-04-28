
# Overview
On information regarding how to build the Matlab jar please consult [Readme.md](Readme.md).

# Usage / Examples

Receiving one message:

```
import ch.psi.bsread.basic.*
receiver = BasicReceiver()
receiver.connect()
message = receiver.receive()
message.getMainHeader().getPulseId()
receiver.close()
```

Simple receiver loop:

```
import ch.psi.bsread.basic.*
receiver = BasicReceiver()
receiver.connect()
while 1
	disp(receiver.receive().getMainHeader().getPulseId())
end
receiver.close()
```