
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

# Development

To be able to generate the Matlab jar, set source compatibility inside `build.gradle` to 1.7 (`sourceCompatibility = 1.7`).
(Inside Eclipse refresh Project via right-click > Gradle > Refresh all, then Project > clean)

Afterwards comment the three callbacks for main header, data header and values  in `Receiver.java`.

Remove the Testcases in `ch.psi.bsread` and `ch.psi.bsread.basic`.

Now build the package via:

```
./gradlew fatJar
```