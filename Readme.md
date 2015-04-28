# Overview
This is the java implementation for bsread.
It provides a `Receiver` as well as an `Sender` implementation to send/receive BSREAD compliant messages.

# Examples

## Receiver
The most simple receiver looks something like this:

```java
Receiver receiver = new Receiver();
		
receiver.connect("tcp://localhost:9000");

// Its also possible to register callbacks for certain message parts.
// These callbacks are triggered within the receive() function 
// (within the same thread) it is guaranteed that the sequence is ordered
// main header, data header, values

// receiver.addDataHeaderHandler(header -> System.out.println(header));
// receiver.addMainHeaderHandler(header -> System.out.println(header) );
// receiver.addValueHandler(data -> System.out.println(data));
		
while(!Thread.currentThread().isInterrupted()){
	Message message = receiver.receive();
			
	System.out.println(message.getMainHeader());
	System.out.println(message.getDataHeader());
	System.out.println(message.getValues());
}
		
receiver.close();
```

# Development

This project can be build by executing

```bash
./gradlew build
```

_Note:_ The first time you execute this command the required jars for the build system will be automatically downloaded and the build will start afterwards. The next time you execute the command the build should be faster.

To upload the built jar to the Maven repository use:

```bash
./gradlew uploadArchives
```

## Matlab

To be able to generate the Matlab jar, set source compatibility inside `build.gradle` to 1.7 (`sourceCompatibility = 1.7`).
(Inside Eclipse refresh Project via right-click > Gradle > Refresh all, then Project > clean)

Afterwards comment the three callbacks for main header, data header and values  in `Receiver.java`.

Remove the Testcases in `ch.psi.bsread` and `ch.psi.bsread.basic`.

Now build the package via:

```
./gradlew matlabJar
```
