# Overview
This is the java implementation for bsread the beam synchronous data acquisition for SwissFEL. 

This project provides a `Receiver` as well as an `Sender` implementation to send and receive BSREAD compliant messages.

The specification of bsread can be found [here](https://docs.google.com/document/d/1BynCjz5Ax-onDW0y8PVQnYmSssb6fAyHkdDl1zh21yY/edit?usp=sharing).

This library can also be used within Matlab to receive data.
The documenation on how to use the library can be found in [Matlab.md](Matlab.md)

# Examples

## Receiver
The most simple receiver looks something like this:

```java
   public static void main(String[] args) {
      IReceiver<Object> receiver = new BasicReceiver();
      // IReceiver<Object> receiver = new Receiver<Object>(new
      // ReceiverConfig<Object>("tcp://localhost:9000", new
      // StandardMessageExtractor<Object>(new MatlabByteConverter())));

      // Terminate program with ctrl+c
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
         receiver.close();
      }));

      // Its also possible to register callbacks for certain message parts.
      // These callbacks are triggered within the receive() function
      // (within the same thread) it is guaranteed that the sequence is
      // ordered
      // main header, data header, values
      //
      // receiver.addDataHeaderHandler(header -> System.out.println(header));
      // receiver.addMainHeaderHandler(header -> System.out.println(header) );
      // receiver.addValueHandler(data -> System.out.println(data));

      try {
         Message<Object> message;
         // Due to https://github.com/zeromq/jeromq/issues/116 you must not use Thread.interrupt()
         // to stop the receiving thread!
         while ((message = receiver.receive()) != null) {
            System.out.println(message.getMainHeader());
            System.out.println(message.getDataHeader());
            System.out.println(message.getValues());
         }
      } finally {
         // make sure allocated resources get cleaned up (multiple calls to receiver.close() are
         // side effect free - see shutdown hook)
         receiver.close();
      }
   }
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

In gradle.build rename `pom.artifactId = 'bsread'` to `pom.artifactId = 'bsread_1_7'`

Now build and upload the package via:

```
./gradlew uploadArchives
```
