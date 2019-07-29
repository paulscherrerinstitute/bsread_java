# Overview
This is the Java implementation of the beam synchronous data acquisition protocol (bsread) for SwissFEL. This project provides a `Receiver` as well as an `Sender` implementation to send and receive BSREAD compliant messages.

The specification of bsread can be found [here](https://docs.google.com/document/d/1BynCjz5Ax-onDW0y8PVQnYmSssb6fAyHkdDl1zh21yY/edit?usp=sharing).

This library does not provide the code needed to access the SwissFEL dispatching layer!


# Usage

To use the library from a project managed with Gradle use this to get this library as dependency:

```
repositories {
    maven { url "https://artifacts.psi.ch/artifactory/libs-snapshots"}
}


dependencies {
    compile('ch.psi:bsread:3.4.4')
}
```

The most simple receiver looks something like this:

```java
   public static void main(String[] args) {
      IReceiver<Object> receiver = new BasicReceiver();
      // ReceiverConfig config = ReceiverConfig<Object>("tcp://localhost:9000");
      // config.setSocketType(ZMQ.SUB);
      // IReceiver<Object> receiver = new BasicReceiver<Object>(config);

      // Terminate program with ctrl+c
      Runtime.getRuntime().addShutdownHook(new Thread(() -> receiver.close()));

      // Its also possible to register callbacks for certain message parts.
      // These callbacks are triggered within the receive() function
      // (within the same thread) it is guaranteed that the sequence is
      // ordered main header, data header, values
      //
      // receiver.addDataHeaderHandler(header -> System.out.println(header));
      // receiver.addMainHeaderHandler(header -> System.out.println(header) );
      // receiver.addValueHandler(data -> System.out.println(data));

      try {
         receiver.connect();
         
         Message<Object> message;
         // Due to https://github.com/zeromq/jeromq/issues/116 you must not use Thread.interrupt()
         // to stop the receiving thread!
         while ((message = receiver.receive()) != null) {
            System.out.println(message.getMainHeader());
            // System.out.println(message.getDataHeader());
            // System.out.println(message.getValues());
         }
      } finally {
         // make sure allocated resources get cleaned up (multiple calls to receiver.close() are
         // side effect free - see shutdown hook)
         receiver.close();
      }
   }
```

## Matlab

The library can also be used within Matlab to receive data. The documentation on how to use the library can be found in [Matlab.md](Matlab.md)

# Development

This project can be build by executing

```bash
./gradlew build
```

To upload the built jar to the artifact repository use:

```bash
./gradlew uploadArchives
```

There is a Docker build container that should be used for production build (to ensure 1.8 compatiblity). The container can be used/started as follows:
```bash
docker run -it --rm -v $(pwd):/data -v$HOME:/root paulscherrerinstitute/centos_build_java:1.8
```

