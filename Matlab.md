
# Development

To be able to generate the Matlab jar, set source compatibility inside `build.gradle` to 1.7 (`sourceCompatibility = 1.7`).

Afterwards comment the three callbacks for main header, data header and values  in `Receiver.java`.

Now build the package via:

```
./gradlew fatJar
```