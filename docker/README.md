# Dockerfile to build Kafka CruiseControl

This is based on [solsson/cruise-control](https://github.com/solsson/dockerfiles/tree/master/cruise-control) with the following additions:
1. Use `openjdk 25`
1. Builds Adobe maintained fork of cruise-control:https://github.com/adobe/cruise-control

## Alternate JDK downloads

The runtime image defaults to Oracle's public Java 25 download without authentication.
Override `java_download_base_url` and `java_download_version` to use another source.
The Dockerfile appends `java_version` and `latest` for Oracle, or just `java_version`
for alternate sources. Do not include the Java-major directory in the base URL.
`java_download_version` selects the archive filename, not a directory.
Alternate sources must provide Oracle-style archive filenames and
matching `.sha256` files for both `x64` and `aarch64`.

For example, to download
`https://jdk.example.com/java/25/jdk-25.0.1.1_linux-aarch64_bin.tar.gz`:

```bash
docker buildx build -f docker/Dockerfile \
  --build-arg java_download_base_url=https://jdk.example.com/java \
  --build-arg java_download_version=25.0.1.1 \
  .
```

For authenticated downloads, also pass an optional API key as a BuildKit secret:

```bash
--secret id=JAVA_DOWNLOAD_APIKEY,env=JAVA_DOWNLOAD_APIKEY
```

The key is sent as an `Authorization: Bearer` header for the archive and checksum,
is not stored in the image, and is not forwarded to a different host on redirects.
Do not pass the key as a build argument or embed it in the URL.

Release builds also default to Oracle. To use an alternate source, enable the
commented `build-args` in `.github/workflows/build-push-docker.yml`, configure the
GitHub Actions secret `JAVA_DOWNLOAD_BASE_URL`, and select a matching
`java_download_version`. For authenticated downloads, also enable the commented
`secrets` input and configure `JAVA_DOWNLOAD_APIKEY`.

`java_version` selects the runtime Java major version (21 or later) and defaults
to 25. The Gradle builder remains `gradle:jdk25` regardless of these overrides.
