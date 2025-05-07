# Build Your Own Kafka (Go)

This repository contains a Go implementation for the CodeCrafters "Build Your Own Kafka" challenge.

The goal is to build a toy Kafka clone capable of handling basic Kafka protocol requests.

[![progress-banner](https://backend.codecrafters.io/progress/kafka/8d73ea99-e90b-4704-9adc-09a12fab7d5a)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is a starting point for Go solutions to the
["Build Your Own Kafka" Challenge](https://codecrafters.io/challenges/kafka).

In this challenge, you'll build a toy Kafka clone that's capable of accepting
and responding to APIVersions & Fetch API requests. You'll also learn about
encoding and decoding messages using the Kafka wire protocol. You'll also learn
about handling the network protocol, event loops, TCP sockets and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Implemented Features

* **Network Layer**: Sets up a TCP server to listen for incoming connections.
* **Protocol Handling**: Decodes basic Kafka request headers (size, apiKey, apiVersion, correlationID, clientID).
* **API Requests**:
  * **APIVersions (ApiKey 18)**: Responds with the supported API versions.
  * **Describe topic (ApiKey 75)**:
  * **Fetch (ApiKey 1)**:

Currently, only the `APIVersions` request is implemented. Further requests (like Fetch, Produce, Metadata) would need to be added to the `ApiHandlers` map in `app/protocol/handler.go` and corresponding handler functions created.
