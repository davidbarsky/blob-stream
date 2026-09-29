<img width="2926" height="1606" alt="blob-stream" src="https://github.com/user-attachments/assets/18979bb4-3c66-4222-a359-9d98f741a021" />

# blob-stream

`blob-stream` is a Kafka-like streaming system for high-throughput workloads that prioritizes total
cost of ownership over ultra-low latency. Brokers accept producer batches, persist compressed
payload segments in blob storage, and publish metadata indexes for pull-based consumers.

Delivery is at least once. Producers can retry ambiguous requests and consumers can replay records
after interrupted commits or rebalances, so applications must tolerate duplicates.

# Goals

- Zero cross-AZ traffic.
- Stateless brokers with zero local storage.
- Brokers can autoscale at will without downtime.
- No independent control plane or cluster manager. Producers, brokers, and consumers coordinate
  entirely either through K8s service discovery and the DynamoDB metadata store.
- Excellent observability via metrics, OTLP traces, and admin HTTP state snapshots.

## System At A Glance

- Producers hash record keys into logical partitions and map them to virtual partitions by
  `writer_id`.
- Producers send `ProduceBatches` requests to the locally discovered broker selected by rendezvous
  hashing.
- Brokers hold producer-partition leases, reserve monotonic sequence ranges, and flush compressed
  segment blobs with metadata indexes.
- Consumers scan metadata windows, range-read selected blob batches, and commit progress per
  virtual partition.

## Requirements

- Blob-stream **requires** a shared accurate clock between brokers and consumers. See the
  [FAQ](docs/faq.md) for more information. This system was designed and implemented around the
  AWS Time Sync Service and assumes accurate clocks.

## Project Layout

- `blob-stream-broker/`: broker binary and write path
- `blob-stream-producer/`: producer client library
- `blob-stream-consumer/`: consumer iterator and coordinator library
- `blob-stream-broker-discovery/`: static and Kubernetes service discovery
- `blob-stream-blob-store/`: blob store trait with in-memory and S3 backends
- `blob-stream-metadata-store/`: metadata and lease stores with in-memory and DynamoDB backends
- `blob-stream-proto/`: protobuf API and configuration schemas
- `blob-stream-types/`: shared wire and storage types
- `blob-stream-integration-tests/`: end-to-end and deterministic fault-injection tests
- `blob-stream-sim/`: deterministic turmoil-net simulation harness with an in-process S3 served
  by `s3s`, scenario tests, and a hegel property test

## Documentation

### Deploy And Operate

- [Local end-to-end walkthrough](examples/local-e2e/README.md): run static-discovery brokers,
  interactive text producers, and consumer groups against local Docker Compose dependencies
- [Dashboard references](examples/dashboards/README.md): broker, producer, and consumer CUE
  definitions to adapt for an observability configuration
- [Kubernetes deployment reference](examples/deployment/README.md): per-cluster Terraform for a
  broker writer, service discovery, and runtime configuration
- [System design and correctness contract](docs/design/README.md)
- [Infrastructure setup](docs/infrastructure.md): configuration, DynamoDB, S3, IAM, and Kubernetes
  discovery RBAC
- [Operations guide](docs/operations.md): runtime controls, metrics, diagnostic endpoints, and
  troubleshooting
- [Producer and consumer integration](docs/integration.md): library entry points, configuration
  contracts, lifecycles, and application-mounted diagnostics
- [Metrics reference](docs/metrics.md): producer, consumer, and broker metric definitions
- [FAQ](docs/faq.md)
- [Cost analysis](docs/cost-analysis.md)

### Contribute And Verify

- Contributions must include a Developer Certificate of Origin sign-off. See [DCO](DCO).
- [Development guide](DEVELOPMENT.md): standalone Cargo and monorepo Bazel workflows, local
  dependencies, stress runner, and Rust API documentation
- [Integration-test audit](plans/TEST_AUDIT.md): deterministic test requirements and hardening work
- [TLA+ model](tla/README.md): finite model of lease, sequence, publication, and cursor safety

The protobuf configuration schema in
[blobstream/v1/config.proto](blob-stream-proto/proto/blobstream/v1/config.proto) is the canonical
field-level reference for broker, producer, and consumer configuration.

### Contact

For async response open GitHub issues and I will try to respond when I can. For lack of a better
option right now, I've also created a #blob-stream channel in [Envoy
Slack](https://www.envoyproxy.io/slack). I will try to answer questions there as well.
