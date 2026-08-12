# Spring Boot Cloud Config Client

Simple test application for validating:

* Spring Cloud Config Client
* Spring Boot Actuator
* `@ConfigurationProperties`
* `@RefreshScope`
* Runtime Kafka environment switching
* Spring Cloud Bus refresh

### Flow

```text
Git → Config Server → Config Client
                       ↓
                  Bus Refresh
                       ↓
              Kafka Consumer
```

### Test endpoints

Returns topics from Config Server:

```bash
curl http://localhost:8081/app/v1/topics
```

Refresh Config Client properties:

```bash
curl -X POST http://localhost:8081/actuator/refresh
```

Refresh all connected instances through Spring Cloud Bus:

```bash
curl -X POST http://localhost:8081/actuator/busrefresh
```

Kafka consumers can switch between AWS and GCP based on the configuration stored in the Config Server, without restarting the application.
