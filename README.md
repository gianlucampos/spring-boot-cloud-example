# Spring Boot Cloud Config Client

Simple test application for validating:

* Spring Cloud Config Client
* Spring Boot Actuator
* `@ConfigurationProperties`
* `@RefreshScope`

The application retrieves configuration from a Spring Cloud Config Server and supports refreshing properties at runtime without restarting.

### Flow

```text
Git → Config Server → Config Client
                       ↓
                  Actuator Refresh
```

### Test endpoints

Returns topics from Config Server:

```bash
curl http://localhost:8081/app/v1/topics
```

Update Config Server properties and refresh Config Client:

```bash
curl -X POST http://localhost:8081/actuator/refresh
```
