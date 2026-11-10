FROM scratch AS jmx
ADD --checksum=sha256:0ad843261c567d1924a017d542e14c40f2afff523d940c25ee000742fde4b3a9 --chmod=0444 \
    https://github.com/prometheus/jmx_exporter/releases/download/1.7.0/jmx_prometheus_javaagent-1.7.0.jar /agent.jar

FROM apache/kafka:4.2.1 AS kafka
COPY --from=jmx /agent.jar /opt/jmx-exporter/agent.jar

FROM quay.io/debezium/connect:3.3.1.Final AS connect
COPY --from=jmx /agent.jar /opt/jmx-exporter/agent.jar
