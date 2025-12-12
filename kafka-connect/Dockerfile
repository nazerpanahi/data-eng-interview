FROM confluentinc/cp-kafka-connect:7.5.0

# Install Confluent JDBC Connector (this includes MySQL support)
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4

# Download MySQL JDBC Driver
RUN curl -L https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar \
    -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/mysql-connector-j-8.2.0.jar