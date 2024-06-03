package com.microsoft.azure.cassandraproxy;

import io.vertx.core.cli.Argument;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.Option;
import io.vertx.core.cli.TypedOption;

import static com.microsoft.azure.cassandraproxy.Proxy.CASSANDRA_SERVER_PORT;
import static com.microsoft.azure.cassandraproxy.Proxy.PROTOCOL_VERSION;

public class CliBuilder {

    public static CLI build() {
        CLI cli = CLI.create("cassandra-proxy")
                .setSummary("A dual write proxy for cassandra.")
                .addOption(
                        new Option().setLongName("help").setShortName("h").setFlag(true).setHelp(true))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("wait")
                        .setShortName("W")
                        .setDescription("wait for write completed on both clusters")
                        .setDefaultValue("true"))
                .addArgument(new Argument()
                        .setDescription("Source cluster. This is the cluster which is authoritative for reads")
                        .setRequired(true)
                        .setArgName("source"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("Source cluster port. This is the cluster which is authoritative for reads")
                        .setLongName("source-port")
                        .setDefaultValue("9042"))
                .addOption(new Option()
                        .setDescription("Source cluster identifier. This is an identifier used in logs and metrics for the source cluster")
                        .setLongName("source-identifier")
                        .setDefaultValue("source node"))
                .addArgument(new Argument()
                        .setRequired(true)
                        .setDescription("Destination cluster. This is the cluster we ignore reads for.")
                        .setArgName("target"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("Destination cluster port. This is the cluster we ignore reads for.")
                        .setLongName("target-port")
                        .setDefaultValue("9042"))
                .addOption(new Option()
                        .setDescription("Target cluster identifier. This is an identifier used in logs and metrics for the target cluster")
                        .setLongName("target-identifier")
                        .setDefaultValue("target node"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("Port number the proxy listens under")
                        .setLongName("proxy-port")
                        .setShortName("p")
                        .setDefaultValue(CASSANDRA_SERVER_PORT))
                .addOption(new Option()
                        .setDescription("Pem file containing the key for the proxy to perform TLS encryption. If not set, no encryption ")
                        .setLongName("proxy-pem-keyfile"))
                .addOption(new Option()
                        .setDescription("Pem file containing the server certificate for the proxy to perform TLS encryption. If not set, no encryption ")
                        .setLongName("proxy-pem-certfile"))
                .addOption(new Option()
                        .setDescription("Jks containing the key for the proxy to perform TLS encryption. If not set, no encryption ")
                        .setLongName("proxy-jks-file"))
                .addOption(new Option()
                        .setDescription("Password for the Jks store specified (Default: changeit)")
                        .setLongName("proxy-jks-password")
                        .setDefaultValue("changeit"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("How many threads should be launched")
                        .setLongName("threads")
                        .setShortName("t")
                        .setDefaultValue("1"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("Supported Cassandra Protocol Version(s). If not set return what source server says")
                        .setLongName(PROTOCOL_VERSION)
                        .setMultiValued(true))
                .addOption(new Option()
                        .setDescription("Supported Cassandra CQL Version(s). If not set return what source server says")
                        .setLongName("cql-version")
                        .setMultiValued(true))
                .addOption(new Option()
                        .setDescription("Supported Cassandra Compression(s). If not set return what source server says")
                        .setLongName("compression")
                        .setMultiValued(true))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("compression-enabled")
                        .setDescription("If set, cassandra's compression is disabled")
                        .setDefaultValue("true"))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("uuid")
                        .setDescription("scan for uuid and generate on proxy for inserts/updates")
                        .setDefaultValue("false"))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("metrics")
                        .setDescription("provide metrics and start metrics server")
                        .setDefaultValue("true"))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("only-message")
                        .setDescription("some C* APPIs will add payloads and warnings to each request (e.g. charges). Setting this to true will only consider the message for cqlDifferentResultCount metric ")
                        .setDefaultValue("true"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("Port number the promethwus metrics are available")
                        .setLongName("metrics-port")
                        .setShortName("mp")
                        .setDefaultValue("28000"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("size of write buffer for client in bytes. This controls when the system will apply back pressure.")
                        .setLongName("write-buffer-size")
                        .setDefaultValue(String.valueOf(64 * 1024)))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("TCP Idle Time Out in s (0 for infinite)")
                        .setLongName("tcp-idle-time-out")
                        .setDefaultValue("0"))
                .addOption(new Option()
                        .setDescription("Target username if different credential from source. The system will use this user/pwd instead.")
                        .setLongName("target-username"))
                .addOption(new Option()
                        .setDescription("Target password if different credential from source. The system will use this user/pwd instead.")
                        .setLongName("target-password"))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("disable-source-tls")
                        .setDescription("disable tls encryption on the source cluster")
                        .setDefaultValue("true"))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("disable-target-tls")
                        .setDescription("disable tls encryption on the source cluster")
                        .setDefaultValue("true"))
                .addOption(new Option()
                        .setDescription("Regex of queries to be filtered out and not be forwarded to target, e.g. 'insert into test .*'")
                        .setLongName("filter-tables"))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("debug")
                        .setDescription("enable debug logging")
                        .setDefaultValue("false"))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("dest-close")
                        .setDescription("if connection to destination server is lost - close the connection")
                        .setDefaultValue("false"))
                .addOption(new Option()
                        .setLongName("ghostIps")
                        .setDescription("list ips to be replaced, e.g. {\"10.25.0.2\":\"192.168.1.4\",...} "
                                + " - this will replace the ip in system.peers with the other one in queries to "
                                + "allow running the proxy on different ips from the source cluster"));

        return cli;
    }
}
