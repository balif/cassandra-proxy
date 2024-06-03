package com.microsoft.azure.cassandraproxy;

import io.vertx.core.cli.CommandLine;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.microsoft.azure.cassandraproxy.CliBuilder.KESPACE_REPLACE_OPTION;

public interface KeyspaceReplacer {
    String replaceKeyspace(String keyspace);

    String replaceQueryKeyspace(String query);

    com.microsoft.azure.cassandraproxy.KeyspaceReplacer DUMMY_KEYSPACE_REPLACER = new DummyKeyspaceReplacer();

    static KeyspaceReplacer buildFromCli(CommandLine commandLine) {
        String keyspaceRepString = commandLine.getRawValueForOption(KESPACE_REPLACE_OPTION);
        if (keyspaceRepString != null) {
            String[] split = keyspaceRepString.split(",");
            if (split.length != 2) {
                throw new IllegalArgumentException(KESPACE_REPLACE_OPTION.getName() + " Have to have exactly two arguments separated by a comma.");
            }
            return new PairKeyspaceReplacer(split[0], split[1]);
        }
        return null;
    }

    /**
     * Used when key replacer is not active
     */
    class DummyKeyspaceReplacer implements com.microsoft.azure.cassandraproxy.KeyspaceReplacer {
        @Override
        public String replaceKeyspace(String keyspace) {
            return keyspace;
        }

        @Override
        public String replaceQueryKeyspace(String query) {
            return query;
        }
    }

    /**
     * Replace source keyspace with target keyspace
     */
    class PairKeyspaceReplacer implements com.microsoft.azure.cassandraproxy.KeyspaceReplacer {

        private final String sourceKeyspace;
        private final String targetKeyspace;

        public PairKeyspaceReplacer(String sourceKeyspace, String targetKeyspace) {
            this.sourceKeyspace = sourceKeyspace;
            this.targetKeyspace = targetKeyspace;
        }

        @Override
        public String replaceKeyspace(String keyspace) {
            return keyspace != null && keyspace.equals(sourceKeyspace) ? targetKeyspace : keyspace;
        }

        @Override
        public String replaceQueryKeyspace(String query) {
            String regex = "(?i).*[USE|INTO|UPDATE].*(" + sourceKeyspace + ")\\\"{0,1}((\\s*$)|(\\..*))";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(query);
            if (matcher.find()) {
                return query.replaceFirst(matcher.group(1), targetKeyspace);
            }
            return query;
        }


    }
}
