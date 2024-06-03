package com.microsoft.azure.cassandraproxy;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KeyspaceReplacer {

    private final String sourceKeyspace;
    private final String targetKeyspace;

    public KeyspaceReplacer(String sourceKeyspace, String targetKeyspace) {
        this.sourceKeyspace = sourceKeyspace;
        this.targetKeyspace = targetKeyspace;
    }

    public String replaceKeyspace(String keyspace) {
        return  keyspace!= null && keyspace.equals(sourceKeyspace) ? targetKeyspace : keyspace;
    }
    public String replaceQueryKeyspace(String query) {
        String regex = "(?i).*[USE|INTO|UPDATE].*("+sourceKeyspace+")\\\"{0,1}((\\s*$)|(\\..*))";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(query);
        if (matcher.find( )) {
            return query.replaceFirst(matcher.group(1), targetKeyspace);
        }
        return query;
    }

}
