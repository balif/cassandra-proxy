//package com.microsoft.azure.cassandraproxy;
//
//import junit.framework.TestCase;
//
//public class KeyspaceReplaceTest extends TestCase {
//
//    public void testReplaceKeyspaceInUse() {
//        KeyspaceReplace keyspaceReplace = new KeyspaceReplace();
//        String res = keyspaceReplace.replaceKeyspace("USE \"fe_profiler\"", "new_key", "fe_profiler");
//        System.out.println(res);
//    }
//
//    public void testReplaceKeyspaceInINSERT() {
//        KeyspaceReplace keyspaceReplace = new KeyspaceReplace();
//        String res = keyspaceReplace.replaceKeyspace("insert into fe_profiler.profiler_fe_agg_state (targetid, id, tag, \"count\", exptimestamp, score, \"timestamp\", version) values ('eeeee','aa','zz',1,1,1,1,1);", "new_key", "fe_profiler");
//        System.out.println(res);
//    }
//}