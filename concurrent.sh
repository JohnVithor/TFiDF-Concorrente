#!/bin/bash
/home/johnvithor/.jdks/openjdk-18/bin/java -Xmx4096m -XX:ActiveProcessorCount=2 -javaagent:/snap/intellij-idea-ultimate/353/lib/idea_rt.jar=38235:/snap/intellij-idea-ultimate/353/bin -Dfile.encoding=UTF-8 -classpath /home/johnvithor/UFRN/Concorrente/TFiDFParquet/target/classes:/home/johnvithor/.m2/repository/org/apache/parquet/parquet-avro/1.12.2/parquet-avro-1.12.2.jar:/home/johnvithor/.m2/repository/org/apache/parquet/parquet-column/1.12.2/parquet-column-1.12.2.jar:/home/johnvithor/.m2/repository/org/apache/parquet/parquet-common/1.12.2/parquet-common-1.12.2.jar:/home/johnvithor/.m2/repository/org/apache/yetus/audience-annotations/0.12.0/audience-annotations-0.12.0.jar:/home/johnvithor/.m2/repository/org/apache/parquet/parquet-encoding/1.12.2/parquet-encoding-1.12.2.jar:/home/johnvithor/.m2/repository/org/apache/parquet/parquet-format-structures/1.12.2/parquet-format-structures-1.12.2.jar:/home/johnvithor/.m2/repository/javax/annotation/javax.annotation-api/1.3.2/javax.annotation-api-1.3.2.jar:/home/johnvithor/.m2/repository/org/apache/avro/avro/1.10.1/avro-1.10.1.jar:/home/johnvithor/.m2/repository/com/fasterxml/jackson/core/jackson-core/2.11.3/jackson-core-2.11.3.jar:/home/johnvithor/.m2/repository/com/fasterxml/jackson/core/jackson-databind/2.11.3/jackson-databind-2.11.3.jar:/home/johnvithor/.m2/repository/com/fasterxml/jackson/core/jackson-annotations/2.11.3/jackson-annotations-2.11.3.jar:/home/johnvithor/.m2/repository/org/apache/commons/commons-compress/1.20/commons-compress-1.20.jar:/home/johnvithor/.m2/repository/org/apache/parquet/parquet-hadoop/1.12.2/parquet-hadoop-1.12.2.jar:/home/johnvithor/.m2/repository/org/apache/parquet/parquet-jackson/1.12.2/parquet-jackson-1.12.2.jar:/home/johnvithor/.m2/repository/org/xerial/snappy/snappy-java/1.1.8/snappy-java-1.1.8.jar:/home/johnvithor/.m2/repository/commons-pool/commons-pool/1.6/commons-pool-1.6.jar:/home/johnvithor/.m2/repository/com/github/luben/zstd-jni/1.4.9-1/zstd-jni-1.4.9-1.jar:/home/johnvithor/.m2/repository/org/apache/hadoop/hadoop-core/1.2.1/hadoop-core-1.2.1.jar:/home/johnvithor/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar:/home/johnvithor/.m2/repository/xmlenc/xmlenc/0.52/xmlenc-0.52.jar:/home/johnvithor/.m2/repository/com/sun/jersey/jersey-core/1.8/jersey-core-1.8.jar:/home/johnvithor/.m2/repository/com/sun/jersey/jersey-json/1.8/jersey-json-1.8.jar:/home/johnvithor/.m2/repository/org/codehaus/jettison/jettison/1.1/jettison-1.1.jar:/home/johnvithor/.m2/repository/stax/stax-api/1.0.1/stax-api-1.0.1.jar:/home/johnvithor/.m2/repository/com/sun/xml/bind/jaxb-impl/2.2.3-1/jaxb-impl-2.2.3-1.jar:/home/johnvithor/.m2/repository/javax/xml/bind/jaxb-api/2.2.2/jaxb-api-2.2.2.jar:/home/johnvithor/.m2/repository/javax/xml/stream/stax-api/1.0-2/stax-api-1.0-2.jar:/home/johnvithor/.m2/repository/javax/activation/activation/1.1/activation-1.1.jar:/home/johnvithor/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.7.1/jackson-core-asl-1.7.1.jar:/home/johnvithor/.m2/repository/org/codehaus/jackson/jackson-jaxrs/1.7.1/jackson-jaxrs-1.7.1.jar:/home/johnvithor/.m2/repository/org/codehaus/jackson/jackson-xc/1.7.1/jackson-xc-1.7.1.jar:/home/johnvithor/.m2/repository/com/sun/jersey/jersey-server/1.8/jersey-server-1.8.jar:/home/johnvithor/.m2/repository/asm/asm/3.1/asm-3.1.jar:/home/johnvithor/.m2/repository/commons-io/commons-io/2.1/commons-io-2.1.jar:/home/johnvithor/.m2/repository/commons-httpclient/commons-httpclient/3.0.1/commons-httpclient-3.0.1.jar:/home/johnvithor/.m2/repository/junit/junit/3.8.1/junit-3.8.1.jar:/home/johnvithor/.m2/repository/commons-logging/commons-logging/1.0.3/commons-logging-1.0.3.jar:/home/johnvithor/.m2/repository/commons-codec/commons-codec/1.4/commons-codec-1.4.jar:/home/johnvithor/.m2/repository/org/apache/commons/commons-math/2.1/commons-math-2.1.jar:/home/johnvithor/.m2/repository/commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar:/home/johnvithor/.m2/repository/commons-collections/commons-collections/3.2.1/commons-collections-3.2.1.jar:/home/johnvithor/.m2/repository/commons-lang/commons-lang/2.4/commons-lang-2.4.jar:/home/johnvithor/.m2/repository/commons-digester/commons-digester/1.8/commons-digester-1.8.jar:/home/johnvithor/.m2/repository/commons-beanutils/commons-beanutils/1.7.0/commons-beanutils-1.7.0.jar:/home/johnvithor/.m2/repository/commons-beanutils/commons-beanutils-core/1.8.0/commons-beanutils-core-1.8.0.jar:/home/johnvithor/.m2/repository/commons-net/commons-net/1.4.1/commons-net-1.4.1.jar:/home/johnvithor/.m2/repository/org/mortbay/jetty/jetty/6.1.26/jetty-6.1.26.jar:/home/johnvithor/.m2/repository/org/mortbay/jetty/servlet-api/2.5-20081211/servlet-api-2.5-20081211.jar:/home/johnvithor/.m2/repository/org/mortbay/jetty/jetty-util/6.1.26/jetty-util-6.1.26.jar:/home/johnvithor/.m2/repository/tomcat/jasper-runtime/5.5.12/jasper-runtime-5.5.12.jar:/home/johnvithor/.m2/repository/tomcat/jasper-compiler/5.5.12/jasper-compiler-5.5.12.jar:/home/johnvithor/.m2/repository/org/mortbay/jetty/jsp-api-2.1/6.1.14/jsp-api-2.1-6.1.14.jar:/home/johnvithor/.m2/repository/org/mortbay/jetty/servlet-api-2.5/6.1.14/servlet-api-2.5-6.1.14.jar:/home/johnvithor/.m2/repository/org/mortbay/jetty/jsp-2.1/6.1.14/jsp-2.1-6.1.14.jar:/home/johnvithor/.m2/repository/ant/ant/1.6.5/ant-1.6.5.jar:/home/johnvithor/.m2/repository/commons-el/commons-el/1.0/commons-el-1.0.jar:/home/johnvithor/.m2/repository/net/java/dev/jets3t/jets3t/0.6.1/jets3t-0.6.1.jar:/home/johnvithor/.m2/repository/hsqldb/hsqldb/1.8.0.10/hsqldb-1.8.0.10.jar:/home/johnvithor/.m2/repository/oro/oro/2.0.8/oro-2.0.8.jar:/home/johnvithor/.m2/repository/org/eclipse/jdt/core/3.1.1/core-3.1.1.jar:/home/johnvithor/.m2/repository/org/codehaus/jackson/jackson-mapper-asl/1.8.8/jackson-mapper-asl-1.8.8.jar:/home/johnvithor/.m2/repository/org/slf4j/slf4j-api/2.0.0-alpha7/slf4j-api-2.0.0-alpha7.jar:/home/johnvithor/.m2/repository/org/slf4j/slf4j-simple/2.0.0-alpha7/slf4j-simple-2.0.0-alpha7.jar:/home/johnvithor/.m2/repository/org/openjdk/jmh/jmh-core/1.35/jmh-core-1.35.jar:/home/johnvithor/.m2/repository/net/sf/jopt-simple/jopt-simple/5.0.4/jopt-simple-5.0.4.jar:/home/johnvithor/.m2/repository/org/apache/commons/commons-math3/3.2/commons-math3-3.2.jar:/home/johnvithor/.m2/repository/net/intelie/tinymap/tinymap/0.9/tinymap-0.9.jar main.Concurrent $1
