/home/johnvithor/UFRN/Concorrente/openjdk-19-loom+6-625_linux-x64_bin/jdk-19/bin/java -Xms2g -Xmx2g -XX:MaxMetaspaceSize=512m -XX:+UnlockExperimentalVMOptions -XX:+UseShenandoahGC --enable-preview -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+FlightRecorder -XX:StartFlightRecording=name="Shenandoah_Loom_Profiler",settings="profile",dumponexit=true,filename="./Shenandoah_Loom_Profiler.jfr" -XX:FlightRecorderOptions=stackdepth=2048 -Dfile.encoding=UTF-8 -classpath /home/johnvithor/UFRN/Concorrente/TFiDF-Concorrente/target/classes:/home/johnvithor/.m2/repository/org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar tfidf.Concurrent


